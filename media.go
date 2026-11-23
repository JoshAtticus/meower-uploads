package main

import (
	"context"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	magickTimeout   = 60 * time.Second
	ffmpegTimeout   = 120 * time.Second
	utilTimeout     = 30 * time.Second
	mediaJobTimeout = 2 * time.Minute
)

var ErrMediaBusy = errors.New("media processing queue is full")

// Coders ImageMagick is allowed to read and write, keyed by detected mime type.
// Anything else is never handed to ImageMagick.
var imageCoders = map[string]string{
	"image/png":  "PNG",
	"image/apng": "PNG",
	"image/jpeg": "JPEG",
	"image/gif":  "GIF",
	"image/webp": "WEBP",
}

// Resource limits passed to every magick invocation, in addition to policy.xml.
var magickLimits = []string{
	"-limit", "memory", "256MiB",
	"-limit", "map", "512MiB",
	"-limit", "disk", "1GiB",
	"-limit", "area", "128MP",
	"-limit", "width", "16KP",
	"-limit", "height", "16KP",
	"-limit", "list-length", "1000",
	"-limit", "thread", "2",
	"-limit", "time", "60",
}

// Initialised in main once the environment has been loaded
var mediaSem chan struct{}

func maxMediaJobs() int {
	if n, err := strconv.Atoi(os.Getenv("MAX_MEDIA_JOBS")); err == nil && n > 0 {
		return n
	}
	return runtime.NumCPU()
}

func imageCoder(mime string) (string, bool) {
	coder, ok := imageCoders[mime]
	return coder, ok
}

func isProcessableImage(mime string) bool {
	_, ok := imageCoders[mime]
	return ok
}

func runCommand(timeout time.Duration, name string, args ...string) ([]byte, error) {
	cmdCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, name, args...)
	cmd.WaitDelay = 5 * time.Second
	return cmd.Output()
}

func runMediaCommand(timeout time.Duration, name string, args ...string) ([]byte, error) {
	select {
	case mediaSem <- struct{}{}:
		defer func() { <-mediaSem }()
	case <-time.After(mediaJobTimeout):
		return nil, ErrMediaBusy
	}
	return runCommand(timeout, name, args...)
}

// Runs magick with resource limits. Inputs and outputs must carry an explicit
// coder prefix (e.g. "PNG:/path") so the file contents can't select the decoder.
func runMagick(args ...string) ([]byte, error) {
	if len(args) > 0 && args[0] == "identify" {
		return runMediaCommand(magickTimeout, "magick", append(append([]string{"identify"}, magickLimits...), args[1:]...)...)
	}
	return runMediaCommand(magickTimeout, "magick", append(append([]string{}, magickLimits...), args...)...)
}

// Extracts the first video frame as a JPEG, only allowing local file access.
func extractFirstFrame(src string, dst string) error {
	_, err := runMediaCommand(
		ffmpegTimeout,
		"ffmpeg",
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-protocol_whitelist", "file",
		"-probesize", "32M",
		"-analyzeduration", "10M",
		"-i", "file:"+src,
		"-frames:v", "1",
		"-q:v", "2",
		"-f", "image2",
		"-y",
		"file:"+dst,
	)
	return err
}

// Gets the mime type of a file from its magic bytes.
func detectMime(path string) (string, error) {
	out, err := runCommand(utilTimeout, "file", "--brief", "--mime-type", path)
	if err != nil {
		return "", err
	}
	mime := strings.TrimSpace(string(out))
	if mime == "" {
		return "", ErrUnsupportedFile
	}
	return mime, nil
}

// Gets width and height of an image using an explicit coder.
func identifyDimensions(coderPath string) (int, int, error) {
	out, err := runMagick("identify", "-ping", "-format", "%w,%h\n", coderPath)
	if err != nil {
		return 0, 0, err
	}
	first := strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]
	parts := strings.Split(first, ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("unexpected identify output %q", first)
	}
	width, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	height, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	return width, height, nil
}

// Gets the number of frames in an image using an explicit coder.
func identifyFrames(coderPath string) (int, error) {
	out, err := runMagick("identify", "-ping", "-format", "%n\n", coderPath)
	if err != nil {
		return 0, err
	}
	first := strings.SplitN(strings.TrimSpace(string(out)), "\n", 2)[0]
	return strconv.Atoi(first)
}

// Largest image the service will open, checked from the file header alone so
// a tiny "decompression bomb" is refused before any decoder allocates memory.
const (
	maxImageSide   = 16384
	maxImagePixels = 100 * 1000 * 1000
)

var ErrImageTooLarge = errors.New("image dimensions are too large")

// Reads the declared dimensions from a PNG, GIF, JPEG or WebP header.
func headerDimensions(path string) (int, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	if cfg, _, err := image.DecodeConfig(file); err == nil {
		return cfg.Width, cfg.Height, nil
	}

	// WebP isn't in the standard library: read the RIFF header directly
	if _, err := file.Seek(0, 0); err != nil {
		return 0, 0, err
	}
	head := make([]byte, 32)
	n, _ := file.Read(head)
	if n >= 30 && string(head[0:4]) == "RIFF" && string(head[8:12]) == "WEBP" {
		switch string(head[12:16]) {
		case "VP8X":
			w := 1 + int(head[24]) | int(head[25])<<8 | int(head[26])<<16
			h := 1 + int(head[27]) | int(head[28])<<8 | int(head[29])<<16
			return w, h, nil
		case "VP8 ":
			return int(head[26]) | int(head[27])<<8&0x3fff, int(head[28]) | int(head[29])<<8&0x3fff, nil
		case "VP8L":
			bits := uint32(head[21]) | uint32(head[22])<<8 | uint32(head[23])<<16 | uint32(head[24])<<24
			return int(bits&0x3fff) + 1, int((bits>>14)&0x3fff) + 1, nil
		}
	}
	return 0, 0, errors.New("could not read image header")
}

// Refuses images whose declared size is unreasonable (or unreadable).
func checkImageHeader(path string) error {
	w, h, err := headerDimensions(path)
	if err != nil {
		return ErrImageTooLarge
	}
	if w <= 0 || h <= 0 || w > maxImageSide || h > maxImageSide || w*h > maxImagePixels {
		return ErrImageTooLarge
	}
	return nil
}
