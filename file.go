package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/minio/minio-go/v7"
	"github.com/vmihailenco/msgpack/v5"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type File struct {
	Id            string `bson:"_id" json:"id"`
	Hash          string `bson:"hash" json:"-"`
	Bucket        string `bson:"bucket" json:"-"`
	Mime          string `bson:"mime" json:"mime"`
	ThumbnailMime string `bson:"thumbnail_mime,omitempty" json:"thumbnail_mime,omitempty"`
	Size          int64  `bson:"size" json:"size"`
	ThumbnailSize int64  `bson:"thumbnail_size,omitempty" json:"thumbnail_size,omitempty"`
	Filename      string `bson:"filename,omitempty" json:"filename,omitempty"`
	Width         int    `bson:"width,omitempty" json:"width,omitempty"`
	Height        int    `bson:"height,omitempty" json:"height,omitempty"`
	Thumbhash     []byte `bson:"thumbhash,omitempty" json:"thumbhash,omitempty"`

	UploadRegion string `bson:"upload_region" json:"-"`
	UploadedBy   string `bson:"uploaded_by" json:"-"`
	UploadedAt   int64  `bson:"uploaded_at" json:"-"`

	Claimed bool `bson:"claimed" json:"-"`
}

func IngestMultipartFile(
	bucket string,
	file multipart.File,
	fileHeader *multipart.FileHeader,
	uploader *User,
) (*File, error) {
	// Init vars
	var f File
	var wg sync.WaitGroup
	var err error
	var id, hashHex string

	// Create file ID
	id, err = generateId()
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// Create directory in ingest directory for temporary files
	ingestDir := fmt.Sprint(os.Getenv("INGEST_DIR"), "/", id)
	defer os.RemoveAll(ingestDir)
	if err := os.Mkdir(ingestDir, 0700); err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	originalPath := fmt.Sprint(ingestDir, "/original")

	// Save file
	dst, err := os.Create(originalPath)
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	defer dst.Close()
	if _, err := io.Copy(dst, file); err != nil {
		sentry.CaptureException(err)
		return nil, err
	}

	// "Ultra HD"
	if uploader.Flags&FlagUltraHDUploads != 0 {
		if mime, err := detectMime(originalPath); err == nil {
			if coder, ok := imageCoder(mime); ok {
				if _, err := runMagick(
					fmt.Sprint(coder, ":", originalPath),
					"-quality",
					"5",
					fmt.Sprint("JPEG:", originalPath, ".jpg"),
				); err == nil {
					os.Rename(fmt.Sprint(originalPath, ".jpg"), originalPath)
				}
			}
		}
	}

	// Get file hash
	var out []byte
	out, err = runCommand(utilTimeout, "sha256sum", originalPath)
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	hashHex = strings.Fields(string(out))[0]

	// Make sure file isn't blocked
	if blocked, err := getBlockStatus(hashHex); blocked || err != nil {
		os.Remove(originalPath)
		if blocked {
			return nil, ErrFileBlocked
		}
		return nil, err
	}

	// Reuse existing object if there is one.
	// The lock stops cleanup from removing the object between the lookup and the insert.
	unlock, err := lockFileHash(bucket, hashHex)
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	err = db.Collection("files").FindOne(
		context.TODO(),
		bson.M{"hash": hashHex, "bucket": bucket},
	).Decode(&f)
	if err != nil && err != mongo.ErrNoDocuments {
		unlock()
		return nil, err
	}
	if f.Hash == hashHex && objectExists(bucket, hashHex) {
		f.Id = id
		f.Filename = cleanFilename(fileHeader.Filename)
		f.UploadedBy = uploader.Username
		f.UploadedAt = time.Now().Unix()
		f.Claimed = false
		err = f.insert()
		unlock()
		if err != nil {
			return &f, err
		}
		return &f, nil
	}
	unlock()

	// Create file details
	f = File{
		Id:           id,
		Hash:         hashHex,
		Bucket:       bucket,
		Filename:     cleanFilename(fileHeader.Filename),
		UploadRegion: s3RegionOrder[0],
		UploadedBy:   uploader.Username,
		UploadedAt:   time.Now().Unix(),
	}

	// Get mime
	f.Mime, err = detectMime(originalPath)
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	coder, isImage := imageCoder(f.Mime)
	originalCoderPath := fmt.Sprint(coder, ":", originalPath)
	objectPath := originalPath

	// Get dimensions, if it is an image
	if isImage {
		// refuse oversized images from their header before ImageMagick sees them
		if err := checkImageHeader(originalPath); err != nil {
			return nil, err
		}
		f.Width, f.Height, err = identifyDimensions(originalCoderPath)
		if err != nil {
			sentry.CaptureException(err)
			return nil, err
		}
	}

	if bucket == "icons" || bucket == "emojis" || bucket == "stickers" {
		// Make sure the file is a supported image
		if !isImage {
			return nil, ErrUnsupportedFile
		}

		// Get frames
		frames, err := identifyFrames(originalCoderPath)
		if err != nil {
			sentry.CaptureException(err)
			return nil, err
		}

		// Choose format to convert to and update mime
		format := "webp"
		f.Mime = "image/webp"
		if frames != 1 {
			format = "gif"
			f.Mime = "image/gif"
		}
		outputCoder, _ := imageCoder(f.Mime)
		outputPath := fmt.Sprint(ingestDir, "/.", format)
		objectPath = outputPath

		// If one of the axis is less than n, use that size rather than n
		var desiredSize int
		switch bucket {
		case "icons":
			desiredSize = 256
		case "emojis":
			desiredSize = 128
		case "stickers":
			desiredSize = 384
		}
		if f.Width < desiredSize {
			desiredSize = f.Width
		} else if f.Height < desiredSize {
			desiredSize = f.Height
		}

		// Remove Exif, optimize, and resize. Animated images must be coalesced
		// first, otherwise resizing the partial frames corrupts them (glitchy playback).
		magickArgs := []string{originalCoderPath}
		if frames != 1 {
			magickArgs = append(magickArgs, "-coalesce")
		}
		magickArgs = append(magickArgs,
			"-quality",
			"90",
			"-resize",
			fmt.Sprint(desiredSize, "x", desiredSize),
			"-auto-orient",
			"-strip",
		)
		if frames != 1 {
			magickArgs = append(magickArgs, "-layers", "optimize")
		}
		magickArgs = append(magickArgs, fmt.Sprint(outputCoder, ":", outputPath))
		if _, err := runMagick(magickArgs...); err != nil {
			sentry.CaptureException(err)
			return nil, err
		}

		// Upload to bucket
		var uploadErr, identifyErr error
		wg.Add(1)
		go func() {
			defer wg.Done()
			var info minio.UploadInfo
			info, uploadErr = s3Clients[s3RegionOrder[0]].FPutObject(
				ctx,
				s3Bucket(bucket),
				hashHex,
				outputPath,
				minio.PutObjectOptions{
					ContentType: f.Mime,
				},
			)
			f.Size = info.Size
		}()

		// Get new width and height
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.Width, f.Height, identifyErr = identifyDimensions(fmt.Sprint(outputCoder, ":", outputPath))
		}()

		wg.Wait()

		if err := errors.Join(uploadErr, identifyErr); err != nil {
			sentry.CaptureException(err)
			return nil, err
		}
	} else if bucket == "attachments" {
		if isImage { // Images
			// Remove Exif and optimize
			optimizedPath := fmt.Sprint(ingestDir, "/optimized")
			objectPath = optimizedPath
			optArgs := []string{originalCoderPath}
			animated := false
			if n, ferr := identifyFrames(originalCoderPath); ferr == nil && n != 1 {
				animated = true
				optArgs = append(optArgs, "-coalesce")
			}
			optArgs = append(optArgs, "-quality", "90", "-auto-orient", "-strip")
			if animated && coder == "GIF" {
				optArgs = append(optArgs, "-layers", "optimize")
			}
			optArgs = append(optArgs, fmt.Sprint(coder, ":", optimizedPath))
			if _, err := runMagick(optArgs...); err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
			f.Thumbhash = computeThumbhash(fmt.Sprint(coder, ":", optimizedPath), ingestDir)

			// Upload optimized to bucket
			var uploadErr, thumbnailErr error
			wg.Add(1)
			go func() {
				defer wg.Done()
				var info minio.UploadInfo
				info, uploadErr = s3Clients[s3RegionOrder[0]].FPutObject(
					ctx,
					s3Bucket(bucket),
					hashHex,
					optimizedPath,
					minio.PutObjectOptions{
						ContentType: f.Mime,
					},
				)
				f.Size = info.Size
			}()

			// Generate thumbnail
			wg.Add(1)
			go func() {
				defer wg.Done()
				thumbnailErr = f.GenerateThumbnail()
			}()

			wg.Wait()

			if err := errors.Join(uploadErr, thumbnailErr); err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
		} else if strings.HasPrefix(f.Mime, "video/") { // Videos
			// Start uploading video to bucket
			var uploadErr, thumbnailErr error
			wg.Add(1)
			go func() {
				defer wg.Done()
				var info minio.UploadInfo
				info, uploadErr = s3Clients[s3RegionOrder[0]].FPutObject(
					ctx,
					s3Bucket(bucket),
					hashHex,
					originalPath,
					minio.PutObjectOptions{
						ContentType: f.Mime,
					},
				)
				f.Size = info.Size
			}()

			// Get first frame and its dimensions
			firstFramePath := fmt.Sprint(ingestDir, "/first_frame.jpg")
			err = extractFirstFrame(originalPath, firstFramePath)
			if err == nil {
				f.Width, f.Height, err = identifyDimensions(fmt.Sprint("JPEG:", firstFramePath))
			}
			if err == nil {
				f.Thumbhash = computeThumbhash(fmt.Sprint("JPEG:", firstFramePath), ingestDir)
			}

			// Generate thumbnail
			if err == nil {
				thumbnailErr = f.GenerateThumbnail()
			}

			wg.Wait()

			if err := errors.Join(err, uploadErr, thumbnailErr); err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
		} else { // Everything else
			info, err := s3Clients[s3RegionOrder[0]].FPutObject(
				ctx,
				s3Bucket(bucket),
				hashHex,
				originalPath,
				minio.PutObjectOptions{
					ContentType: f.Mime,
				},
			)
			if err != nil {
				sentry.CaptureException(err)
				return nil, err
			}
			f.Size = info.Size
		}
	}

	// Create database item
	unlock, err = lockFileHash(bucket, hashHex)
	if err != nil {
		sentry.CaptureException(err)
		return nil, err
	}
	err = f.restoreRemovedObjects(objectPath)
	if err == nil {
		err = f.insert()
	}
	unlock()
	if err != nil {
		return &f, err
	}

	return &f, nil
}

// Cleanup may have removed the objects of an older row with the same hash while
// this file was being processed. Must be called while holding the hash lock.
func (f *File) restoreRemovedObjects(objectPath string) error {
	if !objectExists(f.Bucket, f.Hash) {
		if _, err := s3Clients[s3RegionOrder[0]].FPutObject(
			ctx,
			s3Bucket(f.Bucket),
			f.Hash,
			objectPath,
			minio.PutObjectOptions{
				ContentType: f.Mime,
			},
		); err != nil {
			sentry.CaptureException(err)
			return err
		}
	}
	if f.ThumbnailMime != "" && !objectExists(f.Bucket, f.Hash+"_thumbnail") {
		// Regenerated on the next thumbnail request
		f.ThumbnailMime = ""
		f.ThumbnailSize = 0
	}
	return nil
}

func (f *File) insert() error {
	if _, err := db.Collection("files").InsertOne(context.TODO(), f); err != nil {
		sentry.CaptureException(err)
		return err
	}

	// Send to files automod
	marshaledEvent, err := msgpack.Marshal(map[string]interface{}{
		"type":        0,
		"username":    f.UploadedBy,
		"file_bucket": f.Bucket,
		"file_hashes": []string{f.Hash},
	})
	if err != nil {
		sentry.CaptureException(err)
	} else if err := rdb.Publish(context.TODO(), "automod:files", marshaledEvent).Err(); err != nil {
		fmt.Println(err)
		sentry.CaptureException(err)
	}

	sentry.CaptureMessage(fmt.Sprintf("Uploaded file %s with hash %s to %s region", f.Id, f.Hash, f.UploadRegion))

	return nil
}

func GetFile(id string) (File, error) {
	var f File
	err := db.Collection("files").FindOne(
		context.TODO(),
		bson.M{"_id": id, "uploaded_at": bson.M{"$ne": 0}},
	).Decode(&f)
	return f, err
}

func (f *File) GenerateThumbnail() error {
	isVideo := strings.HasPrefix(f.Mime, "video/")
	coder, isImage := imageCoder(f.Mime)
	if !isImage && !isVideo {
		return ErrUnsupportedFile
	}

	// Create directory in ingest directory for temporary files
	// And download file for processing
	ingestDir := fmt.Sprint(os.Getenv("INGEST_DIR"), "/", f.Id)
	originalPath := fmt.Sprint(ingestDir, "/original")
	if _, err := os.Stat(ingestDir); os.IsNotExist(err) {
		defer os.RemoveAll(ingestDir)
		if err := os.Mkdir(ingestDir, 0700); err != nil {
			sentry.CaptureException(err)
			return err
		}

		obj, _, err := f.GetObject(false)
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		defer obj.Close()

		dst, err := os.Create(originalPath)
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		defer dst.Close()
		if _, err := io.Copy(dst, obj); err != nil {
			sentry.CaptureException(err)
			return err
		}
	}

	// Choose format to use for the thumbnail
	format := "webp"
	if isImage { // use GIF for animated images
		frames, err := identifyFrames(fmt.Sprint(coder, ":", originalPath))
		if err != nil {
			sentry.CaptureException(err)
			return err
		}
		if frames != 1 {
			format = "gif"
		}
	}
	outputCoder, _ := imageCoder(fmt.Sprint("image/", format))

	// Use largest axis that is smaller than 480px
	var desiredSize int
	if f.Width > f.Height {
		desiredSize = f.Width
	} else {
		desiredSize = f.Height
	}
	if desiredSize > 1280 {
		desiredSize = 1280
	}

	// Get first frame if it's a video
	src := fmt.Sprint(coder, ":", originalPath)
	if isVideo {
		firstFramePath := fmt.Sprint(ingestDir, "/first_frame.jpg")
		if _, err := os.Stat(firstFramePath); os.IsNotExist(err) {
			if err := extractFirstFrame(originalPath, firstFramePath); err != nil {
				sentry.CaptureException(err)
				return err
			}
		}
		src = fmt.Sprint("JPEG:", firstFramePath)
	}

	// Create thumbnail
	thumbnailPath := fmt.Sprint(ingestDir, "/thumbnail.", format)
	thumbArgs := []string{src}
	if format == "gif" {
		thumbArgs = append(thumbArgs, "-coalesce")
	}
	thumbArgs = append(thumbArgs, "-quality", "88", "-resize", fmt.Sprint(desiredSize, "x", desiredSize))
	if format == "gif" {
		thumbArgs = append(thumbArgs, "-layers", "optimize")
	}
	thumbArgs = append(thumbArgs, fmt.Sprint(outputCoder, ":", thumbnailPath))
	if _, err := runMagick(thumbArgs...); err != nil {
		sentry.CaptureException(err)
		return err
	}

	// Upload thumbnail
	uploadInfo, err := s3Clients[s3RegionOrder[0]].FPutObject(
		ctx,
		s3Bucket(f.Bucket),
		fmt.Sprint(f.Hash, "_thumbnail"),
		thumbnailPath,
		minio.PutObjectOptions{
			ContentType: fmt.Sprint("image/", format),
		},
	)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}

	// Update file details
	f.ThumbnailMime = fmt.Sprint("image/", format)
	f.ThumbnailSize = uploadInfo.Size
	if _, err := db.Collection("files").UpdateMany(
		context.TODO(),
		bson.M{"hash": f.Hash, "bucket": f.Bucket},
		bson.M{"$set": bson.M{
			"thumbnail_mime": f.ThumbnailMime,
			"thumbnail_size": f.ThumbnailSize,
		}},
	); err != nil {
		sentry.CaptureException(err)
		return err
	}

	return nil
}

func (f *File) HasThumbnail() bool {
	return f.Bucket == "attachments" && (isProcessableImage(f.Mime) || strings.HasPrefix(f.Mime, "video/"))
}

func (f *File) GetObject(thumbnail bool) (*minio.Object, *minio.ObjectInfo, error) {
	objName := f.Hash
	if thumbnail && f.HasThumbnail() {
		// Generate thumbnail if one doesn't exist yet
		if f.ThumbnailMime == "" || f.ThumbnailSize == 0 {
			if err := f.GenerateThumbnail(); err != nil {
				return nil, nil, err
			}
		}

		objName += "_thumbnail"
	}

	// Get object
	obj, err := s3Clients[s3RegionOrder[0]].GetObject(
		ctx,
		s3Bucket(f.Bucket),
		objName,
		minio.GetObjectOptions{},
	)
	if err != nil {
		return nil, nil, err
	}

	// Get object info (used for size)
	objInfo, err := s3Clients[s3RegionOrder[0]].StatObject(
		ctx,
		s3Bucket(f.Bucket),
		objName,
		minio.StatObjectOptions{},
	)
	if err != nil {
		return nil, nil, err
	}

	return obj, &objInfo, nil
}

func (f *File) Delete() error {
	// Hold the hash lock so a concurrent upload can't start reusing the
	// objects between the reference check and their removal
	unlock, err := lockFileHash(f.Bucket, f.Hash)
	if err != nil {
		return err
	}
	defer unlock()

	// Delete database row
	if _, err := db.Collection("files").DeleteOne(
		context.TODO(),
		bson.M{"_id": f.Id},
	); err != nil {
		return err
	}

	// Clean-up objects if nothing else is referencing them
	referenced, err := isFileReferenced(f.Bucket, f.Hash)
	if err != nil {
		return err
	}
	if !referenced {
		var wg sync.WaitGroup
		for _, s3Client := range s3Clients {
			for _, objName := range []string{f.Hash, f.Hash + "_thumbnail"} {
				wg.Add(1)
				go func(s3Client *minio.Client, objName string) {
					defer wg.Done()
					s3Client.RemoveObject(ctx, s3Bucket(f.Bucket), objName, minio.RemoveObjectOptions{})
				}(s3Client, objName)
			}
		}
		wg.Wait()
	}

	return nil
}
