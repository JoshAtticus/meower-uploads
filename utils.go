package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/redis/go-redis/v9"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	ErrUnsupportedFile = errors.New("unsupported file")
	ErrFileBlocked     = errors.New("file blocked")
)

func generateId() (string, error) {
	// Generate bytes
	b := make([]byte, 18)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}

	// Construct ID
	id := base64.URLEncoding.EncodeToString(b)
	id = strings.ReplaceAll(id, "-", "a")
	id = strings.ReplaceAll(id, "_", "b")
	id = strings.ReplaceAll(id, "=", "c")
	return id, err
}

func cleanFilename(filename string) string {
	re := regexp.MustCompile(`[^A-Za-z0-9\.\-\_\+\!\(\)$]`)
	return re.ReplaceAllString(filename, "_")
}

// Delete unclaimed files that are more than 30 minutes old
func cleanupFiles() error {
	cur, err := db.Collection("files").Find(context.TODO(), bson.M{
		"claimed":     false,
		"uploaded_at": bson.M{"$lt": time.Now().Unix() - 1800},
	})
	if err != nil {
		return err
	}

	var files []File
	if err := cur.All(context.TODO(), &files); err != nil {
		return err
	}

	for _, file := range files {
		if err := file.Delete(); err != nil {
			return err
		}
	}

	return nil
}

func isFileReferenced(bucket string, hashHex string) (bool, error) {
	opts := options.Count()
	opts.SetLimit(1)
	count, err := db.Collection("files").CountDocuments(
		context.TODO(),
		bson.M{"hash": hashHex, "bucket": bucket},
		opts,
	)
	return count > 0, err
}

// Get the block status of a file by its hash.
// Returns whether it's blocked.
func getBlockStatus(hashHex string) (bool, error) {
	opts := options.Count()
	opts.SetLimit(1)
	count, err := db.Collection("blocked_files").CountDocuments(
		context.TODO(),
		bson.M{"_id": hashHex},
		opts,
	)
	return count > 0, err
}

const (
	fileLockTTL  = time.Minute
	fileLockWait = 30 * time.Second
)

var ErrFileLocked = errors.New("timed out waiting for file lock")

var unlockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
	return redis.call("del", KEYS[1])
end
return 0
`)

// Lock a bucket+hash pair across all instances.
// Used to make dedup lookups and object removal mutually exclusive.
func lockFileHash(bucket string, hashHex string) (func(), error) {
	key := fmt.Sprint("uploads:lock:", bucket, ":", hashHex)
	token, err := generateId()
	if err != nil {
		return nil, err
	}
	deadline := time.Now().Add(fileLockWait)
	for {
		acquired, err := rdb.SetNX(ctx, key, token, fileLockTTL).Result()
		if err != nil {
			return nil, err
		}
		if acquired {
			return func() {
				unlockScript.Run(ctx, rdb, []string{key}, token)
			}, nil
		}
		if time.Now().After(deadline) {
			return nil, ErrFileLocked
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// S3 bucket names are globally unique on some providers (e.g. Backblaze B2),
// so an optional prefix is added to every bucket name.
func s3Bucket(bucket string) string {
	return os.Getenv("S3_BUCKET_PREFIX") + bucket
}

func objectExists(bucket string, objName string) bool {
	_, err := s3Clients[s3RegionOrder[0]].StatObject(ctx, s3Bucket(bucket), objName, minio.StatObjectOptions{})
	return err == nil
}
