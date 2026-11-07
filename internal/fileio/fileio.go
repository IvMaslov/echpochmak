// Package fileio reads and writes files in object storage on behalf of the
// catalog. The catalog only handles table metadata files: it reads the current
// one, writes new ones and deletes stale ones. Data files are written and read
// by the engines themselves.
package fileio

import (
	"context"
	"errors"
)

// ErrNotFound means the file does not exist.
var ErrNotFound = errors.New("file not found")

// FileIO is the catalog's view of object storage. Locations are full URIs such
// as s3://bucket/path/to/file.
type FileIO interface {
	// Read returns the whole content of the file at location.
	Read(ctx context.Context, location string) ([]byte, error)
	// Write creates the file at location with the given content, replacing
	// any existing file.
	Write(ctx context.Context, location string, data []byte) error
	// Delete removes the file at location. Deleting a missing file succeeds.
	Delete(ctx context.Context, location string) error
}
