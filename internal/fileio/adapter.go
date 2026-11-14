package fileio

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"path"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
)

// AsIcebergIO returns f as a read-only iceberg-go IO, for iceberg-go code that
// reads manifests and manifest lists. Files are read whole; ctx bounds every
// read. Removing files through it is refused: deletions go through f, where
// the caller controls them.
func AsIcebergIO(ctx context.Context, f FileIO) icebergio.IO {
	return &icebergAdapter{ctx: ctx, f: f}
}

var errReadOnly = errors.New("fileio: read-only iceberg IO")

type icebergAdapter struct {
	ctx context.Context //nolint:containedctx // iceberg-go's IO interface has no context parameter.
	f   FileIO
}

func (a *icebergAdapter) Open(name string) (icebergio.File, error) {
	data, err := a.f.Read(a.ctx, name)
	if errors.Is(err, ErrNotFound) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}

	if err != nil {
		return nil, err
	}

	return &memFile{Reader: bytes.NewReader(data), name: name, size: int64(len(data))}, nil
}

func (*icebergAdapter) Remove(name string) error {
	return &fs.PathError{Op: "remove", Path: name, Err: errReadOnly}
}

// memFile is a file read into memory.
type memFile struct {
	*bytes.Reader

	name string
	size int64
}

func (f *memFile) Stat() (fs.FileInfo, error) {
	return fileInfo{name: path.Base(f.name), size: f.size}, nil
}

func (*memFile) Close() error { return nil }

type fileInfo struct {
	name string
	size int64
}

func (i fileInfo) Name() string     { return i.name }
func (i fileInfo) Size() int64      { return i.size }
func (fileInfo) Mode() fs.FileMode  { return 0o444 }
func (fileInfo) ModTime() time.Time { return time.Time{} }
func (fileInfo) IsDir() bool        { return false }
func (fileInfo) Sys() any           { return nil }
