package fileio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"

	icebergio "github.com/apache/iceberg-go/io"
)

// ObjectInfo describes a stored file.
type ObjectInfo struct {
	Location string
	Size     int64
}

// Create opens a streaming writer for the file at location, replacing any
// existing file. The file appears when the writer is closed; a writer that
// fails part-way may leave a partial file behind.
func (f *Iceberg) Create(_ context.Context, location string) (io.WriteCloser, error) {
	fsys, err := f.io(location)
	if err != nil {
		return nil, err
	}

	w, err := fsys.Create(location)
	if err != nil {
		return nil, wrapErr("create", location, err)
	}

	return w, nil
}

// Open opens the file at location for streaming reads.
func (f *Iceberg) Open(_ context.Context, location string) (io.ReadCloser, error) {
	fsys, err := f.io(location)
	if err != nil {
		return nil, err
	}

	file, err := fsys.Open(location)
	if err != nil {
		return nil, wrapErr("open", location, err)
	}

	return file, nil
}

// List returns every file under prefix, a location that names a directory,
// in lexical order. A prefix with no files is an empty list.
func (f *Iceberg) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	fsys, err := f.io(prefix)
	if err != nil {
		return nil, err
	}

	lister, ok := fsys.(icebergio.ListableIO)
	if !ok {
		return nil, fmt.Errorf("file IO for %q cannot list files", prefix)
	}

	root := strings.TrimSuffix(prefix, "/")
	objects := []ObjectInfo{}

	err = lister.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("reading %q: %w", path, err)
		}

		objects = append(objects, ObjectInfo{Location: path, Size: info.Size()})

		return nil
	})

	switch {
	case errors.Is(err, fs.ErrNotExist):
		return []ObjectInfo{}, nil
	case err != nil:
		return nil, wrapErr("list", prefix, err)
	default:
		return objects, nil
	}
}
