package fileio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"net/url"
	"sync"

	icebergio "github.com/apache/iceberg-go/io"
)

// Iceberg implements [FileIO] with the IO layer of iceberg-go, which supports
// s3://, file:// and mem:// locations.
//
// Object store backends must be registered by importing them for side
// effects in main, for example github.com/apache/iceberg-go/io/gocloud/s3.
type Iceberg struct {
	// ctx outlives requests: the IOs it creates are cached and shared, and
	// iceberg-go binds an IO to the context it was created with.
	ctx   context.Context //nolint:containedctx // see above
	props map[string]string

	mu  sync.Mutex
	ios map[string]icebergio.WriteFileIO
}

// NewIceberg returns a FileIO configured with iceberg-go IO properties, such
// as s3.endpoint or s3.access-key-id. ctx bounds the lifetime of the
// underlying clients; pass the server's lifetime context.
func NewIceberg(ctx context.Context, props map[string]string) *Iceberg {
	return &Iceberg{
		ctx:   ctx,
		props: maps.Clone(props),
		ios:   map[string]icebergio.WriteFileIO{},
	}
}

var _ FileIO = (*Iceberg)(nil)

// Read implements [FileIO].
func (f *Iceberg) Read(_ context.Context, location string) ([]byte, error) {
	fsys, err := f.io(location)
	if err != nil {
		return nil, err
	}

	file, err := fsys.Open(location)
	if err != nil {
		return nil, wrapErr("open", location, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, wrapErr("read", location, err)
	}

	return data, nil
}

// Write implements [FileIO].
func (f *Iceberg) Write(_ context.Context, location string, data []byte) error {
	fsys, err := f.io(location)
	if err != nil {
		return err
	}

	w, err := fsys.Create(location)
	if err != nil {
		return wrapErr("create", location, err)
	}

	if _, err := w.Write(data); err != nil {
		_ = w.Close()

		return wrapErr("write", location, err)
	}

	if err := w.Close(); err != nil {
		return wrapErr("write", location, err)
	}

	return nil
}

// Delete implements [FileIO].
func (f *Iceberg) Delete(_ context.Context, location string) error {
	fsys, err := f.io(location)
	if err != nil {
		return err
	}

	if err := fsys.Remove(location); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return wrapErr("delete", location, err)
	}

	return nil
}

// io returns the IO for the scheme and bucket of location, creating it once.
func (f *Iceberg) io(location string) (icebergio.WriteFileIO, error) {
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("parsing location %q: %w", location, err)
	}

	key := u.Scheme + "://" + u.Host

	f.mu.Lock()
	defer f.mu.Unlock()

	if fsys, ok := f.ios[key]; ok {
		return fsys, nil
	}

	fsys, err := icebergio.LoadFS(f.ctx, f.props, location)
	if err != nil {
		return nil, fmt.Errorf("loading file IO for %q: %w", key, err)
	}

	wfs, ok := fsys.(icebergio.WriteFileIO)
	if !ok {
		return nil, fmt.Errorf("file IO for %q does not support writing", key)
	}

	f.ios[key] = wfs

	return wfs, nil
}

func wrapErr(op, location string, err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%s %q: %w", op, location, ErrNotFound)
	}

	return fmt.Errorf("%s %q: %w", op, location, err)
}
