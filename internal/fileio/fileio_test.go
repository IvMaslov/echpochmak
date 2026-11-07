package fileio_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"echpochmak/internal/fileio"
)

func TestIcebergRoundTrip(t *testing.T) {
	t.Parallel()

	locations := map[string]string{
		"mem":  "mem://fileio-test/round-trip/dir/file.json",
		"file": "file://" + filepath.Join(t.TempDir(), "dir", "file.json"),
	}

	for name, location := range locations {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			f := fileio.NewIceberg(context.Background(), nil)

			if err := f.Write(ctx, location, []byte("hello")); err != nil {
				t.Fatalf("Write: %v", err)
			}

			got, err := f.Read(ctx, location)
			if err != nil || string(got) != "hello" {
				t.Fatalf("Read = %q, %v", got, err)
			}

			if err := f.Delete(ctx, location); err != nil {
				t.Fatalf("Delete: %v", err)
			}

			if _, err := f.Read(ctx, location); !errors.Is(err, fileio.ErrNotFound) {
				t.Fatalf("Read after delete error = %v, want ErrNotFound", err)
			}

			if err := f.Delete(ctx, location); err != nil {
				t.Fatalf("Delete of a missing file: %v", err)
			}
		})
	}
}

func TestIcebergUnknownScheme(t *testing.T) {
	t.Parallel()

	f := fileio.NewIceberg(context.Background(), nil)
	if _, err := f.Read(t.Context(), "nope://bucket/file"); err == nil {
		t.Fatal("Read with an unregistered scheme succeeded")
	}
}

// counting is a FileIO that counts reads.
type counting struct {
	mu    sync.Mutex
	files map[string][]byte
	reads int
}

func (c *counting) Read(_ context.Context, location string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reads++

	data, ok := c.files[location]
	if !ok {
		return nil, fileio.ErrNotFound
	}

	return data, nil
}

func (c *counting) Write(_ context.Context, location string, data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.files[location] = data

	return nil
}

func (c *counting) Delete(_ context.Context, location string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.files, location)

	return nil
}

func TestCachedServesRepeatedReads(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	next := &counting{files: map[string][]byte{"a": []byte("A"), "b": []byte("B"), "c": []byte("C")}}
	c := fileio.NewCached(next, 2)

	for _, loc := range []string{"a", "a", "b", "a"} {
		if _, err := c.Read(ctx, loc); err != nil {
			t.Fatalf("Read(%s): %v", loc, err)
		}
	}

	if next.reads != 2 {
		t.Fatalf("underlying reads = %d, want 2", next.reads)
	}

	// Reading c evicts the least recently used entry, b.
	_, _ = c.Read(ctx, "c")
	_, _ = c.Read(ctx, "b")

	if next.reads != 4 {
		t.Fatalf("underlying reads = %d, want 4 after eviction", next.reads)
	}
}

func TestCachedDropsWrittenAndDeletedFiles(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	next := &counting{files: map[string][]byte{"a": []byte("old")}}
	c := fileio.NewCached(next, 10)

	_, _ = c.Read(ctx, "a")

	if err := c.Write(ctx, "a", []byte("new")); err != nil {
		t.Fatal(err)
	}

	if got, _ := c.Read(ctx, "a"); string(got) != "new" {
		t.Fatalf("Read after Write = %q, want new", got)
	}

	if err := c.Delete(ctx, "a"); err != nil {
		t.Fatal(err)
	}

	if _, err := c.Read(ctx, "a"); !errors.Is(err, fileio.ErrNotFound) {
		t.Fatalf("Read after Delete error = %v, want ErrNotFound", err)
	}
}

func TestCachedErrorsAreNotCached(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	next := &counting{files: map[string][]byte{}}
	c := fileio.NewCached(next, 10)

	_, _ = c.Read(ctx, "missing")
	_ = next.Write(ctx, "missing", []byte("now here"))

	if got, err := c.Read(ctx, "missing"); err != nil || string(got) != "now here" {
		t.Fatalf("Read = %q, %v; a failed read must not be cached", got, err)
	}
}

func TestNewCachedDisabled(t *testing.T) {
	t.Parallel()

	next := &counting{files: map[string][]byte{}}
	if fileio.NewCached(next, 0) != fileio.FileIO(next) {
		t.Fatal("NewCached with size 0 must return the underlying FileIO")
	}
}
