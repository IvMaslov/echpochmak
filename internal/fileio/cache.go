package fileio

import (
	"container/list"
	"context"
	"fmt"
	"sync"
)

// Cached decorates a FileIO with an LRU cache of file contents.
//
// The catalog never rewrites a metadata file: every commit writes a new file
// under a new, unique name. File contents are therefore immutable for a given
// location and the cache never needs invalidation; Write and Delete only keep
// it from serving files the catalog itself has replaced or removed.
type Cached struct {
	next FileIO
	size int

	mu    sync.Mutex
	order *list.List // front is most recently used; values are *cacheEntry
	items map[string]*list.Element
}

type cacheEntry struct {
	location string
	data     []byte
}

var _ FileIO = (*Cached)(nil)

// NewCached returns next with an LRU cache of up to size files in front of
// its reads. A size of zero or less disables caching and returns next.
func NewCached(next FileIO, size int) FileIO {
	if size <= 0 {
		return next
	}

	return &Cached{
		next:  next,
		size:  size,
		order: list.New(),
		items: make(map[string]*list.Element, size),
	}
}

// Read implements [FileIO]. Callers must not modify the returned slice.
func (c *Cached) Read(ctx context.Context, location string) ([]byte, error) {
	if data, ok := c.get(location); ok {
		return data, nil
	}

	data, err := c.next.Read(ctx, location)
	if err != nil {
		return nil, err
	}

	c.put(location, data)

	return data, nil
}

// Write implements [FileIO].
func (c *Cached) Write(ctx context.Context, location string, data []byte) error {
	c.remove(location)

	if err := c.next.Write(ctx, location, data); err != nil {
		return err
	}

	return nil
}

// Delete implements [FileIO].
func (c *Cached) Delete(ctx context.Context, location string) error {
	c.remove(location)

	return c.next.Delete(ctx, location)
}

func (c *Cached) get(location string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.items[location]
	if !ok {
		return nil, false
	}

	c.order.MoveToFront(elem)

	return entryOf(elem).data, true
}

func (c *Cached) put(location string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[location]; ok {
		c.order.MoveToFront(elem)

		return
	}

	c.items[location] = c.order.PushFront(&cacheEntry{location: location, data: data})

	for c.order.Len() > c.size {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.items, entryOf(oldest).location)
	}
}

func (c *Cached) remove(location string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.items[location]; ok {
		c.order.Remove(elem)
		delete(c.items, location)
	}
}

// entryOf returns the cache entry held by elem; the list holds nothing else.
func entryOf(elem *list.Element) *cacheEntry {
	entry, ok := elem.Value.(*cacheEntry)
	if !ok {
		panic(fmt.Sprintf("fileio: unexpected cache element %T", elem.Value))
	}

	return entry
}
