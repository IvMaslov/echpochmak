package node

import (
	"context"
	"fmt"
	"sync"
)

// waiters connects a request to the event that completes it, by key: a
// proposal to its applied result, a read to its read index.
type waiters[T any] struct {
	mu sync.Mutex
	m  map[string][]chan T
}

func newWaiters[T any]() *waiters[T] {
	return &waiters[T]{m: map[string][]chan T{}}
}

// register returns a channel that receives the value for key once.
func (w *waiters[T]) register(key string) chan T {
	ch := make(chan T, 1)

	w.mu.Lock()
	w.m[key] = append(w.m[key], ch)
	w.mu.Unlock()

	return ch
}

// cancel unregisters a channel that is no longer waited on.
func (w *waiters[T]) cancel(key string, ch chan T) {
	w.mu.Lock()
	defer w.mu.Unlock()

	chans := w.m[key]
	for i, c := range chans {
		if c == ch {
			chans = append(chans[:i], chans[i+1:]...)

			break
		}
	}

	if len(chans) == 0 {
		delete(w.m, key)
	} else {
		w.m[key] = chans
	}
}

// trigger delivers v to everything waiting on key.
func (w *waiters[T]) trigger(key string, v T) {
	w.mu.Lock()
	chans := w.m[key]
	delete(w.m, key)
	w.mu.Unlock()

	for _, ch := range chans {
		ch <- v
	}
}

// progress tracks a monotonically increasing index, such as the applied
// index, and lets goroutines wait for it to reach a value.
type progress struct {
	mu      sync.Mutex
	index   uint64
	changed chan struct{}
}

func newProgress(index uint64) *progress {
	return &progress{index: index, changed: make(chan struct{})}
}

func (p *progress) set(index uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if index <= p.index {
		return
	}

	p.index = index
	close(p.changed)
	p.changed = make(chan struct{})
}

// wait blocks until the index reaches at least index, or ctx or stop is done.
func (p *progress) wait(ctx context.Context, stop <-chan struct{}, index uint64) error {
	for {
		p.mu.Lock()
		reached, changed := p.index >= index, p.changed
		p.mu.Unlock()

		if reached {
			return nil
		}

		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("waiting for index %d: %w", index, ctx.Err())
		case <-stop:
			return ErrUnavailable
		}
	}
}
