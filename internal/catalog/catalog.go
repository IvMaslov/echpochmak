// Package catalog implements the Iceberg catalog operations on top of a
// [store.Store], which holds namespaces and table pointers, and a
// [fileio.FileIO], which holds table metadata files.
//
// The package has no HTTP types: the api package translates requests into
// calls here and errors from here into Iceberg error responses.
package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/google/uuid"

	"echpochmak/internal/fileio"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// Defaults for [Options].
const (
	DefaultCommitRetries = 4
	baseBackoff          = 10 * time.Millisecond
)

// Options configures a [Service].
type Options struct {
	Store store.Store
	Files fileio.FileIO
	// Warehouse is the base location of tables created without an explicit
	// location in a namespace without a location property.
	Warehouse string
	// CommitRetries is how many times a commit is retried after a concurrent
	// change, as long as its requirements still hold.
	CommitRetries int
	Logger        *slog.Logger

	// NewUUID generates table UUIDs. Defaults to random UUIDs; tests replace
	// it for deterministic locations.
	NewUUID func() uuid.UUID
	// Backoff returns the pause before retry attempt n (starting at 1).
	// Defaults to jittered exponential backoff.
	Backoff func(attempt int) time.Duration
}

// Service implements the catalog operations. It is safe for concurrent use
// and holds no state of its own: replicas sharing a store are equivalent.
type Service struct {
	store     store.Store
	files     fileio.FileIO
	warehouse string
	retries   int
	log       *slog.Logger
	newUUID   func() uuid.UUID
	backoff   func(attempt int) time.Duration
}

// New returns a Service.
func New(opts Options) *Service {
	s := &Service{
		store:     opts.Store,
		files:     opts.Files,
		warehouse: strings.TrimSuffix(opts.Warehouse, "/"),
		retries:   opts.CommitRetries,
		log:       opts.Logger,
		newUUID:   opts.NewUUID,
		backoff:   opts.Backoff,
	}

	if s.log == nil {
		s.log = slog.Default()
	}

	if s.newUUID == nil {
		s.newUUID = uuid.New
	}

	if s.backoff == nil {
		s.backoff = jitteredBackoff
	}

	return s
}

// logFor returns the request's logger, which carries the request ID, or the
// service logger outside of requests.
func (s *Service) logFor(ctx context.Context) *slog.Logger {
	return logger.FromContextOr(ctx, s.log)
}

// Ping reports whether the catalog can serve requests.
func (s *Service) Ping(ctx context.Context) error {
	return s.store.Ping(ctx)
}

// jitteredBackoff waits a random time up to baseBackoff * 2^(attempt-1), so
// competing writers spread out instead of colliding again.
func jitteredBackoff(attempt int) time.Duration {
	limit := baseBackoff << min(attempt-1, 6)

	return time.Duration(rand.Int64N(int64(limit))) //nolint:gosec // G404: jitter needs no cryptographic randomness.
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting to retry: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}
