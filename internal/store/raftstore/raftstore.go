// Package raftstore implements [store.Store] on the raft-replicated KV
// cluster, through its Go client.
//
// Every write is one KV transaction: its compares are the operation's
// preconditions, and its failure branch reads the keys involved, so a
// failure is classified from the same consistent state that made the
// transaction fail, never from a second, racy read. A key's mod revision is
// the [store.Version] of what it holds: store-wide, only growing, and never
// reused.
//
// The key layout is described in arch/phase3/plan.md (D1).
package raftstore

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/store"
)

// TxnBudget is the number of compares, and of operations per branch, that the
// KV store accepts in one transaction. It bounds CommitTables and GetTables:
// in a commit, an update costs one compare and one read, a create three of
// each, and a commit intent one more. So one transaction holds up to 127
// updates or 42 creates with an intent, 128 updates without.
const TxnBudget = 128

// Costs against [TxnBudget]: of one commit, and of the intent of a call.
const (
	updateCost = 1
	createCost = 3
	intentCost = 1
)

// casRetries bounds the read-modify-write loops of property updates and
// renames, which retry when a concurrent write wins the race.
const casRetries = 8

// Options configures a Store.
type Options struct {
	// Prefix is the key prefix of this catalog's state; [DefaultPrefix] when
	// empty. Stores with different prefixes share a cluster independently.
	Prefix string
	// CallTimeout bounds each KV call, the client's retries included. Zero
	// leaves the caller's context as the only bound.
	CallTimeout time.Duration
}

// Store is a [store.Store] on the KV cluster. It is safe for concurrent use,
// and any number of Stores, in any number of processes, can share a cluster.
type Store struct {
	c       *client.Client
	k       keys
	timeout time.Duration
}

var _ store.Store = (*Store)(nil)

// New returns a Store on the cluster c connects to. The caller owns c and
// closes it.
func New(c *client.Client, opts Options) *Store {
	if opts.Prefix == "" {
		opts.Prefix = DefaultPrefix
	}

	return &Store{c: c, k: keys{prefix: opts.Prefix}, timeout: opts.CallTimeout}
}

// bound applies the call timeout to ctx.
func (s *Store) bound(ctx context.Context) (context.Context, context.CancelFunc) {
	if s.timeout <= 0 {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, s.timeout)
}

// Ping implements [store.Store]: a linearizable read, which succeeds only
// while a quorum of the cluster is reachable.
func (s *Store) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("raftstore: %w", err)
	}

	if _, err := s.get(ctx, s.k.ping()); err != nil {
		return err
	}

	return nil
}

// mapErr translates a KV client error into the store's error model. A write
// whose outcome is unknown becomes [store.ErrOutcomeUnknown], which also
// matches [store.ErrUnavailable]; a cluster that could not be reached at all
// is [store.ErrUnavailable] alone: the write certainly did not happen. The
// original error stays wrapped, so context errors still match.
func mapErr(err error) error {
	switch {
	case errors.Is(err, client.ErrInvalidArgument):
		return fmt.Errorf("%w: %w", store.ErrInvalidArgument, err)
	case errors.Is(err, client.ErrOutcomeUnknown):
		return store.OutcomeUnknown(err)
	case errors.Is(err, client.ErrUnavailable):
		return fmt.Errorf("%w: %w", store.ErrUnavailable, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("raftstore: %w", err)
	default:
		return fmt.Errorf("raftstore: %w", err)
	}
}

// txn runs a transaction and maps its error.
func (s *Store) txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()

	resp, err := s.c.Txn(ctx, req)
	if err != nil {
		return nil, mapErr(err)
	}

	return resp, nil
}

// get reads one key, linearizably; a missing key is nil.
func (s *Store) get(ctx context.Context, key []byte) (*kvpb.KeyValue, error) {
	ctx, cancel := s.bound(ctx)
	defer cancel()

	kv, err := s.c.Get(ctx, key)
	if err != nil {
		return nil, mapErr(err)
	}

	return kv, nil
}

// pause waits before retry attempt+1 of a lost compare-and-swap: a random
// time up to 2^attempt milliseconds, so competing writers spread out.
func pause(ctx context.Context, attempt int) error {
	limit := time.Millisecond << min(attempt, 8)
	wait := time.Duration(rand.Int64N(int64(limit))) //nolint:gosec // G404: jitter needs no cryptographic randomness.
	t := time.NewTimer(wait)

	defer t.Stop()

	select {
	case <-ctx.Done():
		return fmt.Errorf("raftstore: %w", ctx.Err())
	case <-t.C:
		return nil
	}
}

func checkCtx(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("raftstore: %w", err)
	}

	return nil
}

// Compare and operation helpers, so transactions read like their intent.

func absent(key []byte) *kvpb.Compare {
	return client.CompareModRevision(key, kvpb.Compare_RESULT_EQUAL, 0)
}

func present(key []byte) *kvpb.Compare {
	return client.CompareModRevision(key, kvpb.Compare_RESULT_GREATER, 0)
}

func atRevision(key []byte, rev int64) *kvpb.Compare {
	return client.CompareModRevision(key, kvpb.Compare_RESULT_EQUAL, rev)
}

func emptyRange(prefix []byte) *kvpb.Compare {
	return client.CompareRangeCount(prefix, prefixEnd(prefix), kvpb.Compare_RESULT_EQUAL, 0)
}

func read(key []byte) *kvpb.RequestOp {
	return client.OpRange(key, nil, 0)
}

// readResult returns the key-value read by the i-th operation of a branch,
// or nil if the key did not exist.
func readResult(resp *kvpb.TxnResponse, i int) *kvpb.KeyValue {
	kvs := resp.GetResponses()[i].GetResponseRange().GetKvs()
	if len(kvs) == 0 {
		return nil
	}

	return kvs[0]
}
