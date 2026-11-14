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
	"time"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/store"
)

// MaxCommitsPerTxn bounds CommitTables. A create needs three compares, and the
// KV store accepts 128 compares and 128 operations per branch.
const MaxCommitsPerTxn = 42

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
// whose outcome is unknown, and a cluster that could not be reached, both
// become [store.ErrUnavailable]: callers must assume the write may have
// happened. The original error stays wrapped, so context errors still match.
func mapErr(err error) error {
	switch {
	case errors.Is(err, client.ErrInvalidArgument):
		return fmt.Errorf("%w: %w", store.ErrInvalidArgument, err)
	case errors.Is(err, client.ErrOutcomeUnknown):
		return fmt.Errorf("%w: write outcome unknown: %w", store.ErrUnavailable, err)
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
