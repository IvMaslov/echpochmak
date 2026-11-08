// Package statemachine is the replicated state of the KV cluster: an ordered
// key-value store with revisions, stored in the node's Pebble database.
//
// Raft decides the order of writes; this package applies them. Applying must
// be deterministic: the result of an entry depends only on the state and the
// entry, never on clocks, randomness or map order, so every node computes the
// same state and the same results.
//
// The state lives under its own key prefix, so it can share a database with
// the raft log. Each applied batch also stores the applied index, term and
// conf state next to the data, which keeps snapshots self-consistent.
package statemachine

import (
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// DefaultDedupWindow is how many revisions a write's result is remembered for
// deduplication.
const DefaultDedupWindow = 100_000

// Options configures a StateMachine.
type Options struct {
	Limits Limits
	// DedupWindow is how many revisions a write's result is remembered for
	// deduplication. Clients must not retry a request ID after that many
	// writes have happened since.
	DedupWindow int64
}

// StateMachine applies committed writes and serves reads.
type StateMachine struct {
	db     *pebble.DB
	limits Limits
	window int64

	mu      sync.RWMutex
	applied *kvpb.AppliedState
}

// Open loads the state machine stored in db.
func Open(db *pebble.DB, opts Options) (*StateMachine, error) {
	if opts.Limits == (Limits{}) {
		opts.Limits = DefaultLimits()
	}

	if opts.DedupWindow <= 0 {
		opts.DedupWindow = DefaultDedupWindow
	}

	applied, err := readAppliedState(db)
	if err != nil {
		return nil, err
	}

	return &StateMachine{
		db:      db,
		limits:  opts.Limits,
		window:  opts.DedupWindow,
		applied: applied,
	}, nil
}

// Limits returns the request limits of the state machine.
func (sm *StateMachine) Limits() Limits {
	return sm.limits
}

// Applied returns a copy of the applied state.
func (sm *StateMachine) Applied() *kvpb.AppliedState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return proto.CloneOf(sm.applied)
}

// AppliedIndex returns the index of the last applied entry.
func (sm *StateMachine) AppliedIndex() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.applied.GetIndex()
}

// SetApplied replaces the in-memory applied state after a snapshot was
// imported and committed by the caller.
func (sm *StateMachine) SetApplied(state *kvpb.AppliedState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.applied = proto.CloneOf(state)
}

func readAppliedState(r pebble.Reader) (*kvpb.AppliedState, error) {
	raw, closer, err := r.Get([]byte(keyApplied))
	if errors.Is(err, pebble.ErrNotFound) {
		return &kvpb.AppliedState{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("reading applied state: %w", err)
	}
	defer closer.Close()

	state := &kvpb.AppliedState{}
	if err := proto.Unmarshal(raw, state); err != nil {
		return nil, fmt.Errorf("decoding applied state: %w", err)
	}

	return state, nil
}
