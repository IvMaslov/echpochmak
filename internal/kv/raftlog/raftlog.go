// Package raftlog stores the raft log and hard state of a node in its Pebble
// database and serves them to the raft library through [raft.Storage].
//
// The log only needs to hold entries that are not yet applied, or that slow
// followers still need: the state machine itself is durable. Compact drops
// the entries before a point; a follower that needs older entries gets a
// snapshot of the state machine instead.
package raftlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Key layout of the raft log inside the node's Pebble database.
const (
	prefixLog    = "r/log/"
	keyHardState = "r/hardstate"
	keyTruncated = "r/truncated"
)

// SnapshotSource describes the state machine to the log: the snapshot raft
// would get now. Its conf state is the configuration as of the applied index.
type SnapshotSource interface {
	SnapshotMetadata() (*raftpb.SnapshotMetadata, error)
}

// Storage is the raft log of a node. It is safe for concurrent use: the raft
// library reads it from its own goroutine while the node writes to it.
type Storage struct {
	db     *pebble.DB
	source SnapshotSource

	mu        sync.RWMutex
	hardState *raftpb.HardState
	// truncatedIndex and truncatedTerm identify the last entry dropped by
	// compaction, or the snapshot the log starts after.
	truncatedIndex uint64
	truncatedTerm  uint64
	lastIndex      uint64
}

var _ raft.Storage = (*Storage)(nil)

// Open loads the raft log stored in db.
func Open(db *pebble.DB, source SnapshotSource) (*Storage, error) {
	s := &Storage{db: db, source: source, hardState: &raftpb.HardState{}}

	if err := getProto(db, []byte(keyHardState), s.hardState); err != nil {
		return nil, err
	}

	if err := s.loadTruncated(); err != nil {
		return nil, err
	}

	last, err := s.findLastIndex()
	if err != nil {
		return nil, err
	}

	s.lastIndex = last

	return s, nil
}

// IsEmpty reports whether the node has never stored any raft state.
func (s *Storage) IsEmpty() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return raft.IsEmptyHardState(s.hardState) && s.lastIndex == 0 && s.truncatedIndex == 0
}

// InitialState implements [raft.Storage].
func (s *Storage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	meta, err := s.source.SnapshotMetadata()
	if err != nil {
		return nil, nil, fmt.Errorf("reading conf state: %w", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return proto.CloneOf(s.hardState), proto.CloneOf(meta.GetConfState()), nil
}

// Entries implements [raft.Storage]. It returns at least one entry even if it
// is larger than maxSize.
func (s *Storage) Entries(lo, hi, maxSize uint64) ([]*raftpb.Entry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if lo <= s.truncatedIndex {
		return nil, raft.ErrCompacted
	}

	if hi > s.lastIndex+1 {
		return nil, fmt.Errorf("entries up to %d requested, but the last is %d: %w",
			hi, s.lastIndex, raft.ErrUnavailable)
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: logKey(lo), UpperBound: logKey(hi)})
	if err != nil {
		return nil, fmt.Errorf("opening log iterator: %w", err)
	}
	defer iter.Close()

	entries := make([]*raftpb.Entry, 0, min(hi-lo, 64))

	var size uint64

	for valid := iter.First(); valid; valid = iter.Next() {
		e := &raftpb.Entry{}
		if err := proto.Unmarshal(iter.Value(), e); err != nil {
			return nil, fmt.Errorf("decoding log entry: %w", err)
		}

		size += uint64(proto.Size(e)) //nolint:gosec // G115: sizes are never negative.
		if len(entries) > 0 && size > maxSize {
			break
		}

		entries = append(entries, e)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("reading log: %w", err)
	}

	return entries, nil
}

// Term implements [raft.Storage].
func (s *Storage) Term(i uint64) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch {
	case i == s.truncatedIndex:
		return s.truncatedTerm, nil
	case i < s.truncatedIndex:
		return 0, raft.ErrCompacted
	case i > s.lastIndex:
		return 0, raft.ErrUnavailable
	}

	e := &raftpb.Entry{}
	if err := getProto(s.db, logKey(i), e); err != nil {
		return 0, err
	}

	return e.GetTerm(), nil
}

// LastIndex implements [raft.Storage].
func (s *Storage) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.lastIndex, nil
}

// FirstIndex implements [raft.Storage].
func (s *Storage) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.truncatedIndex + 1, nil
}

// Snapshot implements [raft.Storage]. It describes the state machine as it
// is now and carries no data: the transport streams the data when it sends
// the snapshot, from a view that matches this metadata or is newer.
func (s *Storage) Snapshot() (*raftpb.Snapshot, error) {
	meta, err := s.source.SnapshotMetadata()
	if err != nil {
		return nil, fmt.Errorf("reading snapshot metadata: %w", err)
	}

	return &raftpb.Snapshot{Metadata: meta}, nil
}

// Save persists the hard state and new entries from a raft Ready, in one
// batch. Entries that overlap the log replace its tail. With mustSync set, the
// write is durable before Save returns, which raft requires before sending
// the messages of the same Ready.
func (s *Storage) Save(hs *raftpb.HardState, entries []*raftpb.Entry, mustSync bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.db.NewBatch()
	defer b.Close()

	if !raft.IsEmptyHardState(hs) {
		if err := setProto(b, []byte(keyHardState), hs); err != nil {
			return err
		}
	}

	last, err := s.appendEntries(b, entries)
	if err != nil {
		return err
	}

	opts := pebble.NoSync
	if mustSync {
		opts = pebble.Sync
	}

	if err := b.Commit(opts); err != nil {
		return fmt.Errorf("committing raft log: %w", err)
	}

	if !raft.IsEmptyHardState(hs) {
		s.hardState = proto.CloneOf(hs)
	}

	s.lastIndex = last

	return nil
}

// appendEntries adds entries to b and returns the new last index.
func (s *Storage) appendEntries(b *pebble.Batch, entries []*raftpb.Entry) (uint64, error) {
	// Entries the log has already compacted away are committed; skip them.
	for len(entries) > 0 && entries[0].GetIndex() <= s.truncatedIndex {
		entries = entries[1:]
	}

	if len(entries) == 0 {
		return s.lastIndex, nil
	}

	first := entries[0].GetIndex()
	if first > s.lastIndex+1 {
		return 0, fmt.Errorf("appending entry %d would leave a gap after %d", first, s.lastIndex)
	}

	// Replace any conflicting tail.
	if first <= s.lastIndex {
		if err := b.DeleteRange(logKey(first), logKey(s.lastIndex+1), nil); err != nil {
			return 0, fmt.Errorf("truncating log tail: %w", err)
		}
	}

	for _, e := range entries {
		if err := setProto(b, logKey(e.GetIndex()), e); err != nil {
			return 0, err
		}
	}

	return entries[len(entries)-1].GetIndex(), nil
}

// Compact drops the entries up to and including index, which must be
// applied. Compacting an index that is already compacted does nothing.
func (s *Storage) Compact(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index <= s.truncatedIndex {
		return nil
	}

	if index > s.lastIndex {
		return fmt.Errorf("compacting to %d, beyond the last index %d", index, s.lastIndex)
	}

	e := &raftpb.Entry{}
	if err := getProto(s.db, logKey(index), e); err != nil {
		return err
	}

	b := s.db.NewBatch()
	defer b.Close()

	if err := b.DeleteRange(logKey(s.truncatedIndex+1), logKey(index+1), nil); err != nil {
		return fmt.Errorf("compacting log: %w", err)
	}

	if err := b.Set([]byte(keyTruncated), encodeTruncated(index, e.GetTerm()), nil); err != nil {
		return fmt.Errorf("writing truncation point: %w", err)
	}

	if err := b.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing compaction: %w", err)
	}

	s.truncatedIndex, s.truncatedTerm = index, e.GetTerm()

	return nil
}

// ResetToSnapshot adds to b the writes that make the log start after a
// snapshot: every entry is dropped. The caller commits b, synced, together
// with the state machine import, then calls [Storage.ApplySnapshot].
func (s *Storage) ResetToSnapshot(b *pebble.Batch, meta *raftpb.SnapshotMetadata) error {
	if err := b.DeleteRange([]byte(prefixLog), prefixEnd(prefixLog), nil); err != nil {
		return fmt.Errorf("clearing log: %w", err)
	}

	if err := b.Set([]byte(keyTruncated), encodeTruncated(meta.GetIndex(), meta.GetTerm()), nil); err != nil {
		return fmt.Errorf("writing truncation point: %w", err)
	}

	return nil
}

// ApplySnapshot updates the in-memory view after the batch from
// [Storage.ResetToSnapshot] was committed.
func (s *Storage) ApplySnapshot(meta *raftpb.SnapshotMetadata) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.truncatedIndex, s.truncatedTerm = meta.GetIndex(), meta.GetTerm()
	s.lastIndex = meta.GetIndex()
}

func (s *Storage) loadTruncated() error {
	raw, closer, err := s.db.Get([]byte(keyTruncated))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("reading truncation point: %w", err)
	}
	defer closer.Close()

	if len(raw) != 16 {
		return fmt.Errorf("corrupt truncation point of %d bytes", len(raw))
	}

	s.truncatedIndex = binary.BigEndian.Uint64(raw[:8])
	s.truncatedTerm = binary.BigEndian.Uint64(raw[8:])

	return nil
}

func (s *Storage) findLastIndex() (uint64, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte(prefixLog), UpperBound: prefixEnd(prefixLog)})
	if err != nil {
		return 0, fmt.Errorf("opening log iterator: %w", err)
	}
	defer iter.Close()

	if !iter.Last() {
		if err := iter.Error(); err != nil {
			return 0, fmt.Errorf("reading log: %w", err)
		}

		return s.truncatedIndex, nil
	}

	return binary.BigEndian.Uint64(iter.Key()[len(prefixLog):]), nil
}

func logKey(index uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(prefixLog), index)
}

func encodeTruncated(index, term uint64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, index), term)
}

// prefixEnd returns the smallest key greater than every key with prefix p,
// which must not end in 0xff.
func prefixEnd(p string) []byte {
	end := []byte(p)
	end[len(end)-1]++

	return end
}

func getProto(r pebble.Reader, key []byte, m proto.Message) error {
	raw, closer, err := r.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil
	}

	if err != nil {
		return fmt.Errorf("reading %q: %w", key, err)
	}
	defer closer.Close()

	if err := proto.Unmarshal(raw, m); err != nil {
		return fmt.Errorf("decoding %q: %w", key, err)
	}

	return nil
}

func setProto(b *pebble.Batch, key []byte, m proto.Message) error {
	raw, err := proto.Marshal(m)
	if err != nil {
		return fmt.Errorf("encoding %q: %w", key, err)
	}

	if err := b.Set(key, raw, nil); err != nil {
		return fmt.Errorf("writing %q: %w", key, err)
	}

	return nil
}
