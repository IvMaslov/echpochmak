package raftlog_test

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/raftlog"
)

// fixedSource reports a fixed snapshot, standing in for the state machine.
type fixedSource struct {
	meta *raftpb.SnapshotMetadata
}

func (f fixedSource) SnapshotMetadata() (*raftpb.SnapshotMetadata, error) {
	return f.meta, nil
}

var noSnapshot = fixedSource{meta: &raftpb.SnapshotMetadata{ConfState: &raftpb.ConfState{}}}

func openDB(t *testing.T, fs vfs.FS) *pebble.DB {
	t.Helper()

	db, err := pebble.Open("db", &pebble.Options{FS: fs})
	if err != nil {
		t.Fatalf("opening pebble: %v", err)
	}

	return db
}

func open(t *testing.T, db *pebble.DB) *raftlog.Storage {
	t.Helper()

	s, err := raftlog.Open(db, noSnapshot)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	return s
}

func newStorage(t *testing.T) *raftlog.Storage {
	t.Helper()

	db := openDB(t, vfs.NewMem())
	t.Cleanup(func() { _ = db.Close() })

	return open(t, db)
}

func entry(index, term uint64) *raftpb.Entry {
	return &raftpb.Entry{Index: new(index), Term: new(term), Data: []byte{byte(index)}}
}

// entries returns entries lo..hi (inclusive), all with the given term.
func entries(lo, hi, term uint64) []*raftpb.Entry {
	out := make([]*raftpb.Entry, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		out = append(out, entry(i, term))
	}

	return out
}

func save(t *testing.T, s *raftlog.Storage, es []*raftpb.Entry, sync bool) {
	t.Helper()

	if err := s.Save(&raftpb.HardState{}, es, sync); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func indexes(t *testing.T, es []*raftpb.Entry) []uint64 {
	t.Helper()

	out := make([]uint64, 0, len(es))
	for _, e := range es {
		out = append(out, e.GetIndex())
	}

	return out
}

func mustIndexes(t *testing.T, s *raftlog.Storage, first, last uint64) {
	t.Helper()

	gotFirst, _ := s.FirstIndex()
	gotLast, _ := s.LastIndex()

	if gotFirst != first || gotLast != last {
		t.Fatalf("FirstIndex, LastIndex = %d, %d; want %d, %d", gotFirst, gotLast, first, last)
	}
}

func TestEmpty(t *testing.T) {
	t.Parallel()

	s := newStorage(t)

	if !s.IsEmpty() {
		t.Fatal("new storage is not empty")
	}

	mustIndexes(t, s, 1, 0)

	if term, err := s.Term(0); err != nil || term != 0 {
		t.Fatalf("Term(0) = %d, %v; want 0, nil", term, err)
	}
}

func TestAppendAndEntries(t *testing.T) {
	t.Parallel()

	s := newStorage(t)
	save(t, s, entries(1, 5, 1), true)
	mustIndexes(t, s, 1, 5)

	es, err := s.Entries(2, 5, 1<<20)
	if err != nil || len(es) != 3 || es[0].GetIndex() != 2 {
		t.Fatalf("Entries(2, 5) = %v, %v", indexes(t, es), err)
	}

	// maxSize 0 still returns one entry.
	es, err = s.Entries(2, 5, 0)
	if err != nil || len(es) != 1 {
		t.Fatalf("Entries with maxSize 0 = %v, %v; want one entry", indexes(t, es), err)
	}

	if _, err := s.Entries(2, 7, 1<<20); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Entries beyond the last index: error %v, want ErrUnavailable", err)
	}

	if term, err := s.Term(3); err != nil || term != 1 {
		t.Fatalf("Term(3) = %d, %v", term, err)
	}

	if _, err := s.Term(6); !errors.Is(err, raft.ErrUnavailable) {
		t.Fatalf("Term(6) error = %v, want ErrUnavailable", err)
	}
}

func TestAppendReplacesConflictingTail(t *testing.T) {
	t.Parallel()

	s := newStorage(t)
	save(t, s, entries(1, 5, 1), true)

	// A new leader overwrites entries 3..5 with 3..4 of term 2.
	save(t, s, entries(3, 4, 2), true)
	mustIndexes(t, s, 1, 4)

	for i, want := range map[uint64]uint64{2: 1, 3: 2, 4: 2} {
		if term, err := s.Term(i); err != nil || term != want {
			t.Fatalf("Term(%d) = %d, %v; want %d", i, term, err, want)
		}
	}

	if err := s.Save(&raftpb.HardState{}, entries(7, 8, 2), true); err == nil {
		t.Fatal("appending with a gap succeeded")
	}
}

func TestCompact(t *testing.T) {
	t.Parallel()

	s := newStorage(t)
	save(t, s, entries(1, 10, 1), true)

	if err := s.Compact(4); err != nil {
		t.Fatalf("Compact(4): %v", err)
	}

	mustIndexes(t, s, 5, 10)

	if term, err := s.Term(4); err != nil || term != 1 {
		t.Fatalf("Term of the truncation point = %d, %v; want 1", term, err)
	}

	if _, err := s.Term(3); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Term(3) error = %v, want ErrCompacted", err)
	}

	if _, err := s.Entries(4, 6, 1<<20); !errors.Is(err, raft.ErrCompacted) {
		t.Fatalf("Entries(4, 6) error = %v, want ErrCompacted", err)
	}

	if err := s.Compact(2); err != nil {
		t.Fatalf("compacting below the truncation point: %v", err)
	}

	if err := s.Compact(11); err == nil {
		t.Fatal("compacting beyond the last index succeeded")
	}

	// Appending entries that are already compacted skips them.
	save(t, s, entries(3, 11, 1), true)
	mustIndexes(t, s, 5, 11)
}

func TestHardStateAndRestart(t *testing.T) {
	t.Parallel()

	db := openDB(t, vfs.NewMem())
	t.Cleanup(func() { _ = db.Close() })

	s := open(t, db)
	hs := &raftpb.HardState{Term: new(uint64(3)), Vote: new(uint64(2)), Commit: new(uint64(4))}

	if err := s.Save(hs, entries(1, 6, 3), true); err != nil {
		t.Fatal(err)
	}

	if err := s.Compact(2); err != nil {
		t.Fatal(err)
	}

	reopened := open(t, db)
	mustIndexes(t, reopened, 3, 6)

	got, _, err := reopened.InitialState()
	if err != nil || !proto.Equal(got, hs) {
		t.Fatalf("InitialState = %v, %v; want %v", got, err, hs)
	}

	if reopened.IsEmpty() {
		t.Fatal("storage with state reports empty")
	}
}

func TestResetToSnapshot(t *testing.T) {
	t.Parallel()

	db := openDB(t, vfs.NewMem())
	t.Cleanup(func() { _ = db.Close() })

	s := open(t, db)
	save(t, s, entries(1, 5, 1), true)

	meta := &raftpb.SnapshotMetadata{Index: new(uint64(20)), Term: new(uint64(4))}
	b := db.NewBatch()

	if err := s.ResetToSnapshot(b, meta); err != nil {
		t.Fatal(err)
	}

	if err := b.Commit(pebble.Sync); err != nil {
		t.Fatal(err)
	}

	s.ApplySnapshot(meta)
	mustIndexes(t, s, 21, 20)

	if term, err := s.Term(20); err != nil || term != 4 {
		t.Fatalf("Term(20) = %d, %v; want 4", term, err)
	}

	// The next entries follow the snapshot, and survive a restart.
	save(t, s, entries(21, 22, 4), true)
	mustIndexes(t, open(t, db), 21, 22)
}

func TestSnapshotComesFromTheSource(t *testing.T) {
	t.Parallel()

	db := openDB(t, vfs.NewMem())
	t.Cleanup(func() { _ = db.Close() })

	meta := &raftpb.SnapshotMetadata{
		Index:     new(uint64(7)),
		Term:      new(uint64(2)),
		ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
	}

	s, err := raftlog.Open(db, fixedSource{meta: meta})
	if err != nil {
		t.Fatal(err)
	}

	snap, err := s.Snapshot()
	if err != nil || !proto.Equal(snap.GetMetadata(), meta) || len(snap.GetData()) != 0 {
		t.Fatalf("Snapshot = %v, %v; want metadata %v without data", snap, err, meta)
	}

	_, cs, err := s.InitialState()
	if err != nil || !proto.Equal(cs, meta.GetConfState()) {
		t.Fatalf("InitialState conf state = %v, %v", cs, err)
	}
}

// TestCrashKeepsOnlySyncedWrites checks the durability contract raft relies
// on: what was saved with sync survives a crash, what was not may vanish.
func TestCrashKeepsOnlySyncedWrites(t *testing.T) {
	t.Parallel()

	fs := vfs.NewCrashableMem()
	db := openDB(t, fs)
	s := open(t, db)

	save(t, s, entries(1, 3, 1), true)
	save(t, s, entries(4, 6, 1), false)
	mustIndexes(t, s, 1, 6)

	crashed := fs.CrashClone(vfs.CrashCloneCfg{})
	_ = db.Close()

	after := openDB(t, crashed)
	t.Cleanup(func() { _ = after.Close() })

	mustIndexes(t, open(t, after), 1, 3)
}
