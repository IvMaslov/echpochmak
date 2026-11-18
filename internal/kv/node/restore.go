package node

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/raftlog"
	"echpochmak/internal/kv/statemachine"
)

// restoreMarker exists in a data directory while a restore writes it. A node
// refuses to start while it exists, so a restore that was interrupted can
// never start as an empty node and bootstrap a new, empty cluster.
const restoreMarker = "RESTORE_IN_PROGRESS"

// DefaultRevisionBump is how far a restore moves the revision forward. Writes
// after the snapshot are lost, but their revisions were handed out; the
// restored cluster continues above anything the old one can have issued, so
// a revision is never reused.
const DefaultRevisionBump = 1_000_000_000

// RestoreConfig configures [Restore].
type RestoreConfig struct {
	// ID is the restored node's ID; it must be one of Members.
	ID uint64
	// DataDir is the new node's data directory. It must not hold a database.
	DataDir string
	// FS is the filesystem of DataDir; defaults to the operating system's.
	FS vfs.FS
	// Members are the voters of the new cluster. Every node of the new
	// cluster is restored with the same members and the same snapshot.
	Members []*kvpb.Member
	// RevisionBump is added to the snapshot's revision; zero means
	// [DefaultRevisionBump].
	RevisionBump int64
	// Salt makes the new cluster ID differ from every other cluster with the
	// same members: pass the snapshot file's checksum, which every node of
	// the new cluster shares.
	Salt string
}

// Restore creates a node's data directory from a snapshot: data, the state
// machine's record stream, is imported, and the node starts as a member of a
// new cluster at the snapshot's index. The new cluster has its own ID, so a
// node of the old cluster is rejected by its peers. Restore returns the
// applied state it wrote.
//
// The caller verifies the snapshot file first: Restore checks what it reads
// but cannot undo what an interrupted run left behind, except by refusing to
// start a node from it.
func Restore(cfg RestoreConfig, data io.Reader) (*kvpb.AppliedState, error) {
	if cfg.FS == nil {
		cfg.FS = vfs.Default
	}

	if cfg.RevisionBump == 0 {
		cfg.RevisionBump = DefaultRevisionBump
	}

	if err := validateRestore(cfg); err != nil {
		return nil, err
	}

	if err := createDataDir(cfg.FS, cfg.DataDir); err != nil {
		return nil, err
	}

	marker := filepath.Join(cfg.DataDir, restoreMarker)
	if err := writeFileSynced(cfg.FS, marker, strings.NewReader("restore started\n")); err != nil {
		return nil, fmt.Errorf("marking the restore: %w", err)
	}

	state, err := restoreDB(cfg, data)
	if err != nil {
		return nil, err
	}

	if err := cfg.FS.Remove(marker); err != nil {
		return nil, fmt.Errorf("removing the restore marker: %w", err)
	}

	if err := syncDir(cfg.FS, cfg.DataDir); err != nil {
		return nil, err
	}

	return state, nil
}

func validateRestore(cfg RestoreConfig) error {
	if cfg.ID == 0 {
		return errors.New("node ID must not be zero")
	}

	if !slices.ContainsFunc(cfg.Members, func(m *kvpb.Member) bool { return m.GetId() == cfg.ID }) {
		return fmt.Errorf("the new cluster's members do not include node %d", cfg.ID)
	}

	if cfg.RevisionBump < 0 {
		return errors.New("the revision bump must not be negative")
	}

	hasState, err := HasState(cfg.FS, cfg.DataDir)
	if err != nil {
		return err
	}

	if hasState {
		return fmt.Errorf("data directory %s already holds a database; restore into an empty one", cfg.DataDir)
	}

	if _, err := cfg.FS.Stat(filepath.Join(cfg.DataDir, restoreMarker)); err == nil {
		return fmt.Errorf("data directory %s holds an interrupted restore; empty it first", cfg.DataDir)
	}

	return nil
}

// restoreDB writes the database of a restored node in one synced batch.
func restoreDB(cfg RestoreConfig, data io.Reader) (*kvpb.AppliedState, error) {
	db, err := pebble.Open(filepath.Join(cfg.DataDir, dbDir), &pebble.Options{FS: cfg.FS, Logger: pebbleLogger{}})
	if err != nil {
		return nil, fmt.Errorf("opening database: %w", err)
	}
	defer db.Close()

	sm, err := statemachine.Open(db, statemachine.Options{})
	if err != nil {
		return nil, err
	}

	b := db.NewBatch()
	defer b.Close()

	state, err := sm.ImportSnapshot(b, data)
	if err != nil {
		return nil, fmt.Errorf("importing the snapshot: %w", err)
	}

	state, meta, err := restoredState(state, cfg)
	if err != nil {
		return nil, err
	}

	if err := statemachine.WriteApplied(b, state); err != nil {
		return nil, err
	}

	hs := &raftpb.HardState{Term: new(meta.GetTerm()), Commit: new(meta.GetIndex())}
	if err := raftlog.WriteRestored(b, meta, hs); err != nil {
		return nil, err
	}

	clusterID := restoredClusterID(cfg.Members, cfg.Salt)
	if err := b.Set([]byte(keyClusterID), []byte(clusterID), nil); err != nil {
		return nil, fmt.Errorf("writing cluster ID: %w", err)
	}

	if err := b.Commit(pebble.Sync); err != nil {
		return nil, fmt.Errorf("committing the restored state: %w", err)
	}

	return state, nil
}

// restoredState rewrites an imported applied state for the new cluster: its
// members and conf state, and the revision bump. It returns the state and the
// raft snapshot metadata it corresponds to.
func restoredState(
	imported *kvpb.AppliedState,
	cfg RestoreConfig,
) (*kvpb.AppliedState, *raftpb.SnapshotMetadata, error) {
	if imported.GetIndex() == 0 {
		return nil, nil, errors.New("the snapshot has no applied entries")
	}

	members := make([]*kvpb.Member, 0, len(cfg.Members))
	voters := make([]uint64, 0, len(cfg.Members))

	for _, m := range cfg.Members {
		members = append(members, &kvpb.Member{Id: m.GetId(), PeerUrl: m.GetPeerUrl()})
		voters = append(voters, m.GetId())
	}

	slices.Sort(voters)

	cs := &raftpb.ConfState{Voters: voters}

	rawCS, err := proto.Marshal(cs)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding conf state: %w", err)
	}

	state := proto.CloneOf(imported)
	state.Members = members
	state.ConfState = rawCS
	state.Revision = imported.GetRevision() + cfg.RevisionBump

	meta := &raftpb.SnapshotMetadata{
		Index:     new(state.GetIndex()),
		Term:      new(state.GetTerm()),
		ConfState: cs,
	}

	return state, meta, nil
}

// restoredClusterID derives the ID of a restored cluster from its members
// and a salt, so every node restored from the same snapshot computes the
// same ID, and it differs from the bootstrapped cluster with those members.
func restoredClusterID(members []*kvpb.Member, salt string) string {
	parts := make([]string, 0, len(members))
	for _, m := range members {
		parts = append(parts, strconv.FormatUint(m.GetId(), 10)+"="+m.GetPeerUrl())
	}

	slices.Sort(parts)
	sum := sha256.Sum256([]byte("restored|" + strings.Join(parts, ",") + "|" + salt))

	return hex.EncodeToString(sum[:8])
}

// checkRestoreMarker fails if dataDir holds an interrupted restore.
func checkRestoreMarker(fs vfs.FS, dataDir string) error {
	_, err := fs.Stat(filepath.Join(dataDir, restoreMarker))

	switch {
	case err == nil:
		return fmt.Errorf("data directory %s holds an interrupted restore; empty it and restore again", dataDir)
	case errors.Is(err, os.ErrNotExist):
		return nil
	default:
		return fmt.Errorf("checking for an interrupted restore: %w", err)
	}
}

// ExportedState describes a snapshot written by [ExportDataDir].
type ExportedState struct {
	ClusterID string
	State     *kvpb.AppliedState
}

// ExportDataDir writes the replicated state of a stopped node, read from its
// data directory, to w as a record stream. It is the recovery path when a
// cluster lost its quorum for good: the node may lack the last entries the
// lost majority committed.
func ExportDataDir(fs vfs.FS, dataDir string, w io.Writer) (ExportedState, error) {
	if fs == nil {
		fs = vfs.Default
	}

	hasState, err := HasState(fs, dataDir)
	if err != nil {
		return ExportedState{}, err
	}

	if !hasState {
		return ExportedState{}, fmt.Errorf("data directory %s holds no database", dataDir)
	}

	db, err := pebble.Open(filepath.Join(dataDir, dbDir), &pebble.Options{
		FS:       fs,
		Logger:   pebbleLogger{},
		ReadOnly: true,
	})
	if err != nil {
		return ExportedState{}, fmt.Errorf("opening database (is the node still running?): %w", err)
	}
	defer db.Close()

	raw, closer, err := db.Get([]byte(keyClusterID))
	if err != nil {
		return ExportedState{}, fmt.Errorf("reading cluster ID: %w", err)
	}

	clusterID := string(raw)
	_ = closer.Close()

	sm, err := statemachine.Open(db, statemachine.Options{})
	if err != nil {
		return ExportedState{}, err
	}

	state, err := sm.ExportSnapshot(w)
	if err != nil {
		return ExportedState{}, fmt.Errorf("exporting state: %w", err)
	}

	return ExportedState{ClusterID: clusterID, State: state}, nil
}
