package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/statemachine"
)

// snapshotDir holds snapshots received from the leader until they are
// applied.
const snapshotDir = "snapshots"

// OutgoingSnapshot is a snapshot of this node's state, ready to stream to a
// follower. Its metadata matches its data.
type OutgoingSnapshot struct {
	export *statemachine.Export
	meta   *raftpb.SnapshotMetadata
}

// Metadata returns the index, term and configuration of the snapshot.
func (s *OutgoingSnapshot) Metadata() *raftpb.SnapshotMetadata {
	return proto.CloneOf(s.meta)
}

// WriteTo streams the snapshot data.
func (s *OutgoingSnapshot) WriteTo(w io.Writer) (int64, error) {
	n, err := s.export.WriteTo(w)
	if err != nil {
		return n, fmt.Errorf("streaming snapshot: %w", err)
	}

	return n, nil
}

// Close releases the snapshot.
func (s *OutgoingSnapshot) Close() {
	s.export.Close()
}

// OpenSnapshot captures the current state for a follower that needs a
// snapshot. The state may be newer than the snapshot raft asked for; the
// transport sends this snapshot's metadata, which describes the data that
// is actually sent.
func (n *Node) OpenSnapshot() (*OutgoingSnapshot, error) {
	export, err := n.sm.NewExport()
	if err != nil {
		return nil, fmt.Errorf("opening snapshot: %w", err)
	}

	meta, err := snapshotMetadata(export.State())
	if err != nil {
		export.Close()

		return nil, err
	}

	return &OutgoingSnapshot{export: export, meta: meta}, nil
}

// ReceiveSnapshot stages the data of a snapshot sent by the leader, then
// passes the snapshot message to raft. msg is the MsgSnap, and data must be
// verified by the caller. The state is replaced once raft accepts the
// snapshot and the main loop applies it.
func (n *Node) ReceiveSnapshot(ctx context.Context, msg *raftpb.Message, data io.Reader) error {
	meta := msg.GetSnapshot().GetMetadata()
	name := fmt.Sprintf("%016x-%016x.snap", meta.GetTerm(), meta.GetIndex())
	path := filepath.Join(n.cfg.DataDir, snapshotDir, name)

	if err := writeFileSynced(n.cfg.FS, path, data); err != nil {
		return fmt.Errorf("staging snapshot: %w", err)
	}

	msg = proto.CloneOf(msg)
	msg.Snapshot.Data = []byte(path)

	n.log.InfoContext(ctx, "snapshot received",
		slog.Uint64("from", msg.GetFrom()),
		slog.Uint64("index", meta.GetIndex()),
	)

	return n.Step(ctx, msg)
}

func writeFileSynced(fs vfs.FS, path string, data io.Reader) error {
	f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}

	if _, err := io.Copy(f, data); err != nil {
		_ = f.Close()
		_ = fs.Remove(path)

		return fmt.Errorf("writing %s: %w", path, err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return fmt.Errorf("syncing %s: %w", path, err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing %s: %w", path, err)
	}

	return nil
}

// cleanSnapshotDir removes staged snapshots left by a previous run: raft
// asks for a fresh one if it still needs it.
func cleanSnapshotDir(fs vfs.FS, dataDir string, log *slog.Logger) {
	dir := filepath.Join(dataDir, snapshotDir)

	names, err := fs.List(dir)
	if err != nil {
		return
	}

	for _, name := range names {
		if !strings.HasSuffix(name, ".snap") {
			continue
		}

		if err := fs.Remove(filepath.Join(dir, name)); err != nil {
			log.Warn("removing stale snapshot failed", slog.String("file", name), slog.Any("error", err))
		}
	}
}
