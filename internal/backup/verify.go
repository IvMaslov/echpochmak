package backup

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/cockroachdb/pebble/v2/vfs"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/snapshot"
)

// Verify proves that a backup can be restored: it downloads the snapshot,
// checks it against its manifest and its own checksum, restores it into an
// in-memory node, and compares the restored state with the manifest.
func (m *Manager) Verify(ctx context.Context, manifest Manifest) error {
	f, err := os.CreateTemp("", "kv-backup-verify-*.snap")
	if err != nil {
		return fmt.Errorf("creating a temporary file: %w", err)
	}

	defer func() {
		_ = f.Close()
		_ = os.Remove(f.Name())
	}()

	if err := m.Fetch(ctx, manifest, f); err != nil {
		return err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewinding the snapshot: %w", err)
	}

	return verifyRestore(f, manifest)
}

// verifyBump is the revision bump of a verification restore: small and
// known, so the restored revision can be checked.
const verifyBump = 1

// verifyRestore restores the snapshot in f into memory and checks it.
func verifyRestore(f io.ReadSeeker, manifest Manifest) error {
	state, err := snapshot.Restore(f, node.RestoreConfig{
		ID:           1,
		DataDir:      "/verify",
		FS:           vfs.NewMem(),
		Members:      []*kvpb.Member{{Id: 1, PeerUrl: "http://verify.invalid:2380"}},
		RevisionBump: verifyBump,
	})
	if err != nil {
		return fmt.Errorf("restoring %s: %w", manifest.Snapshot, err)
	}

	sameIndex := state.GetIndex() == manifest.Index && state.GetTerm() == manifest.Term
	if !sameIndex || state.GetRevision() != manifest.Revision+verifyBump {
		return fmt.Errorf("%s restored at index %d, term %d, revision %d; the manifest says %d, %d, %d",
			manifest.Snapshot, state.GetIndex(), state.GetTerm(), state.GetRevision()-verifyBump,
			manifest.Index, manifest.Term, manifest.Revision)
	}

	return nil
}
