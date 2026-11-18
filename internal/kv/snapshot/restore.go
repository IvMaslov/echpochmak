package snapshot

import (
	"fmt"
	"io"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
)

// Restore verifies the whole snapshot file f, then restores it into a new
// node data directory as cfg describes; see [node.Restore]. The new cluster
// ID is salted with the file's checksum, so every node restored from the
// same file joins the same new cluster. Nothing is written if f is invalid.
func Restore(f io.ReadSeeker, cfg node.RestoreConfig) (*kvpb.AppliedState, error) {
	info, err := Verify(f)
	if err != nil {
		return nil, err
	}

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewinding the snapshot: %w", err)
	}

	r, err := NewReader(f)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	cfg.Salt = info.Checksum

	state, err := node.Restore(cfg, r)
	if err != nil {
		return nil, err
	}

	if state.GetIndex() != info.Header.GetIndex() || state.GetTerm() != info.Header.GetTerm() {
		return nil, fmt.Errorf("%w: the data is at %d/%d but the header says %d/%d", ErrCorrupt,
			state.GetIndex(), state.GetTerm(), info.Header.GetIndex(), info.Header.GetTerm())
	}

	return state, nil
}
