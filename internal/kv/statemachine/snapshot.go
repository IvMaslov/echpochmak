package statemachine

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// maxSnapshotRecordBytes bounds a single key or value in a snapshot stream,
// so a corrupt length cannot make the reader allocate without limit.
const maxSnapshotRecordBytes = 64 << 20

// Export is a consistent view of the whole replicated state, to be streamed
// as a snapshot. Its data and applied state always match.
type Export struct {
	snap  *pebble.Snapshot
	state *kvpb.AppliedState
}

// NewExport opens a snapshot of the current state. Close it when done.
func (sm *StateMachine) NewExport() (*Export, error) {
	snap := sm.db.NewSnapshot()

	state, err := readAppliedState(snap)
	if err != nil {
		_ = snap.Close()

		return nil, err
	}

	return &Export{snap: snap, state: state}, nil
}

// State returns the applied state the export corresponds to.
func (e *Export) State() *kvpb.AppliedState {
	return proto.CloneOf(e.state)
}

// Close releases the export.
func (e *Export) Close() {
	_ = e.snap.Close()
}

// WriteTo writes the state to w. The format is a sequence of records, each a
// key and a value, both prefixed with their length as a uvarint.
func (e *Export) WriteTo(w io.Writer) (int64, error) {
	iter, err := e.snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixState),
		UpperBound: prefixEnd([]byte(prefixState)),
	})
	if err != nil {
		return 0, fmt.Errorf("opening snapshot iterator: %w", err)
	}
	defer iter.Close()

	cw := &countingWriter{w: w}
	bw := bufio.NewWriter(cw)

	for valid := iter.First(); valid; valid = iter.Next() {
		if err := writeRecord(bw, iter.Key(), iter.Value()); err != nil {
			return cw.n, err
		}
	}

	if err := iter.Error(); err != nil {
		return cw.n, fmt.Errorf("iterating snapshot: %w", err)
	}

	if err := bw.Flush(); err != nil {
		return cw.n, fmt.Errorf("writing snapshot: %w", err)
	}

	return cw.n, nil
}

// ExportSnapshot writes the whole replicated state to w and returns the
// applied state it corresponds to.
func (sm *StateMachine) ExportSnapshot(w io.Writer) (*kvpb.AppliedState, error) {
	e, err := sm.NewExport()
	if err != nil {
		return nil, err
	}
	defer e.Close()

	if _, err := e.WriteTo(w); err != nil {
		return nil, err
	}

	return e.State(), nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)

	return n, err //nolint:wrapcheck // a pass-through writer.
}

// ImportSnapshot replaces the replicated state with the snapshot read from r,
// writing into b, and returns the applied state of the snapshot. The caller
// commits b and then calls [StateMachine.SetApplied].
func (sm *StateMachine) ImportSnapshot(b *pebble.Batch, r io.Reader) (*kvpb.AppliedState, error) {
	if err := b.DeleteRange([]byte(prefixState), prefixEnd([]byte(prefixState)), nil); err != nil {
		return nil, fmt.Errorf("clearing state: %w", err)
	}

	br := bufio.NewReader(r)

	var state *kvpb.AppliedState

	for {
		key, value, err := readRecord(br)
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		if !bytes.HasPrefix(key, []byte(prefixState)) {
			return nil, fmt.Errorf("snapshot contains a key outside the state: %q", key)
		}

		if string(key) == keyApplied {
			state = &kvpb.AppliedState{}
			if err := proto.Unmarshal(value, state); err != nil {
				return nil, fmt.Errorf("decoding snapshot applied state: %w", err)
			}
		}

		if err := b.Set(key, value, nil); err != nil {
			return nil, fmt.Errorf("writing snapshot record: %w", err)
		}
	}

	if state == nil {
		return nil, errors.New("snapshot has no applied state")
	}

	return state, nil
}

func writeRecord(w *bufio.Writer, key, value []byte) error {
	for _, part := range [][]byte{key, value} {
		var lenBuf [binary.MaxVarintLen64]byte

		n := binary.PutUvarint(lenBuf[:], uint64(len(part)))
		if _, err := w.Write(lenBuf[:n]); err != nil {
			return fmt.Errorf("writing snapshot: %w", err)
		}

		if _, err := w.Write(part); err != nil {
			return fmt.Errorf("writing snapshot: %w", err)
		}
	}

	return nil
}

// readRecord returns io.EOF only at a clean record boundary.
func readRecord(r *bufio.Reader) (key, value []byte, err error) {
	key, err = readPart(r)
	if err != nil {
		return nil, nil, err
	}

	value, err = readPart(r)
	if errors.Is(err, io.EOF) {
		return nil, nil, fmt.Errorf("truncated snapshot record: %w", io.ErrUnexpectedEOF)
	}

	return key, value, err
}

func readPart(r *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if errors.Is(err, io.EOF) {
		return nil, io.EOF
	}

	if err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}

	if n > maxSnapshotRecordBytes {
		return nil, fmt.Errorf("snapshot record of %d bytes exceeds the limit", n)
	}

	part := make([]byte, n)
	if _, err := io.ReadFull(r, part); err != nil {
		return nil, fmt.Errorf("reading snapshot: %w", err)
	}

	return part, nil
}
