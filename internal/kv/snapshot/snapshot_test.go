package snapshot_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/snapshot"
)

func header() *kvpb.SnapshotHeader {
	return &kvpb.SnapshotHeader{
		ClusterId: "c1",
		NodeId:    2,
		Index:     41,
		Term:      3,
		Revision:  17,
		Members:   []*kvpb.Member{{Id: 1, PeerUrl: "http://kv1:2380"}},
	}
}

func write(t *testing.T, body []byte) []byte {
	t.Helper()

	var buf bytes.Buffer

	w, err := snapshot.NewWriter(&buf, header())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	if _, err := w.Write(body); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	return buf.Bytes()
}

func read(file []byte) (*kvpb.SnapshotHeader, []byte, error) {
	r, err := snapshot.NewReader(bytes.NewReader(file))
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	body, err := io.ReadAll(r)

	return r.Header(), body, err
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, 1, 100, 1 << 20} {
		body := bytes.Repeat([]byte("record "), size/7+1)[:size]
		file := write(t, body)

		h, got, err := read(file)
		if err != nil {
			t.Fatalf("size %d: read: %v", size, err)
		}

		if !bytes.Equal(got, body) {
			t.Fatalf("size %d: body differs", size)
		}

		if h.GetIndex() != 41 || h.GetRevision() != 17 || h.GetClusterId() != "c1" {
			t.Fatalf("header = %v", h)
		}

		info, err := snapshot.Verify(bytes.NewReader(file))
		if err != nil || len(info.Checksum) != 64 || info.BodySize != uint64(size) {
			t.Fatalf("size %d: Verify = %+v, %v", size, info, err)
		}
	}
}

func TestTruncationIsDetected(t *testing.T) {
	t.Parallel()

	file := write(t, bytes.Repeat([]byte("abc"), 10_000))

	for _, n := range []int{0, 4, 12, 20, len(file) / 2, len(file) - 40, len(file) - 1} {
		_, err := snapshot.Verify(bytes.NewReader(file[:n]))
		if !errors.Is(err, snapshot.ErrCorrupt) {
			t.Fatalf("file cut at %d of %d: error %v, want ErrCorrupt", n, len(file), err)
		}
	}
}

func TestFlippedByteIsDetected(t *testing.T) {
	t.Parallel()

	file := write(t, bytes.Repeat([]byte("abc"), 1_000))

	for i := range file {
		corrupt := bytes.Clone(file)
		corrupt[i] ^= 0x01

		if _, err := snapshot.Verify(bytes.NewReader(corrupt)); err == nil {
			t.Fatalf("a flipped bit at byte %d of %d went unnoticed", i, len(file))
		}
	}
}

func TestUnknownVersionIsRejected(t *testing.T) {
	t.Parallel()

	file := write(t, []byte("x"))
	binary.BigEndian.PutUint32(file[len(snapshot.Magic):], snapshot.FormatVersion+1)

	if _, err := snapshot.Verify(bytes.NewReader(file)); !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf("error %v, want ErrCorrupt", err)
	}
}

func TestNotASnapshot(t *testing.T) {
	t.Parallel()

	_, err := snapshot.Verify(bytes.NewReader([]byte("PK\x03\x04 this is a zip file")))
	if !errors.Is(err, snapshot.ErrCorrupt) {
		t.Fatalf("error %v, want ErrCorrupt", err)
	}
}
