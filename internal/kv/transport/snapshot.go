package transport

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

var (
	errNoHandler      = errors.New("transport has no node yet")
	errSnapshotBusy   = errors.New("a snapshot to this peer is already in flight")
	errChecksum       = errors.New("snapshot checksum mismatch")
	errNoSnapshotHead = errors.New("snapshot stream does not start with a header")
)

// castagnoli is the CRC-32C table used for snapshot checksums.
var castagnoli = crc32.MakeTable(crc32.Castagnoli) //nolint:gochecknoglobals // read-only table.

// sendSnapshot streams a snapshot of this node's current state to a peer and
// reports the outcome to raft. The state may be newer than the snapshot raft
// asked for; the header carries the metadata of what is actually sent.
func (t *Transport) sendSnapshot(h Handler, p *peer, msg *raftpb.Message) {
	start := time.Now()
	err := t.streamSnapshot(h, p, msg)

	h.ReportSnapshot(p.id, err == nil)
	t.cfg.Metrics.snapshotSent(err)

	if err != nil {
		t.log.Warn("sending snapshot failed", slog.Uint64("peer", p.id), slog.Any("error", err))

		return
	}

	t.log.Info("snapshot sent", slog.Uint64("peer", p.id), slog.Duration("duration", time.Since(start)))
}

func (t *Transport) streamSnapshot(h Handler, p *peer, msg *raftpb.Message) error {
	select {
	case p.busy <- struct{}{}:
		defer func() { <-p.busy }()
	default:
		return errSnapshotBusy
	}

	select {
	case t.snaps <- struct{}{}:
		defer func() { <-t.snaps }()
	case <-p.ctx.Done():
		return fmt.Errorf("peer stopped: %w", p.ctx.Err())
	}

	snap, err := h.OpenSnapshot()
	if err != nil {
		return fmt.Errorf("opening snapshot: %w", err)
	}
	defer snap.Close()

	header := proto.CloneOf(msg)
	header.Snapshot.Metadata = snap.Metadata()

	raw, err := proto.Marshal(header)
	if err != nil {
		return fmt.Errorf("encoding snapshot header: %w", err)
	}

	ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(p.ctx, clusterIDHeader, h.ClusterID()),
		t.cfg.SnapshotTimeout)
	defer cancel()

	stream, err := p.client.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("opening snapshot stream: %w", err)
	}

	if err := stream.Send(&kvpb.SnapshotRequest{Chunk: &kvpb.SnapshotRequest_Header{Header: raw}}); err != nil {
		return fmt.Errorf("sending snapshot header: %w", err)
	}

	w := &chunkWriter{stream: stream, size: t.cfg.SnapshotChunkBytes, crc: crc32.New(castagnoli)}
	if _, err := snap.WriteTo(w); err != nil {
		return err
	}

	if err := w.flush(); err != nil {
		return err
	}

	trailer := &kvpb.SnapshotRequest{Chunk: &kvpb.SnapshotRequest_Checksum{Checksum: w.crc.Sum32()}}
	if err := stream.Send(trailer); err != nil {
		return fmt.Errorf("sending snapshot checksum: %w", err)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("peer rejected snapshot: %w", err)
	}

	return nil
}

// chunkWriter sends written bytes as snapshot data chunks of a fixed size.
type chunkWriter struct {
	stream interface {
		Send(*kvpb.SnapshotRequest) error
	}
	size int
	buf  []byte
	crc  interface {
		io.Writer
		Sum32() uint32
	}
}

func (w *chunkWriter) Write(p []byte) (int, error) {
	n := len(p)

	for len(p) > 0 {
		take := min(w.size-len(w.buf), len(p))
		w.buf = append(w.buf, p[:take]...)
		p = p[take:]

		if len(w.buf) == w.size {
			if err := w.flush(); err != nil {
				return 0, err
			}
		}
	}

	return n, nil
}

func (w *chunkWriter) flush() error {
	if len(w.buf) == 0 {
		return nil
	}

	_, _ = w.crc.Write(w.buf) // hash writes never fail.

	if err := w.stream.Send(&kvpb.SnapshotRequest{Chunk: &kvpb.SnapshotRequest_Data{Data: w.buf}}); err != nil {
		return fmt.Errorf("sending snapshot data: %w", err)
	}

	w.buf = make([]byte, 0, w.size)

	return nil
}
