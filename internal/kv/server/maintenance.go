package server

import (
	"time"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/snapshot"
	"echpochmak/internal/version"
)

// snapshotChunkBytes is the size of one streamed piece of a snapshot file.
const snapshotChunkBytes = 1 << 20

type maintenanceService struct {
	kvpb.UnimplementedMaintenanceServiceServer

	n *node.Node
}

// ExportSnapshot implements [kvpb.MaintenanceServiceServer]: a linearizable
// snapshot, streamed in the snapshot file format.
func (s *maintenanceService) ExportSnapshot(
	_ *kvpb.ExportSnapshotRequest,
	stream kvpb.MaintenanceService_ExportSnapshotServer,
) error {
	snap, err := s.n.OpenLinearizableSnapshot(stream.Context())
	if err != nil {
		return toStatus(err)
	}
	defer snap.Close()

	state := snap.State()
	header := &kvpb.SnapshotHeader{
		ClusterId:       s.n.ClusterID(),
		NodeId:          s.n.ID(),
		Index:           state.GetIndex(),
		Term:            state.GetTerm(),
		Revision:        state.GetRevision(),
		Members:         state.GetMembers(),
		CreatedUnixNano: time.Now().UnixNano(),
		Version:         version.Get().Version,
	}

	if err := stream.Send(&kvpb.ExportSnapshotResponse{Header: header}); err != nil {
		return err
	}

	out := &chunkSender{send: func(p []byte) error {
		return stream.Send(&kvpb.ExportSnapshotResponse{Chunk: p})
	}}

	w, err := snapshot.NewWriter(out, header)
	if err != nil {
		return toStatus(err)
	}

	if _, err := snap.WriteTo(w); err != nil {
		return toStatus(err)
	}

	if err := w.Close(); err != nil {
		return toStatus(err)
	}

	return out.flush()
}

// chunkSender collects writes into chunks of up to snapshotChunkBytes and
// sends each full chunk.
type chunkSender struct {
	send func([]byte) error
	buf  []byte
}

func (c *chunkSender) Write(p []byte) (int, error) {
	n := len(p)

	for len(p) > 0 {
		take := min(len(p), snapshotChunkBytes-len(c.buf))
		c.buf = append(c.buf, p[:take]...)
		p = p[take:]

		if len(c.buf) == snapshotChunkBytes {
			if err := c.flush(); err != nil {
				return n - len(p), err
			}
		}
	}

	return n, nil
}

func (c *chunkSender) flush() error {
	if len(c.buf) == 0 {
		return nil
	}

	chunk := c.buf
	c.buf = make([]byte, 0, snapshotChunkBytes)

	return c.send(chunk)
}
