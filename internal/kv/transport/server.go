package transport

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// Register adds the peer service to a gRPC server, so other nodes can reach
// this one.
func (t *Transport) Register(s *grpc.Server) {
	kvpb.RegisterPeerServiceServer(s, &peerService{t: t})
}

type peerService struct {
	kvpb.UnimplementedPeerServiceServer

	t *Transport
}

// handler returns the node, after checking that the caller belongs to its
// cluster.
func (s *peerService) handler(ctx context.Context) (Handler, error) {
	h := s.t.getHandler()
	if h == nil {
		return nil, status.Error(codes.Unavailable, "node is starting")
	}

	md, _ := metadata.FromIncomingContext(ctx)
	if got := md.Get(clusterIDHeader); len(got) != 1 || got[0] != h.ClusterID() {
		return nil, status.Errorf(codes.FailedPrecondition, "caller is not in cluster %s", h.ClusterID())
	}

	return h, nil
}

// Raft receives a stream of raft messages from a peer.
func (s *peerService) Raft(stream grpc.ClientStreamingServer[kvpb.RaftRequest, kvpb.RaftResponse]) error {
	h, err := s.handler(stream.Context())
	if err != nil {
		return err
	}

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return stream.SendAndClose(&kvpb.RaftResponse{})
		}

		if err != nil {
			return err //nolint:wrapcheck // gRPC status errors pass through.
		}

		msg := &raftpb.Message{}
		if err := proto.Unmarshal(req.GetMessage(), msg); err != nil {
			return status.Errorf(codes.InvalidArgument, "decoding raft message: %v", err)
		}

		if err := h.Step(stream.Context(), msg); err != nil {
			return status.Errorf(codes.Unavailable, "stepping raft message: %v", err)
		}
	}
}

// Snapshot receives a snapshot from the leader: header, data, checksum.
func (s *peerService) Snapshot(stream grpc.ClientStreamingServer[kvpb.SnapshotRequest, kvpb.SnapshotResponse]) error {
	h, err := s.handler(stream.Context())
	if err != nil {
		return err
	}

	first, err := stream.Recv()
	if err != nil {
		return err //nolint:wrapcheck // gRPC status errors pass through.
	}

	if first.GetHeader() == nil {
		return status.Error(codes.InvalidArgument, errNoSnapshotHead.Error())
	}

	msg := &raftpb.Message{}
	if err := proto.Unmarshal(first.GetHeader(), msg); err != nil {
		return status.Errorf(codes.InvalidArgument, "decoding snapshot header: %v", err)
	}

	// The data flows through a pipe into the node, which stages it on disk.
	// A bad checksum closes the pipe with an error, so the node discards the
	// staged file and never passes the snapshot to raft.
	pr, pw := io.Pipe()

	done := make(chan error, 1)

	go func() { done <- h.ReceiveSnapshot(stream.Context(), msg, pr) }()

	if err := pumpSnapshot(stream, pw); err != nil {
		_ = pw.CloseWithError(err) // CloseWithError always returns nil.

		// The node fails because the pipe failed; err already says why.
		if nodeErr := <-done; nodeErr != nil {
			s.t.log.Debug("snapshot discarded", slog.Any("error", nodeErr))
		}

		return status.Errorf(codes.DataLoss, "receiving snapshot: %v", err)
	}

	_ = pw.Close() // Close always returns nil.

	if err := <-done; err != nil {
		return status.Errorf(codes.Internal, "applying snapshot: %v", err)
	}

	return stream.SendAndClose(&kvpb.SnapshotResponse{})
}

// pumpSnapshot copies data chunks into w and verifies the trailing checksum.
func pumpSnapshot(
	stream grpc.ClientStreamingServer[kvpb.SnapshotRequest, kvpb.SnapshotResponse],
	w io.Writer,
) error {
	crc := crc32.New(castagnoli)

	for {
		req, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("snapshot stream ended before its checksum: %w", err)
		}

		switch chunk := req.GetChunk().(type) {
		case *kvpb.SnapshotRequest_Data:
			_, _ = crc.Write(chunk.Data) // hash writes never fail.

			if _, err := w.Write(chunk.Data); err != nil {
				return fmt.Errorf("staging snapshot data: %w", err)
			}
		case *kvpb.SnapshotRequest_Checksum:
			if chunk.Checksum != crc.Sum32() {
				return errChecksum
			}

			return nil
		default:
			return errNoSnapshotHead
		}
	}
}
