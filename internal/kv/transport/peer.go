package transport

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// peer is the connection to one other node.
type peer struct {
	t      *Transport
	id     uint64
	target string
	conn   *grpc.ClientConn
	client kvpb.PeerServiceClient
	queue  chan *raftpb.Message
	busy   chan struct{}   // holds a token while a snapshot to this peer is sent
	ctx    context.Context //nolint:containedctx // the peer's lifetime.
	cancel context.CancelFunc
}

func newPeer(t *Transport, id uint64, target string) (*peer, error) {
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("dialing %s: %w", target, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	p := &peer{
		t:      t,
		id:     id,
		target: target,
		conn:   conn,
		client: kvpb.NewPeerServiceClient(conn),
		queue:  make(chan *raftpb.Message, t.cfg.QueueSize),
		busy:   make(chan struct{}, 1),
		ctx:    ctx,
		cancel: cancel,
	}

	go p.run()

	return p, nil
}

func (p *peer) stop() {
	p.cancel()
	_ = p.conn.Close()
}

// enqueue queues a message, or drops it when the queue is full.
func (p *peer) enqueue(h Handler, msg *raftpb.Message) {
	select {
	case p.queue <- msg:
	default:
		h.ReportUnreachable(p.id)
	}
}

// run keeps a stream open to the peer and sends queued messages on it.
func (p *peer) run() {
	backoff := defaultReconnectBackoff

	for p.ctx.Err() == nil {
		err := p.stream()
		if p.ctx.Err() != nil {
			return
		}

		if h := p.t.getHandler(); h != nil {
			h.ReportUnreachable(p.id)
		}

		p.t.log.Debug("peer stream broken", slog.Uint64("peer", p.id), slog.Any("error", err))

		select {
		case <-p.ctx.Done():
			return
		case <-time.After(backoff):
		}

		backoff = min(2*backoff, defaultMaxReconnectBackoff)
	}
}

// stream sends queued messages until the stream fails or the peer stops.
func (p *peer) stream() error {
	h := p.t.getHandler()
	if h == nil {
		return errNoHandler
	}

	ctx, cancel := context.WithCancel(metadata.AppendToOutgoingContext(p.ctx, clusterIDHeader, h.ClusterID()))
	defer cancel()

	stream, err := p.client.Raft(ctx)
	if err != nil {
		return fmt.Errorf("opening raft stream: %w", err)
	}

	for {
		select {
		case <-p.ctx.Done():
			_, _ = stream.CloseAndRecv()

			return nil
		case msg := <-p.queue:
			raw, err := proto.Marshal(msg)
			if err != nil {
				return fmt.Errorf("encoding raft message: %w", err)
			}

			if err := stream.Send(&kvpb.RaftRequest{Message: raw}); err != nil {
				return fmt.Errorf("sending raft message: %w", err)
			}
		}
	}
}
