// Package transport carries raft traffic between nodes over gRPC.
//
// Each peer gets one long-lived client stream for raft messages, fed by a
// bounded queue so the node's main loop never blocks: when the queue is full
// or the stream breaks, messages are dropped and the peer is reported
// unreachable, and raft retransmits. Snapshots go over a separate stream, in
// chunks, so their size is not limited by the gRPC message size.
package transport

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"

	"echpochmak/internal/kv/node"
)

// Metadata key that carries the cluster ID on every peer call.
const clusterIDHeader = "x-kv-cluster-id"

// Defaults for [Config].
const (
	DefaultQueueSize           = 4096
	DefaultMaxConcurrentSnaps  = 2
	DefaultSnapshotChunkBytes  = 1 << 20
	DefaultSnapshotTimeout     = 10 * time.Minute
	defaultReconnectBackoff    = 100 * time.Millisecond
	defaultMaxReconnectBackoff = 2 * time.Second
)

// Handler is the node side of the transport: where received messages go,
// and where outgoing snapshots come from. *node.Node implements it.
type Handler interface {
	ClusterID() string
	Step(ctx context.Context, msg *raftpb.Message) error
	ReportUnreachable(id uint64)
	ReportSnapshot(id uint64, ok bool)
	OpenSnapshot() (*node.OutgoingSnapshot, error)
	ReceiveSnapshot(ctx context.Context, msg *raftpb.Message, data io.Reader) error
}

// Config configures a Transport.
type Config struct {
	// QueueSize is the number of messages buffered per peer.
	QueueSize int
	// MaxConcurrentSnapshots limits outgoing snapshots of this node.
	MaxConcurrentSnapshots int
	SnapshotChunkBytes     int
	SnapshotTimeout        time.Duration
	Logger                 *slog.Logger
}

func (c *Config) setDefaults() {
	if c.QueueSize <= 0 {
		c.QueueSize = DefaultQueueSize
	}

	if c.MaxConcurrentSnapshots <= 0 {
		c.MaxConcurrentSnapshots = DefaultMaxConcurrentSnaps
	}

	if c.SnapshotChunkBytes <= 0 {
		c.SnapshotChunkBytes = DefaultSnapshotChunkBytes
	}

	if c.SnapshotTimeout <= 0 {
		c.SnapshotTimeout = DefaultSnapshotTimeout
	}

	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Transport implements [node.Transport] over gRPC, and serves the peer
// service that receives from other nodes.
type Transport struct {
	cfg   Config
	log   *slog.Logger
	snaps chan struct{} // semaphore for outgoing snapshots

	mu      sync.RWMutex
	handler Handler
	peers   map[uint64]*peer
	closed  bool
}

var _ node.Transport = (*Transport)(nil)

// New returns a transport. Call [Transport.SetHandler] once the node exists;
// until then nothing is sent or received.
func New(cfg Config) *Transport {
	cfg.setDefaults()

	return &Transport{
		cfg:   cfg,
		log:   cfg.Logger,
		snaps: make(chan struct{}, cfg.MaxConcurrentSnapshots),
		peers: map[uint64]*peer{},
	}
}

// SetHandler connects the transport to its node.
func (t *Transport) SetHandler(h Handler) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.handler = h
}

func (t *Transport) getHandler() Handler {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.handler
}

// Send implements [node.Transport]. It never blocks.
func (t *Transport) Send(msgs []*raftpb.Message) {
	h := t.getHandler()
	if h == nil {
		return
	}

	for _, msg := range msgs {
		t.mu.RLock()
		p := t.peers[msg.GetTo()]
		t.mu.RUnlock()

		switch {
		case p == nil:
			h.ReportUnreachable(msg.GetTo())
		case msg.GetType() == raftpb.MessageType_MsgSnap:
			go t.sendSnapshot(h, p, msg)
		default:
			p.enqueue(h, msg)
		}
	}
}

// AddPeer implements [node.Transport]. Adding a known peer with a new URL
// reconnects to it.
func (t *Transport) AddPeer(id uint64, url string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return
	}

	target := dialTarget(url)

	if p, ok := t.peers[id]; ok {
		if p.target == target {
			return
		}

		p.stop()
	}

	p, err := newPeer(t, id, target)
	if err != nil {
		t.log.Error("connecting to peer failed",
			slog.Uint64("peer", id),
			slog.String("url", url),
			slog.Any("error", err),
		)

		return
	}

	t.peers[id] = p
}

// RemovePeer implements [node.Transport].
func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if p, ok := t.peers[id]; ok {
		p.stop()
		delete(t.peers, id)
	}
}

// Close disconnects from every peer.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true

	for id, p := range t.peers {
		p.stop()
		delete(t.peers, id)
	}
}

// dialTarget turns a peer URL such as http://host:2380 into a gRPC target.
func dialTarget(url string) string {
	for _, scheme := range []string{"http://", "https://"} {
		url = strings.TrimPrefix(url, scheme)
	}

	return strings.TrimSuffix(url, "/")
}
