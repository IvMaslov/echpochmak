// Package kvtest runs KV clusters in one process for tests: nodes on
// in-memory filesystems that can crash, connected by an in-memory network
// that can drop messages and partition the cluster.
package kvtest

import (
	"bytes"
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/node"
)

// linkQueue is the number of messages a link buffers before dropping.
const linkQueue = 1024

// Network connects nodes in memory. Messages on one link are delivered in
// order, like a TCP stream; faults drop them.
//
// Each attached node instance has an epoch. Messages are dropped unless they
// come from the sender's current instance, so a crashed instance cannot
// reach the cluster, even with messages it queued before the crash.
type Network struct {
	mu       sync.Mutex
	nodes    map[uint64]*node.Node
	epochs   map[uint64]uint64
	links    map[[2]uint64]chan envelope
	cut      map[[2]uint64]bool
	dropRate float64
	rng      *rand.Rand
	stop     chan struct{}
	wg       sync.WaitGroup
}

// NewNetwork returns an empty network.
func NewNetwork(seed uint64) *Network {
	return &Network{
		nodes:  map[uint64]*node.Node{},
		epochs: map[uint64]uint64{},
		links:  map[[2]uint64]chan envelope{},
		cut:    map[[2]uint64]bool{},
		rng:    rand.New(rand.NewPCG(seed, seed)), //nolint:gosec // G404: test fault injection.
		stop:   make(chan struct{}),
	}
}

// Close stops delivering messages.
func (net *Network) Close() {
	close(net.stop)
	net.wg.Wait()
}

// envelope is a message with the epoch of the instance that sent it.
type envelope struct {
	msg   *raftpb.Message
	epoch uint64
}

// Transport returns the transport for a new instance of node id. The
// instance can send once it is attached with [Network.Attach].
func (net *Network) Transport(id uint64) node.Transport {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.epochs[id]++

	return &memTransport{net: net, from: id, epoch: net.epochs[id]}
}

// Attach makes n reachable as id.
func (net *Network) Attach(id uint64, n *node.Node) {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.nodes[id] = n
}

// Detach cuts node id off completely: nothing reaches it, and nothing it
// sent, or still sends, reaches anyone.
func (net *Network) Detach(id uint64) {
	net.mu.Lock()
	defer net.mu.Unlock()

	delete(net.nodes, id)
	net.epochs[id]++
}

// Partition cuts every link between nodes in different groups. Nodes not
// listed keep all their links.
func (net *Network) Partition(groups ...[]uint64) {
	net.mu.Lock()
	defer net.mu.Unlock()

	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}

			for _, from := range a {
				for _, to := range b {
					net.cut[[2]uint64{from, to}] = true
				}
			}
		}
	}
}

// Isolate cuts every link of id, in both directions.
func (net *Network) Isolate(id uint64, others []uint64) {
	net.Partition([]uint64{id}, others)
}

// Heal restores every link.
func (net *Network) Heal() {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.cut = map[[2]uint64]bool{}
}

// SetDropRate makes the network drop that fraction of messages at random.
func (net *Network) SetDropRate(rate float64) {
	net.mu.Lock()
	defer net.mu.Unlock()

	net.dropRate = rate
}

// deliverable reports whether a message on link, sent by the instance with
// epoch, should be delivered now.
func (net *Network) deliverable(link [2]uint64, epoch uint64) bool {
	net.mu.Lock()
	defer net.mu.Unlock()

	if net.cut[link] || net.epochs[link[0]] != epoch {
		return false
	}

	return net.dropRate == 0 || net.rng.Float64() >= net.dropRate
}

func (net *Network) target(id uint64) *node.Node {
	net.mu.Lock()
	defer net.mu.Unlock()

	return net.nodes[id]
}

// link returns the queue of a link, starting its delivery goroutine once.
func (net *Network) link(from, to uint64) chan envelope {
	net.mu.Lock()
	defer net.mu.Unlock()

	key := [2]uint64{from, to}
	if q, ok := net.links[key]; ok {
		return q
	}

	q := make(chan envelope, linkQueue)
	net.links[key] = q

	net.wg.Go(func() { net.deliver(key, q) })

	return q
}

func (net *Network) deliver(link [2]uint64, q chan envelope) {
	for {
		select {
		case <-net.stop:
			return
		case env := <-q:
			if !net.deliverable(link, env.epoch) {
				continue
			}

			if n := net.target(link[1]); n != nil {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_ = n.Step(ctx, env.msg)

				cancel()
			}
		}
	}
}

// sendSnapshot streams a snapshot of the sender's state to the target, as
// the real transport does: the data is taken now, and the message carries
// the metadata that matches it.
func (net *Network) sendSnapshot(from, epoch uint64, msg *raftpb.Message) {
	sender := net.target(from)
	ok := false

	defer func() {
		if sender != nil {
			sender.ReportSnapshot(msg.GetTo(), ok)
		}
	}()

	if sender == nil || !net.deliverable([2]uint64{from, msg.GetTo()}, epoch) {
		return
	}

	target := net.target(msg.GetTo())
	if target == nil {
		return
	}

	snap, err := sender.OpenSnapshot()
	if err != nil {
		return
	}
	defer snap.Close()

	var data bytes.Buffer
	if _, err := snap.WriteTo(&data); err != nil {
		return
	}

	msg = proto.CloneOf(msg)
	msg.Snapshot.Metadata = snap.Metadata()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ok = target.ReceiveSnapshot(ctx, msg, &data) == nil
}

type memTransport struct {
	net   *Network
	from  uint64
	epoch uint64
}

func (t *memTransport) Send(msgs []*raftpb.Message) {
	for _, msg := range msgs {
		if msg.GetType() == raftpb.MessageType_MsgSnap {
			t.net.wg.Go(func() { t.net.sendSnapshot(t.from, t.epoch, msg) })

			continue
		}

		select {
		case t.net.link(t.from, msg.GetTo()) <- envelope{msg: msg, epoch: t.epoch}:
		default:
			if n := t.net.target(t.from); n != nil {
				n.ReportUnreachable(msg.GetTo())
			}
		}
	}
}

func (*memTransport) AddPeer(uint64, string) {}

func (*memTransport) RemovePeer(uint64) {}
