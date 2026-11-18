package node

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/statemachine"
)

// requestIDBytes is the length of request IDs the node generates for writes
// that come without one.
const requestIDBytes = 16

// Propose replicates a write and returns its result once this node has
// applied it. Any node accepts writes; raft forwards them to the leader.
//
// A request ID makes the write safe to retry: if the outcome is unknown,
// proposing it again with the same ID applies it at most once. Without one,
// the node generates an ID, which only serves to match the result.
func (n *Node) Propose(ctx context.Context, p *kvpb.Proposal) (*kvpb.Result, error) {
	if err := n.sm.Limits().ValidateProposal(p); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	if len(p.GetRequestId()) == 0 {
		p = proto.CloneOf(p)
		p.RequestId = newRequestID()
	}

	data, err := proto.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encoding proposal: %w", err)
	}

	if len(data) > n.cfg.MaxRequestBytes {
		return nil, fmt.Errorf("%w: request of %d bytes exceeds the limit of %d",
			ErrInvalidRequest, len(data), n.cfg.MaxRequestBytes)
	}

	ctx, span := n.cfg.Tracer.Start(ctx, "kv.propose", trace.WithAttributes(attribute.Int("kv.bytes", len(data))))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()

	start := time.Now()
	key := string(p.GetRequestId())
	ch := n.proposals.register(key)
	defer n.proposals.cancel(key, ch)

	if err := n.raft.Propose(ctx, data); err != nil {
		err = proposeError(err)
		n.metrics.proposal(proposalOutcome(err), start)

		return nil, err
	}

	select {
	case res := <-ch:
		if res.GetError() != "" {
			n.metrics.proposal(proposalInvalid, start)

			return nil, fmt.Errorf("%w: %s", ErrInvalidRequest, res.GetError())
		}

		n.metrics.proposal(proposalApplied, start)

		return res, nil
	case <-ctx.Done():
		n.metrics.proposal(proposalTimedOut, start)

		return nil, fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
	case <-n.stopc:
		n.metrics.proposal(proposalTimedOut, start)

		return nil, fmt.Errorf("%w: node stopped", ErrOutcomeUnknown)
	}
}

// proposalOutcome names the result of a proposal that raft did not accept.
func proposalOutcome(err error) string {
	if errors.Is(err, ErrUnavailable) {
		return proposalDropped
	}

	return proposalTimedOut
}

// proposeError classifies a failed raft.Propose. Only a dropped proposal
// certainly never entered the log; a cancelled or stopped one may have.
func proposeError(err error) error {
	switch {
	case errors.Is(err, raft.ErrProposalDropped):
		return fmt.Errorf("%w: proposal dropped, probably no leader: %w", ErrUnavailable, err)
	case errors.Is(err, raft.ErrStopped):
		return fmt.Errorf("%w: node stopped: %w", ErrOutcomeUnknown, err)
	default:
		return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
	}
}

// Read runs fn on a view of the state. Unless serializable is set, the view
// is linearizable: it reflects every write that completed anywhere in the
// cluster before Read was called. A serializable view is local and may be
// stale.
func (n *Node) Read(ctx context.Context, serializable bool, fn func(*statemachine.View) error) error {
	if !serializable {
		if err := n.waitReadIndex(ctx); err != nil {
			return err
		}
	}

	select {
	case <-n.stopc:
		return fmt.Errorf("%w: node stopped", ErrUnavailable)
	default:
	}

	v := n.sm.NewView()
	defer v.Close()

	return fn(v)
}

// waitReadIndex confirms with a quorum that this node's leader is still the
// leader, gets the commit index, and waits until this node has applied it.
func (n *Node) waitReadIndex(ctx context.Context) error {
	ctx, span := n.cfg.Tracer.Start(ctx, "kv.read_index")
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()

	start := time.Now()
	rctx := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, n.cfg.ID), n.readSeq.Add(1))
	key := string(rctx)

	ch := n.reads.register(key)
	defer n.reads.cancel(key, ch)

	if err := n.raft.ReadIndex(ctx, rctx); err != nil {
		return fmt.Errorf("%w: requesting read index: %w", ErrUnavailable, err)
	}

	var index uint64

	select {
	case index = <-ch:
	case <-ctx.Done():
		return fmt.Errorf("%w: read index not confirmed, probably no leader: %w", ErrUnavailable, ctx.Err())
	case <-n.stopc:
		return fmt.Errorf("%w: node stopped", ErrUnavailable)
	}

	if err := n.applied.wait(ctx, n.stopc, index); err != nil {
		return fmt.Errorf("%w: waiting to apply index %d: %w", ErrUnavailable, index, err)
	}

	n.metrics.observe(func(m *Metrics) prometheus.Histogram { return m.readIndexDuration }, start)

	return nil
}

// Step passes a raft message received from another node to raft.
func (n *Node) Step(ctx context.Context, msg *raftpb.Message) error {
	if err := n.raft.Step(ctx, msg); err != nil {
		return fmt.Errorf("stepping raft message: %w", err)
	}

	return nil
}

// ReportUnreachable tells raft that a message to a peer was not delivered.
func (n *Node) ReportUnreachable(id uint64) {
	n.raft.ReportUnreachable(id)
}

// ReportSnapshot tells raft whether a snapshot reached a peer.
func (n *Node) ReportSnapshot(id uint64, ok bool) {
	status := raft.SnapshotFinish
	if !ok {
		status = raft.SnapshotFailure
	}

	n.raft.ReportSnapshot(id, status)
}

// SnapshotMetadata implements [raftlog.SnapshotSource]: the snapshot of the
// state machine as it is now.
func (n *Node) SnapshotMetadata() (*raftpb.SnapshotMetadata, error) {
	return snapshotMetadata(n.sm.Applied())
}

func snapshotMetadata(state *kvpb.AppliedState) (*raftpb.SnapshotMetadata, error) {
	cs := &raftpb.ConfState{}
	if err := proto.Unmarshal(state.GetConfState(), cs); err != nil {
		return nil, fmt.Errorf("decoding conf state: %w", err)
	}

	return &raftpb.SnapshotMetadata{
		Index:     new(state.GetIndex()),
		Term:      new(state.GetTerm()),
		ConfState: cs,
	}, nil
}

func newRequestID() []byte {
	id := make([]byte, requestIDBytes)
	// crypto/rand.Read never fails; it crashes the program if the system
	// random source does.
	_, _ = rand.Read(id)

	return id
}
