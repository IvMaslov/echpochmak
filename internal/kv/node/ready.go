package node

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/statemachine"
)

// run is the node's main loop. It handles each raft Ready in the order the
// raft library requires: persist, send, apply, then Advance.
func (n *Node) run() {
	defer close(n.done)

	for {
		select {
		case <-n.stopc:
			return
		case rd := <-n.raft.Ready():
			if err := n.handleReady(rd); err != nil {
				// The node cannot continue without breaking raft's
				// guarantees; stop it rather than serve wrong data.
				n.log.Error("node failed, stopping", slog.Any("error", err))
				n.failStop()

				return
			}
		}
	}
}

// failStop stops the node from inside the main loop.
func (n *Node) failStop() {
	go func() { _ = n.Stop() }()
}

func (n *Node) handleReady(rd raft.Ready) error {
	defer n.metrics.observe(func(m *Metrics) prometheus.Histogram { return m.readyLoop }, time.Now())

	n.updateSoftState(rd.SoftState)

	if rd.HardState != nil {
		n.term.Store(rd.GetTerm())
	}

	// 1. Persist. An incoming snapshot replaces the log, so it goes first;
	// entries in the same Ready follow it.
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := n.applySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("applying snapshot: %w", err)
		}
	}

	saveStart := time.Now()

	if err := n.storage.Save(rd.HardState, rd.Entries, rd.MustSync); err != nil {
		return fmt.Errorf("saving raft state: %w", err)
	}

	if rd.MustSync {
		n.metrics.observe(func(m *Metrics) prometheus.Histogram { return m.walFsync }, saveStart)
	}

	// 2. Send, only after the state the messages depend on is durable.
	n.transport.Send(rd.Messages)

	// 3. Apply.
	applyStart := time.Now()

	removed, err := n.applyCommitted(rd.CommittedEntries)
	if err != nil {
		return fmt.Errorf("applying entries: %w", err)
	}

	if len(rd.CommittedEntries) > 0 {
		n.metrics.observe(func(m *Metrics) prometheus.Histogram { return m.applyBatch }, applyStart)
	}

	// 4. Confirm linearizable reads.
	for _, rs := range rd.ReadStates {
		n.reads.trigger(string(rs.RequestCtx), rs.Index)
	}

	n.raft.Advance()

	if err := n.maybeCompact(); err != nil {
		n.log.Warn("log compaction failed", slog.Any("error", err))
	}

	if removed {
		n.log.Warn("this node was removed from the cluster, stopping")
		n.failStop()
	}

	return nil
}

func (n *Node) updateSoftState(ss *raft.SoftState) {
	if ss == nil {
		return
	}

	n.state.Store(ss.RaftState)

	if prev := n.leader.Swap(ss.Lead); prev != ss.Lead {
		if ss.Lead != 0 {
			n.metrics.leaderChanged()
		}

		n.log.Info("leader changed",
			slog.Uint64("leader", ss.Lead),
			slog.Uint64("previous", prev),
			slog.String("state", ss.RaftState.String()),
		)
	}
}

// applied is what applying one entry produced, to act on once the batch is
// committed.
type applied struct {
	// proposalID and result complete a waiting proposal.
	proposalID []byte
	result     *kvpb.Result
	// confID completes a waiting membership change.
	confID []byte
	// peerChange updates the transport for a membership change.
	peerChange func()
	// removed is set when this node was removed.
	removed bool
}

// applyCommitted applies entries in one batch and completes the requests
// waiting on them. It reports whether this node was removed.
func (n *Node) applyCommitted(entries []*raftpb.Entry) (bool, error) {
	if len(entries) == 0 {
		return false, nil
	}

	batch := n.sm.NewBatch()
	defer batch.Close()

	outcomes := make([]applied, 0, len(entries))

	for _, e := range entries {
		out, err := n.applyEntry(batch, e)
		if err != nil {
			return false, err
		}

		outcomes = append(outcomes, out)
	}

	if err := batch.Commit(false); err != nil {
		return false, err
	}

	n.applied.set(entries[len(entries)-1].GetIndex())

	removed := false

	for _, out := range outcomes {
		if out.peerChange != nil {
			out.peerChange()
		}

		if len(out.proposalID) > 0 {
			n.proposals.trigger(string(out.proposalID), out.result)
		}

		if len(out.confID) > 0 {
			n.confChanges.trigger(string(out.confID), struct{}{})
		}

		removed = removed || out.removed
	}

	return removed, nil
}

func (n *Node) applyEntry(batch *statemachine.Batch, e *raftpb.Entry) (applied, error) {
	switch e.GetType() {
	case raftpb.EntryType_EntryNormal:
		if len(e.GetData()) == 0 {
			batch.ApplyNoop(e.GetIndex(), e.GetTerm())

			return applied{}, nil
		}

		p := &kvpb.Proposal{}
		if err := proto.Unmarshal(e.GetData(), p); err != nil {
			return applied{}, fmt.Errorf("decoding entry %d: %w", e.GetIndex(), err)
		}

		res, err := batch.Apply(e.GetIndex(), e.GetTerm(), p)
		if err != nil {
			return applied{}, err
		}

		return applied{proposalID: p.GetRequestId(), result: res}, nil
	case raftpb.EntryType_EntryConfChange:
		return n.applyConfChange(batch, e)
	case raftpb.EntryType_EntryConfChangeV2:
		return applied{}, fmt.Errorf("entry %d: joint consensus conf changes are not supported", e.GetIndex())
	default:
		return applied{}, fmt.Errorf("entry %d has unknown type %s", e.GetIndex(), e.GetType())
	}
}

// applyConfChange applies a membership change to raft and the batch.
func (n *Node) applyConfChange(batch *statemachine.Batch, e *raftpb.Entry) (applied, error) {
	cc := &raftpb.ConfChange{}
	if err := proto.Unmarshal(e.GetData(), cc); err != nil {
		return applied{}, fmt.Errorf("decoding conf change %d: %w", e.GetIndex(), err)
	}

	cctx := &kvpb.ConfChangeContext{}
	if err := proto.Unmarshal(cc.GetContext(), cctx); err != nil {
		return applied{}, fmt.Errorf("decoding conf change context %d: %w", e.GetIndex(), err)
	}

	cs := n.raft.ApplyConfChange(cc)

	if err := batch.ApplyConfChange(e.GetIndex(), e.GetTerm(), cc, cs, cctx.GetMember()); err != nil {
		return applied{}, err
	}

	id := cc.GetNodeId()
	removal := cc.GetType() == raftpb.ConfChangeType_ConfChangeRemoveNode
	member := cctx.GetMember()

	peerChange := func() {
		switch {
		case removal && id != n.cfg.ID:
			n.transport.RemovePeer(id)
		case !removal && member.GetPeerUrl() != "":
			n.addPeer(member)
		}
	}

	n.log.Info("membership changed",
		slog.String("change", cc.GetType().String()),
		slog.Uint64("member", id),
		slog.Uint64("index", e.GetIndex()),
	)

	return applied{
		confID:     cctx.GetRequestId(),
		peerChange: peerChange,
		removed:    removal && id == n.cfg.ID,
	}, nil
}

// maybeCompact drops log entries that every follower within reach has, once
// enough entries were applied since the last check.
func (n *Node) maybeCompact() error {
	applied := n.sm.AppliedIndex()
	if applied < n.lastCompactCheck+n.cfg.SnapshotCount {
		return nil
	}

	n.lastCompactCheck = applied

	if applied <= n.cfg.CatchUpEntries {
		return nil
	}

	target := applied - n.cfg.CatchUpEntries
	if err := n.storage.Compact(target); err != nil {
		return fmt.Errorf("compacting log to %d: %w", target, err)
	}

	n.metrics.compacted()
	n.log.Debug("log compacted", slog.Uint64("up_to", target))

	return nil
}

// applySnapshot replaces the node's state with a snapshot received from the
// leader. The snapshot data was staged by [Node.ReceiveSnapshot]; its file
// name travels in the snapshot's Data.
func (n *Node) applySnapshot(snap *raftpb.Snapshot) error {
	meta := snap.GetMetadata()
	path := string(snap.GetData())

	f, err := n.cfg.FS.Open(path)
	if err != nil {
		return fmt.Errorf("opening staged snapshot: %w", err)
	}
	defer f.Close()

	b := n.db.NewBatch()
	defer b.Close()

	state, err := n.sm.ImportSnapshot(b, f)
	if err != nil {
		return err
	}

	if state.GetIndex() != meta.GetIndex() || state.GetTerm() != meta.GetTerm() {
		return fmt.Errorf("snapshot data is at %d/%d but its metadata says %d/%d",
			state.GetIndex(), state.GetTerm(), meta.GetIndex(), meta.GetTerm())
	}

	if err := n.storage.ResetToSnapshot(b, meta); err != nil {
		return err
	}

	if err := b.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("committing snapshot: %w", err)
	}

	n.sm.SetApplied(state)
	n.storage.ApplySnapshot(meta)
	n.metrics.snapshotApplied()
	n.applied.set(meta.GetIndex())
	n.lastCompactCheck = meta.GetIndex()

	for _, m := range state.GetMembers() {
		n.addPeer(m)
	}

	if err := n.cfg.FS.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		n.log.Warn("removing staged snapshot failed", slog.Any("error", err))
	}

	n.log.Info("snapshot applied",
		slog.Uint64("index", meta.GetIndex()),
		slog.Uint64("term", meta.GetTerm()),
	)

	return nil
}
