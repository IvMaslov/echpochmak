package node

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// leaderPollInterval is how often a leadership transfer checks for the new
// leader.
const leaderPollInterval = 20 * time.Millisecond

// Status describes a node and, on the leader, its followers.
type Status struct {
	ID        uint64
	ClusterID string
	Leader    uint64
	Term      uint64
	RaftState string
	Commit    uint64
	Applied   uint64
	Members   []*kvpb.Member
	// Followers is the replication progress of each other member, known only
	// on the leader.
	Followers []FollowerProgress
}

// FollowerProgress is a follower's replication state, as seen by the leader.
type FollowerProgress struct {
	ID      uint64
	Match   uint64
	State   string
	Learner bool
}

// Status returns the node's current status.
func (n *Node) Status() Status {
	st := n.raft.Status()

	s := Status{
		ID:        n.cfg.ID,
		ClusterID: n.clusterID,
		Leader:    st.Lead,
		Term:      st.GetTerm(),
		RaftState: st.RaftState.String(),
		Commit:    st.GetCommit(),
		Applied:   n.sm.AppliedIndex(),
		Members:   n.Members(),
		Followers: []FollowerProgress{},
	}

	for id, pr := range st.Progress {
		if id == n.cfg.ID {
			continue
		}

		s.Followers = append(s.Followers, FollowerProgress{
			ID:      id,
			Match:   pr.Match,
			State:   pr.State.String(),
			Learner: pr.IsLearner,
		})
	}

	slices.SortFunc(s.Followers, func(a, b FollowerProgress) int { return cmp.Compare(a.ID, b.ID) })

	return s
}

// AddMember adds a node to the cluster as a learner. It replicates the log
// but does not vote until [Node.PromoteMember].
func (n *Node) AddMember(ctx context.Context, id uint64, peerURL string) ([]*kvpb.Member, error) {
	if id == 0 || peerURL == "" {
		return nil, fmt.Errorf("%w: a member needs an ID and a peer URL", ErrInvalidRequest)
	}

	if n.member(id) != nil {
		return nil, fmt.Errorf("%w: node %d is already a member", ErrMember, id)
	}

	member := &kvpb.Member{Id: id, PeerUrl: peerURL, Learner: true}

	return n.changeMembership(ctx, raftpb.ConfChangeType_ConfChangeAddLearnerNode, member)
}

// PromoteMember makes a learner a voter. It refuses until the learner is
// within the configured lag of the leader, so promoting cannot leave the
// cluster waiting for a voter that is far behind.
func (n *Node) PromoteMember(ctx context.Context, id uint64) ([]*kvpb.Member, error) {
	m := n.member(id)

	switch {
	case m == nil:
		return nil, fmt.Errorf("%w: node %d is not a member", ErrMember, id)
	case !m.GetLearner():
		return nil, fmt.Errorf("%w: node %d is already a voter", ErrMember, id)
	}

	if !n.IsLeader() {
		return nil, &NotLeaderError{Leader: n.Leader()}
	}

	st := n.raft.Status()
	lead := st.Progress[n.cfg.ID].Match
	learner := st.Progress[id].Match

	if learner+n.cfg.PromoteMaxLag < lead {
		return nil, fmt.Errorf("%w: node %d is %d entries behind the leader; wait for it to catch up",
			ErrMember, id, lead-learner)
	}

	promoted := &kvpb.Member{Id: id, PeerUrl: m.GetPeerUrl()}

	return n.changeMembership(ctx, raftpb.ConfChangeType_ConfChangeAddNode, promoted)
}

// RemoveMember removes a node from the cluster. A removed node stops itself
// once it learns of its removal.
func (n *Node) RemoveMember(ctx context.Context, id uint64) ([]*kvpb.Member, error) {
	if n.member(id) == nil {
		return nil, fmt.Errorf("%w: node %d is not a member", ErrMember, id)
	}

	return n.changeMembership(ctx, raftpb.ConfChangeType_ConfChangeRemoveNode, &kvpb.Member{Id: id})
}

// changeMembership proposes a conf change and waits until it is applied.
// Only the leader proposes changes, and only one at a time.
func (n *Node) changeMembership(
	ctx context.Context,
	typ raftpb.ConfChangeType,
	member *kvpb.Member,
) ([]*kvpb.Member, error) {
	if !n.IsLeader() {
		return nil, &NotLeaderError{Leader: n.Leader()}
	}

	if !n.confPending.CompareAndSwap(false, true) {
		return nil, ErrConfChangePending
	}
	defer n.confPending.Store(false)

	id := newRequestID()

	raw, err := proto.Marshal(&kvpb.ConfChangeContext{RequestId: id, Member: member})
	if err != nil {
		return nil, fmt.Errorf("encoding conf change context: %w", err)
	}

	cc := &raftpb.ConfChange{Type: typ.Enum(), NodeId: new(member.GetId()), Context: raw}

	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()

	ch := n.confChanges.register(string(id))
	defer n.confChanges.cancel(string(id), ch)

	if err := n.raft.ProposeConfChange(ctx, cc); err != nil {
		return nil, proposeError(err)
	}

	select {
	case <-ch:
		n.log.InfoContext(ctx, "membership change applied",
			slog.String("change", typ.String()),
			slog.Uint64("member", member.GetId()),
		)

		return n.Members(), nil
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: %w", ErrOutcomeUnknown, ctx.Err())
	case <-n.stopc:
		return nil, fmt.Errorf("%w: node stopped", ErrOutcomeUnknown)
	}
}

// TransferLeadership hands leadership to target, or with target 0 to the
// most up-to-date voter, and waits for the transfer to complete.
func (n *Node) TransferLeadership(ctx context.Context, target uint64) error {
	if !n.IsLeader() {
		return &NotLeaderError{Leader: n.Leader()}
	}

	st := n.raft.Status()

	if target == 0 {
		target = mostUpToDateVoter(st, n.cfg.ID)
	}

	if pr, ok := st.Progress[target]; !ok || pr.IsLearner || target == n.cfg.ID {
		return fmt.Errorf("%w: node %d is not another voter", ErrMember, target)
	}

	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()

	n.raft.TransferLeadership(ctx, n.cfg.ID, target)

	t := time.NewTicker(leaderPollInterval)
	defer t.Stop()

	for n.Leader() != target {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: leadership transfer to %d did not complete: %w",
				ErrUnavailable, target, ctx.Err())
		case <-n.stopc:
			return fmt.Errorf("%w: node stopped", ErrUnavailable)
		case <-t.C:
		}
	}

	n.log.InfoContext(ctx, "leadership transferred", slog.Uint64("to", target))

	return nil
}

func mostUpToDateVoter(st raft.Status, self uint64) uint64 {
	var best uint64

	var bestMatch uint64

	ids := make([]uint64, 0, len(st.Progress))
	for id := range st.Progress {
		ids = append(ids, id)
	}

	// Sorted for a deterministic choice between equally up-to-date voters.
	slices.Sort(ids)

	for _, id := range ids {
		pr := st.Progress[id]
		if id == self || pr.IsLearner || pr.State != tracker.StateReplicate {
			continue
		}

		if best == 0 || pr.Match > bestMatch {
			best, bestMatch = id, pr.Match
		}
	}

	return best
}

func (n *Node) member(id uint64) *kvpb.Member {
	for _, m := range n.Members() {
		if m.GetId() == id {
			return m
		}
	}

	return nil
}
