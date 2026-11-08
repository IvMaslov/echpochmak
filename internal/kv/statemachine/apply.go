package statemachine

import (
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// Batch applies committed entries atomically. Every entry is applied to the
// same Pebble batch, which also records the new applied state; nothing is
// visible to readers until Commit.
type Batch struct {
	sm    *StateMachine
	b     *pebble.Batch
	state *kvpb.AppliedState
}

// NewBatch starts applying entries on top of the current state.
func (sm *StateMachine) NewBatch() *Batch {
	return &Batch{
		sm:    sm,
		b:     sm.db.NewIndexedBatch(),
		state: sm.Applied(),
	}
}

// Close releases the batch. It is safe to call after Commit.
func (b *Batch) Close() {
	_ = b.b.Close()
}

// Commit writes the batch and the new applied state. It does not need to be
// synced: entries after the last synced applied index are applied again from
// the raft log after a crash.
func (b *Batch) Commit(sync bool) error {
	raw, err := proto.Marshal(b.state)
	if err != nil {
		return fmt.Errorf("encoding applied state: %w", err)
	}

	if err := b.b.Set([]byte(keyApplied), raw, nil); err != nil {
		return fmt.Errorf("writing applied state: %w", err)
	}

	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}

	if err := b.b.Commit(opts); err != nil {
		return fmt.Errorf("committing applied entries: %w", err)
	}

	b.sm.SetApplied(b.state)

	return nil
}

// ApplyNoop applies an entry without a write, such as the empty entry a new
// leader appends.
func (b *Batch) ApplyNoop(index, term uint64) {
	b.advance(index, term)
}

// Apply applies a write and returns its result. A write whose request ID was
// already applied is not executed again; its earlier result is returned.
// An error means the database failed, not the request: invalid requests get
// a Result with Error set.
func (b *Batch) Apply(index, term uint64, p *kvpb.Proposal) (*kvpb.Result, error) {
	b.advance(index, term)

	id := p.GetRequestId()
	if len(id) > 0 {
		rec, err := b.dedupRecord(id)
		if err != nil {
			return nil, err
		}

		if rec != nil {
			return rec.GetResult(), nil
		}
	}

	res, err := b.execute(p)
	if err != nil {
		return nil, err
	}

	if len(id) > 0 {
		if err := b.remember(id, res); err != nil {
			return nil, err
		}
	}

	if err := b.pruneDedup(); err != nil {
		return nil, err
	}

	return res, nil
}

// ApplyConfChange records a membership change. cs is the configuration raft
// returned for the change, and member the node it concerns, if known.
func (b *Batch) ApplyConfChange(
	index, term uint64,
	cc *raftpb.ConfChange,
	cs *raftpb.ConfState,
	member *kvpb.Member,
) error {
	b.advance(index, term)

	raw, err := proto.Marshal(cs)
	if err != nil {
		return fmt.Errorf("encoding conf state: %w", err)
	}

	b.state.ConfState = raw
	b.state.Members = updateMembers(b.state.GetMembers(), cc, member)

	return nil
}

func (b *Batch) advance(index, term uint64) {
	b.state.Index = index
	b.state.Term = term
}

func (b *Batch) execute(p *kvpb.Proposal) (*kvpb.Result, error) {
	if err := b.sm.limits.ValidateProposal(p); err != nil {
		// An invalid proposal is a result, not a failure: every node records
		// the same verdict for the entry.
		return &kvpb.Result{Error: err.Error()}, nil //nolint:nilerr // see above.
	}

	switch op := p.GetOperation().(type) {
	case *kvpb.Proposal_Put:
		rev := b.state.GetRevision() + 1
		if err := b.put(op.Put, rev); err != nil {
			return nil, err
		}

		b.state.Revision = rev

		return &kvpb.Result{Result: &kvpb.Result_Put{Put: &kvpb.PutResponse{Header: b.header()}}}, nil
	case *kvpb.Proposal_DeleteRange:
		rev := b.state.GetRevision() + 1

		n, err := b.deleteRange(op.DeleteRange)
		if err != nil {
			return nil, err
		}

		if n > 0 {
			b.state.Revision = rev
		}

		resp := &kvpb.DeleteRangeResponse{Header: b.header(), Deleted: n}

		return &kvpb.Result{Result: &kvpb.Result_DeleteRange{DeleteRange: resp}}, nil
	case *kvpb.Proposal_Txn:
		resp, err := b.txn(op.Txn)
		if err != nil {
			return nil, err
		}

		return &kvpb.Result{Result: &kvpb.Result_Txn{Txn: resp}}, nil
	default:
		return &kvpb.Result{Error: "proposal has no operation"}, nil
	}
}

// txn runs a transaction. All of its writes share one new revision, which
// is only taken if something was written.
func (b *Batch) txn(req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	ok, err := compareAll(b.b, req.GetCompare())
	if err != nil {
		return nil, err
	}

	ops := req.GetFailure()
	if ok {
		ops = req.GetSuccess()
	}

	rev := b.state.GetRevision() + 1
	wrote := false
	responses := make([]*kvpb.ResponseOp, 0, len(ops))

	for _, op := range ops {
		resp, changed, err := b.op(op, rev)
		if err != nil {
			return nil, err
		}

		wrote = wrote || changed

		responses = append(responses, resp)
	}

	if wrote {
		b.state.Revision = rev
	}

	return &kvpb.TxnResponse{Header: b.header(), Succeeded: ok, Responses: responses}, nil
}

// op runs one operation of a transaction branch at revision rev and reports
// whether it changed the state.
func (b *Batch) op(op *kvpb.RequestOp, rev int64) (*kvpb.ResponseOp, bool, error) {
	switch r := op.GetRequest().(type) {
	case *kvpb.RequestOp_RequestRange:
		resp, err := rangeOp(b.b, r.RequestRange, b.sm.limits.MaxRangeLimit)
		if err != nil {
			return nil, false, err
		}

		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_ResponseRange{ResponseRange: resp}}, false, nil
	case *kvpb.RequestOp_RequestPut:
		if err := b.put(r.RequestPut, rev); err != nil {
			return nil, false, err
		}

		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_ResponsePut{ResponsePut: &kvpb.PutResponse{}}}, true, nil
	case *kvpb.RequestOp_RequestDeleteRange:
		n, err := b.deleteRange(r.RequestDeleteRange)
		if err != nil {
			return nil, false, err
		}

		resp := &kvpb.DeleteRangeResponse{Deleted: n}

		return &kvpb.ResponseOp{Response: &kvpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: resp}}, n > 0, nil
	default:
		return nil, false, fmt.Errorf("%w: operation has no request", ErrInvalidRequest)
	}
}

func (b *Batch) put(req *kvpb.PutRequest, rev int64) error {
	kv, err := getKV(b.b, req.GetKey())
	if err != nil {
		return err
	}

	next := &kvpb.KeyValue{
		Value:          req.GetValue(),
		CreateRevision: rev,
		ModRevision:    rev,
		Version:        1,
	}

	if kv != nil {
		next.CreateRevision = kv.GetCreateRevision()
		next.Version = kv.GetVersion() + 1
	}

	raw, err := proto.Marshal(next)
	if err != nil {
		return fmt.Errorf("encoding key: %w", err)
	}

	if err := b.b.Set(dataKey(req.GetKey()), raw, nil); err != nil {
		return fmt.Errorf("writing key: %w", err)
	}

	return nil
}

func (b *Batch) deleteRange(req *kvpb.DeleteRangeRequest) (int64, error) {
	keys := [][]byte{}

	if _, err := scan(b.b, req.GetKey(), req.GetRangeEnd(), 0, func(kv *kvpb.KeyValue) error {
		keys = append(keys, kv.GetKey())

		return nil
	}); err != nil {
		return 0, err
	}

	for _, k := range keys {
		if err := b.b.Delete(dataKey(k), nil); err != nil {
			return 0, fmt.Errorf("deleting key: %w", err)
		}
	}

	return int64(len(keys)), nil
}

func (b *Batch) header() *kvpb.ResponseHeader {
	return &kvpb.ResponseHeader{Revision: b.state.GetRevision()}
}

func (b *Batch) dedupRecord(id []byte) (*kvpb.DedupRecord, error) {
	raw, closer, err := b.b.Get(dedupKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil //nolint:nilnil // no record is not an error.
	}

	if err != nil {
		return nil, fmt.Errorf("reading dedup record: %w", err)
	}
	defer closer.Close()

	rec := &kvpb.DedupRecord{}
	if err := proto.Unmarshal(raw, rec); err != nil {
		return nil, fmt.Errorf("decoding dedup record: %w", err)
	}

	return rec, nil
}

func (b *Batch) remember(id []byte, res *kvpb.Result) error {
	rev := b.state.GetRevision()

	raw, err := proto.Marshal(&kvpb.DedupRecord{Revision: rev, Result: res})
	if err != nil {
		return fmt.Errorf("encoding dedup record: %w", err)
	}

	if err := b.b.Set(dedupKey(id), raw, nil); err != nil {
		return fmt.Errorf("writing dedup record: %w", err)
	}

	if err := b.b.Set(dedupIndexKey(rev, id), nil, nil); err != nil {
		return fmt.Errorf("writing dedup index: %w", err)
	}

	return nil
}

// pruneDedup forgets results older than the dedup window. It goes by
// revision, not time, so every node prunes the same records.
func (b *Batch) pruneDedup() error {
	bound := b.state.GetRevision() - b.sm.window
	if bound <= 0 {
		return nil
	}

	iter, err := b.b.NewIter(&pebble.IterOptions{
		LowerBound: []byte(prefixDedupByRev),
		UpperBound: dedupIndexKey(bound, nil),
	})
	if err != nil {
		return fmt.Errorf("opening dedup index: %w", err)
	}

	stale := [][]byte{}
	for valid := iter.First(); valid; valid = iter.Next() {
		stale = append(stale, append([]byte(nil), iter.Key()...))
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("iterating dedup index: %w", err)
	}

	for _, k := range stale {
		_, id := parseDedupIndexKey(k)

		if err := b.b.Delete(k, nil); err != nil {
			return fmt.Errorf("pruning dedup index: %w", err)
		}

		if err := b.b.Delete(dedupKey(id), nil); err != nil {
			return fmt.Errorf("pruning dedup record: %w", err)
		}
	}

	return nil
}

// updateMembers returns members after a conf change, sorted by ID.
func updateMembers(members []*kvpb.Member, cc *raftpb.ConfChange, member *kvpb.Member) []*kvpb.Member {
	id := cc.GetNodeId()
	out := make([]*kvpb.Member, 0, len(members)+1)

	var existing *kvpb.Member

	for _, m := range members {
		if m.GetId() == id {
			existing = m

			continue
		}

		out = append(out, m)
	}

	switch cc.GetType() {
	case raftpb.ConfChangeType_ConfChangeAddNode, raftpb.ConfChangeType_ConfChangeAddLearnerNode:
		next := &kvpb.Member{Id: id}

		switch {
		case member != nil:
			next.PeerUrl = member.GetPeerUrl()
		case existing != nil:
			next.PeerUrl = existing.GetPeerUrl()
		}

		next.Learner = cc.GetType() == raftpb.ConfChangeType_ConfChangeAddLearnerNode
		out = append(out, next)
	case raftpb.ConfChangeType_ConfChangeRemoveNode:
		// Already left out above.
	case raftpb.ConfChangeType_ConfChangeUpdateNode:
		if existing != nil {
			out = append(out, existing)
		}
	}

	slices.SortFunc(out, func(a, b *kvpb.Member) int { return cmp.Compare(a.GetId(), b.GetId()) })

	return out
}
