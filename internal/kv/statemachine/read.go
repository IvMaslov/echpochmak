package statemachine

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"echpochmak/internal/kv/kvpb"
)

// View is a consistent, read-only view of the state.
type View struct {
	snap   *pebble.Snapshot
	limits Limits
}

// NewView returns a view of the current state. Close it when done.
func (sm *StateMachine) NewView() *View {
	return &View{snap: sm.db.NewSnapshot(), limits: sm.limits}
}

// Close releases the view.
func (v *View) Close() {
	_ = v.snap.Close()
}

// Range serves a range request.
func (v *View) Range(req *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	if err := v.limits.ValidateRange(req); err != nil {
		return nil, err
	}

	resp, err := rangeOp(v.snap, req, v.limits.MaxRangeLimit)
	if err != nil {
		return nil, err
	}

	header, err := v.header()
	if err != nil {
		return nil, err
	}

	resp.Header = header

	return resp, nil
}

// Txn serves a read-only transaction; see [IsReadOnly].
func (v *View) Txn(req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	if !IsReadOnly(req) {
		return nil, fmt.Errorf("%w: transaction writes and must be proposed", ErrInvalidRequest)
	}

	if err := v.limits.ValidateTxn(req); err != nil {
		return nil, err
	}

	ok, err := compareAll(v.snap, req.GetCompare())
	if err != nil {
		return nil, err
	}

	ops := req.GetFailure()
	if ok {
		ops = req.GetSuccess()
	}

	responses := make([]*kvpb.ResponseOp, 0, len(ops))

	for _, op := range ops {
		resp, err := rangeOp(v.snap, op.GetRequestRange(), v.limits.MaxRangeLimit)
		if err != nil {
			return nil, err
		}

		responses = append(responses, &kvpb.ResponseOp{Response: &kvpb.ResponseOp_ResponseRange{ResponseRange: resp}})
	}

	header, err := v.header()
	if err != nil {
		return nil, err
	}

	return &kvpb.TxnResponse{Header: header, Succeeded: ok, Responses: responses}, nil
}

func (v *View) header() (*kvpb.ResponseHeader, error) {
	state, err := readAppliedState(v.snap)
	if err != nil {
		return nil, err
	}

	return &kvpb.ResponseHeader{Revision: state.GetRevision()}, nil
}

// IsReadOnly reports whether a transaction only reads, so it can be served
// without going through raft.
func IsReadOnly(req *kvpb.TxnRequest) bool {
	for _, ops := range [][]*kvpb.RequestOp{req.GetSuccess(), req.GetFailure()} {
		for _, op := range ops {
			if op.GetRequestRange() == nil {
				return false
			}
		}
	}

	return true
}
