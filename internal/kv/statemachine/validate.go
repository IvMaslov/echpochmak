package statemachine

import (
	"errors"
	"fmt"

	"echpochmak/internal/kv/kvpb"
)

// ErrInvalidRequest means a request breaks the protocol or the limits.
var ErrInvalidRequest = errors.New("invalid request")

// Limits bounds what a single request may do.
type Limits struct {
	MaxKeyBytes int
	// MaxOps is the maximum number of compares, and of operations per branch,
	// in a Txn.
	MaxOps int
	// MaxRangeLimit caps the number of keys a Range returns.
	MaxRangeLimit int64
}

// DefaultLimits returns the limits used when none are configured.
func DefaultLimits() Limits {
	return Limits{
		MaxKeyBytes:   4096,
		MaxOps:        128,
		MaxRangeLimit: 10000,
	}
}

// ValidateProposal reports whether p is a well-formed write. The same check
// runs before proposing and when applying, so every node reaches the same
// verdict on every entry.
func (l Limits) ValidateProposal(p *kvpb.Proposal) error {
	switch op := p.GetOperation().(type) {
	case *kvpb.Proposal_Put:
		return l.validatePut(op.Put)
	case *kvpb.Proposal_DeleteRange:
		return l.validateRange(op.DeleteRange.GetKey(), op.DeleteRange.GetRangeEnd())
	case *kvpb.Proposal_Txn:
		return l.ValidateTxn(op.Txn)
	default:
		return fmt.Errorf("%w: proposal has no operation", ErrInvalidRequest)
	}
}

// ValidateRange reports whether a range request is well-formed.
func (l Limits) ValidateRange(req *kvpb.RangeRequest) error {
	if req.GetLimit() < 0 {
		return fmt.Errorf("%w: negative limit", ErrInvalidRequest)
	}

	return l.validateRange(req.GetKey(), req.GetRangeEnd())
}

// ValidateTxn reports whether a transaction is well-formed.
func (l Limits) ValidateTxn(txn *kvpb.TxnRequest) error {
	tooMany := len(txn.GetCompare()) > l.MaxOps ||
		len(txn.GetSuccess()) > l.MaxOps ||
		len(txn.GetFailure()) > l.MaxOps
	if tooMany {
		return fmt.Errorf("%w: more than %d compares or operations per branch", ErrInvalidRequest, l.MaxOps)
	}

	for _, c := range txn.GetCompare() {
		if err := l.validateCompare(c); err != nil {
			return err
		}
	}

	for _, ops := range [][]*kvpb.RequestOp{txn.GetSuccess(), txn.GetFailure()} {
		for _, op := range ops {
			if err := l.validateOp(op); err != nil {
				return err
			}
		}
	}

	return nil
}

func (l Limits) validateOp(op *kvpb.RequestOp) error {
	switch r := op.GetRequest().(type) {
	case *kvpb.RequestOp_RequestRange:
		return l.ValidateRange(r.RequestRange)
	case *kvpb.RequestOp_RequestPut:
		return l.validatePut(r.RequestPut)
	case *kvpb.RequestOp_RequestDeleteRange:
		return l.validateRange(r.RequestDeleteRange.GetKey(), r.RequestDeleteRange.GetRangeEnd())
	default:
		return fmt.Errorf("%w: operation has no request", ErrInvalidRequest)
	}
}

func (l Limits) validateCompare(c *kvpb.Compare) error {
	if c.GetResult() == kvpb.Compare_RESULT_UNSPECIFIED {
		return fmt.Errorf("%w: compare without result", ErrInvalidRequest)
	}

	switch c.GetTarget().(type) {
	case *kvpb.Compare_RangeCount:
		return l.validateRange(c.GetKey(), c.GetRangeEnd())
	case *kvpb.Compare_ModRevision, *kvpb.Compare_Version, *kvpb.Compare_Value:
		if len(c.GetRangeEnd()) > 0 {
			return fmt.Errorf("%w: range_end is only allowed for range_count", ErrInvalidRequest)
		}

		return l.validateKey(c.GetKey())
	default:
		return fmt.Errorf("%w: compare without target", ErrInvalidRequest)
	}
}

func (l Limits) validatePut(put *kvpb.PutRequest) error {
	return l.validateKey(put.GetKey())
}

func (l Limits) validateKey(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("%w: empty key", ErrInvalidRequest)
	}

	if len(key) > l.MaxKeyBytes {
		return fmt.Errorf("%w: key longer than %d bytes", ErrInvalidRequest, l.MaxKeyBytes)
	}

	return nil
}

// validateRange accepts an empty key only for "every key" ranges.
func (l Limits) validateRange(key, rangeEnd []byte) error {
	if len(key) == 0 && string(rangeEnd) == rangeAllFromKey {
		return nil
	}

	if err := l.validateKey(key); err != nil {
		return err
	}

	if len(rangeEnd) > l.MaxKeyBytes {
		return fmt.Errorf("%w: range end longer than %d bytes", ErrInvalidRequest, l.MaxKeyBytes)
	}

	return nil
}
