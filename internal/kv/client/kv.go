package client

import (
	"context"

	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// RangeAllFromKey is the range end that means every key from the start key
// on.
var RangeAllFromKey = []byte{0} //nolint:gochecknoglobals // a protocol constant; byte slices cannot be const.

// Range returns keys in [key, rangeEnd). Reads are linearizable unless
// req.Serializable is set.
func (c *Client) Range(ctx context.Context, req *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	var resp *kvpb.RangeResponse

	err := c.do(ctx, true, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.kv[i].Range(ctx, req)

		return err
	})

	return resp, err
}

// Get returns the key-value stored under key, or nil if there is none.
func (c *Client) Get(ctx context.Context, key []byte) (*kvpb.KeyValue, error) {
	resp, err := c.Range(ctx, &kvpb.RangeRequest{Key: key})
	if err != nil {
		return nil, err
	}

	if len(resp.GetKvs()) == 0 {
		return nil, nil //nolint:nilnil // a missing key is not an error.
	}

	return resp.GetKvs()[0], nil
}

// Put writes a key.
func (c *Client) Put(ctx context.Context, key, value []byte) (*kvpb.PutResponse, error) {
	req := &kvpb.PutRequest{Key: key, Value: value, RequestId: NewRequestID()}

	var resp *kvpb.PutResponse

	err := c.do(ctx, true, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.kv[i].Put(ctx, req)

		return err
	})

	return resp, err
}

// Delete deletes the keys in [key, rangeEnd); an empty rangeEnd deletes only
// key.
func (c *Client) Delete(ctx context.Context, key, rangeEnd []byte) (*kvpb.DeleteRangeResponse, error) {
	req := &kvpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd, RequestId: NewRequestID()}

	var resp *kvpb.DeleteRangeResponse

	err := c.do(ctx, true, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.kv[i].DeleteRange(ctx, req)

		return err
	})

	return resp, err
}

// Txn runs a transaction. A request ID is added if req has none; pass your
// own to make retries across separate calls safe.
func (c *Client) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	if len(req.GetRequestId()) == 0 {
		req = proto.CloneOf(req)
		req.RequestId = NewRequestID()
	}

	var resp *kvpb.TxnResponse

	err := c.do(ctx, true, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.kv[i].Txn(ctx, req)

		return err
	})

	return resp, err
}

// CompareModRevision compares the revision of key's last write; 0 means the
// key does not exist.
func CompareModRevision(key []byte, result kvpb.Compare_Result, rev int64) *kvpb.Compare {
	return &kvpb.Compare{Key: key, Result: result, Target: &kvpb.Compare_ModRevision{ModRevision: rev}}
}

// CompareRangeCount compares the number of keys in [key, rangeEnd).
func CompareRangeCount(key, rangeEnd []byte, result kvpb.Compare_Result, n int64) *kvpb.Compare {
	return &kvpb.Compare{
		Key:      key,
		RangeEnd: rangeEnd,
		Result:   result,
		Target:   &kvpb.Compare_RangeCount{RangeCount: n},
	}
}

// OpPut is a put operation for a Txn.
func OpPut(key, value []byte) *kvpb.RequestOp {
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_RequestPut{RequestPut: &kvpb.PutRequest{Key: key, Value: value}}}
}

// OpDelete is a delete operation for a Txn.
func OpDelete(key, rangeEnd []byte) *kvpb.RequestOp {
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_RequestDeleteRange{
		RequestDeleteRange: &kvpb.DeleteRangeRequest{Key: key, RangeEnd: rangeEnd},
	}}
}

// OpRange is a range operation for a Txn.
func OpRange(key, rangeEnd []byte, limit int64) *kvpb.RequestOp {
	return &kvpb.RequestOp{Request: &kvpb.RequestOp_RequestRange{
		RequestRange: &kvpb.RangeRequest{Key: key, RangeEnd: rangeEnd, Limit: limit},
	}}
}

// PrefixEnd returns the range end that covers every key with prefix.
func PrefixEnd(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++

			return end[:i+1]
		}
	}

	return RangeAllFromKey
}
