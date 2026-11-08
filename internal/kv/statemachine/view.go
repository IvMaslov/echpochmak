package statemachine

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"echpochmak/internal/kv/kvpb"
)

// getKV returns the key-value stored under key, or nil if there is none.
func getKV(r pebble.Reader, key []byte) (*kvpb.KeyValue, error) {
	raw, closer, err := r.Get(dataKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil //nolint:nilnil // a missing key is not an error.
	}

	if err != nil {
		return nil, fmt.Errorf("reading key: %w", err)
	}
	defer closer.Close()

	return decodeKV(key, raw)
}

func decodeKV(key, raw []byte) (*kvpb.KeyValue, error) {
	kv := &kvpb.KeyValue{}
	if err := proto.Unmarshal(raw, kv); err != nil {
		return nil, fmt.Errorf("decoding key %q: %w", key, err)
	}

	kv.Key = bytes.Clone(key)

	return kv, nil
}

// scan calls fn for the keys in [key, rangeEnd) in order, stopping after
// limit keys (0 means no limit). It reports whether more keys exist.
func scan(r pebble.Reader, key, rangeEnd []byte, limit int64, fn func(*kvpb.KeyValue) error) (bool, error) {
	lower, upper := dataRange(key, rangeEnd)

	iter, err := r.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return false, fmt.Errorf("opening iterator: %w", err)
	}
	defer iter.Close()

	var n int64

	for valid := iter.First(); valid; valid = iter.Next() {
		if limit > 0 && n == limit {
			return true, nil
		}

		userKey := iter.Key()[len(prefixData):]

		kv, err := decodeKV(userKey, iter.Value())
		if err != nil {
			return false, err
		}

		if err := fn(kv); err != nil {
			return false, err
		}

		n++
	}

	if err := iter.Error(); err != nil {
		return false, fmt.Errorf("iterating: %w", err)
	}

	return false, nil
}

// countRange counts the keys in [key, rangeEnd), stopping once it reaches
// atMost: callers only need to know how the count compares to a number.
func countRange(r pebble.Reader, key, rangeEnd []byte, atMost int64) (int64, error) {
	var n int64

	_, err := scan(r, key, rangeEnd, atMost, func(*kvpb.KeyValue) error {
		n++

		return nil
	})

	return n, err
}

// rangeOp serves a range request against r.
func rangeOp(r pebble.Reader, req *kvpb.RangeRequest, maxLimit int64) (*kvpb.RangeResponse, error) {
	limit := req.GetLimit()
	if limit <= 0 || limit > maxLimit {
		limit = maxLimit
	}

	resp := &kvpb.RangeResponse{Kvs: []*kvpb.KeyValue{}}

	more, err := scan(r, req.GetKey(), req.GetRangeEnd(), limit, func(kv *kvpb.KeyValue) error {
		resp.Kvs = append(resp.Kvs, kv)

		return nil
	})
	if err != nil {
		return nil, err
	}

	resp.More = more

	return resp, nil
}

// compare evaluates one compare against r.
func compare(r pebble.Reader, c *kvpb.Compare) (bool, error) {
	if target, ok := c.GetTarget().(*kvpb.Compare_RangeCount); ok {
		// Counting one past the target is enough to decide every result.
		n, err := countRange(r, c.GetKey(), c.GetRangeEnd(), target.RangeCount+1)
		if err != nil {
			return false, err
		}

		return holds(c.GetResult(), cmpInt(n, target.RangeCount)), nil
	}

	kv, err := getKV(r, c.GetKey())
	if err != nil {
		return false, err
	}

	switch target := c.GetTarget().(type) {
	case *kvpb.Compare_ModRevision:
		return holds(c.GetResult(), cmpInt(kv.GetModRevision(), target.ModRevision)), nil
	case *kvpb.Compare_Version:
		return holds(c.GetResult(), cmpInt(kv.GetVersion(), target.Version)), nil
	case *kvpb.Compare_Value:
		if kv == nil {
			return false, nil
		}

		return holds(c.GetResult(), bytes.Compare(kv.GetValue(), target.Value)), nil
	default:
		return false, fmt.Errorf("%w: compare without target", ErrInvalidRequest)
	}
}

func compareAll(r pebble.Reader, compares []*kvpb.Compare) (bool, error) {
	for _, c := range compares {
		ok, err := compare(r, c)
		if err != nil || !ok {
			return false, err
		}
	}

	return true, nil
}

func cmpInt(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// holds reports whether a comparison outcome (-1, 0, 1) satisfies result.
func holds(result kvpb.Compare_Result, cmp int) bool {
	switch result {
	case kvpb.Compare_RESULT_EQUAL:
		return cmp == 0
	case kvpb.Compare_RESULT_NOT_EQUAL:
		return cmp != 0
	case kvpb.Compare_RESULT_LESS:
		return cmp < 0
	case kvpb.Compare_RESULT_GREATER:
		return cmp > 0
	case kvpb.Compare_RESULT_UNSPECIFIED:
		return false
	default:
		return false
	}
}
