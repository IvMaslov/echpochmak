package idempotency

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
)

// Key spaces of the KV store, under the catalog's prefix.
const (
	spaceRecords = "idem/"
	spaceExpiry  = "idemexp/"
)

// KVStore keeps records in the KV cluster, shared by every catalog replica.
// A record's key is "<prefix>idem/<key>"; an index entry
// "<prefix>idemexp/<expiry>/<key>" orders records by expiry for the janitor.
type KVStore struct {
	c      *client.Client
	prefix string
}

var _ Store = (*KVStore)(nil)

// NewKVStore returns a store under prefix, normally the catalog's key prefix.
func NewKVStore(c *client.Client, prefix string) *KVStore {
	return &KVStore{c: c, prefix: prefix}
}

func (s *KVStore) recordKey(key string) []byte {
	return []byte(s.prefix + spaceRecords + key)
}

// expiryKey sorts by expiry: a big-endian timestamp sorts like the time.
func (s *KVStore) expiryKey(at time.Time, key string) []byte {
	k := []byte(s.prefix + spaceExpiry)
	k = binary.BigEndian.AppendUint64(k, uint64(at.UnixNano()))

	return append(append(k, '/'), key...)
}

// Create implements [Store].
func (s *KVStore) Create(ctx context.Context, key string, rec Record) (Record, bool, error) {
	raw, err := json.Marshal(rec)
	if err != nil {
		return Record{}, false, fmt.Errorf("encoding idempotency record: %w", err)
	}

	k := s.recordKey(key)

	resp, err := s.c.Txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{client.CompareModRevision(k, kvpb.Compare_RESULT_EQUAL, 0)},
		Success: []*kvpb.RequestOp{
			client.OpPut(k, raw),
			client.OpPut(s.expiryKey(rec.ExpiresAt, key), nil),
		},
		Failure: []*kvpb.RequestOp{client.OpRange(k, nil, 0)},
	})
	if err != nil {
		return Record{}, false, fmt.Errorf("creating idempotency record: %w", err)
	}

	if resp.GetSucceeded() {
		rec.Version = resp.GetHeader().GetRevision()

		return rec, true, nil
	}

	kvs := resp.GetResponses()[0].GetResponseRange().GetKvs()
	if len(kvs) == 0 {
		// Deleted between the compare and the read; the caller retries.
		return Record{}, false, errors.New("idempotency record vanished during creation")
	}

	existing, err := decode(kvs[0])

	return existing, false, err
}

// Replace implements [Store].
func (s *KVStore) Replace(ctx context.Context, key string, version int64, rec Record) (Record, bool, error) {
	raw, err := json.Marshal(rec)
	if err != nil {
		return Record{}, false, fmt.Errorf("encoding idempotency record: %w", err)
	}

	k := s.recordKey(key)

	resp, err := s.c.Txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{client.CompareModRevision(k, kvpb.Compare_RESULT_EQUAL, version)},
		Success: []*kvpb.RequestOp{client.OpPut(k, raw)},
	})
	if err != nil {
		return Record{}, false, fmt.Errorf("replacing idempotency record: %w", err)
	}

	if !resp.GetSucceeded() {
		return Record{}, false, nil
	}

	rec.Version = resp.GetHeader().GetRevision()

	return rec, true, nil
}

// Delete implements [Store]. The expiry index entry is left for the
// janitor, which deletes index entries whatever their record's state.
func (s *KVStore) Delete(ctx context.Context, key string, version int64) (bool, error) {
	k := s.recordKey(key)

	resp, err := s.c.Txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{client.CompareModRevision(k, kvpb.Compare_RESULT_EQUAL, version)},
		Success: []*kvpb.RequestOp{client.OpDelete(k, nil)},
	})
	if err != nil {
		return false, fmt.Errorf("deleting idempotency record: %w", err)
	}

	return resp.GetSucceeded(), nil
}

// Expire implements [Store]. A record's expiry never changes, so every
// index entry before now names a record that is expired or already gone.
func (s *KVStore) Expire(ctx context.Context, now time.Time, limit int) (int, error) {
	start := []byte(s.prefix + spaceExpiry)

	resp, err := s.c.Range(ctx, &kvpb.RangeRequest{
		Key:      start,
		RangeEnd: s.expiryKey(now, ""),
		Limit:    int64(limit),
	})
	if err != nil {
		return 0, fmt.Errorf("listing expired idempotency records: %w", err)
	}

	for _, kv := range resp.GetKvs() {
		key := string(kv.GetKey()[len(start)+8+1:])

		if _, err := s.c.Txn(ctx, &kvpb.TxnRequest{Success: []*kvpb.RequestOp{
			client.OpDelete(s.recordKey(key), nil),
			client.OpDelete(kv.GetKey(), nil),
		}}); err != nil {
			return 0, fmt.Errorf("deleting an expired idempotency record: %w", err)
		}
	}

	return len(resp.GetKvs()), nil
}

func decode(kv *kvpb.KeyValue) (Record, error) {
	var rec Record
	if err := json.Unmarshal(kv.GetValue(), &rec); err != nil {
		return Record{}, fmt.Errorf("decoding idempotency record: %w", err)
	}

	rec.Version = kv.GetModRevision()

	return rec, nil
}
