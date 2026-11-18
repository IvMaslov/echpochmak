package raftstore

import (
	"context"
	"fmt"
	"slices"
	"time"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/store"
)

// CreateIntent implements [store.Store]. Intent IDs are UUIDv7, so key order
// is creation order.
func (s *Store) CreateIntent(ctx context.Context, intent store.Intent) (store.Intent, error) {
	if err := checkCtx(ctx); err != nil {
		return store.Intent{}, err
	}

	if err := intent.Validate(); err != nil {
		return store.Intent{}, err
	}

	id, err := store.NewIntentID()
	if err != nil {
		return store.Intent{}, err
	}

	created := cloneIntent(intent)
	created.ID = id
	created.Created = time.Now().UTC()
	created.Condemned = false

	value, err := encodeIntent(created)
	if err != nil {
		return store.Intent{}, err
	}

	key := s.k.intent(id)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{absent(key)},
		Success: []*kvpb.RequestOp{client.OpPut(key, value)},
	})
	if err != nil {
		return store.Intent{}, err
	}

	if !resp.GetSucceeded() {
		// A UUIDv7 collision; the caller retries the attempt.
		return store.Intent{}, fmt.Errorf("%w: intent ID %q already taken", store.ErrIntentChanged, id)
	}

	created.Version = store.Version(resp.GetHeader().GetRevision())

	return created, nil
}

// ListIntents implements [store.Store].
func (s *Store) ListIntents(ctx context.Context, limit int) ([]store.Intent, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if limit <= 0 {
		return nil, fmt.Errorf("%w: non-positive limit %d", store.ErrInvalidArgument, limit)
	}

	prefix := s.k.intents()

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Success: []*kvpb.RequestOp{client.OpRange(prefix, prefixEnd(prefix), int64(limit))},
	})
	if err != nil {
		return nil, err
	}

	kvs := resp.GetResponses()[0].GetResponseRange().GetKvs()
	intents := make([]store.Intent, 0, len(kvs))

	for _, kv := range kvs {
		intent, err := decodeIntent(nameOf(kv.GetKey(), prefix), kv)
		if err != nil {
			return nil, err
		}

		intents = append(intents, intent)
	}

	return intents, nil
}

// CondemnIntent implements [store.Store]: a compare-and-swap on the intent's
// revision, which moves it, so a commit holding the old revision fails.
func (s *Store) CondemnIntent(ctx context.Context, ref store.IntentRef) (store.Intent, error) {
	if err := checkCtx(ctx); err != nil {
		return store.Intent{}, err
	}

	key := s.k.intent(ref.ID)

	// The value is rewritten from what the transaction reads, so the read
	// and the swap must see the same revision: read first, then swap at it.
	kv, err := s.get(ctx, key)
	if err != nil {
		return store.Intent{}, err
	}

	if kv == nil || store.Version(kv.GetModRevision()) != ref.Version {
		return store.Intent{}, fmt.Errorf("%w: %q", store.ErrIntentChanged, ref.ID)
	}

	intent, err := decodeIntent(ref.ID, kv)
	if err != nil {
		return store.Intent{}, err
	}

	intent.Condemned = true

	value, err := encodeIntent(intent)
	if err != nil {
		return store.Intent{}, err
	}

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{atRevision(key, int64(ref.Version))},
		Success: []*kvpb.RequestOp{client.OpPut(key, value)},
	})
	if err != nil {
		return store.Intent{}, err
	}

	if !resp.GetSucceeded() {
		return store.Intent{}, fmt.Errorf("%w: %q", store.ErrIntentChanged, ref.ID)
	}

	intent.Version = store.Version(resp.GetHeader().GetRevision())

	return intent, nil
}

// ReleaseIntent implements [store.Store].
func (s *Store) ReleaseIntent(ctx context.Context, ref store.IntentRef) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	key := s.k.intent(ref.ID)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{atRevision(key, int64(ref.Version))},
		Success: []*kvpb.RequestOp{client.OpDelete(key, nil)},
	})
	if err != nil {
		return err
	}

	if !resp.GetSucceeded() {
		return fmt.Errorf("%w: %q", store.ErrIntentChanged, ref.ID)
	}

	return nil
}

func encodeIntent(intent store.Intent) ([]byte, error) {
	idents := make([]identValue, 0, len(intent.Idents))
	for _, id := range intent.Idents {
		idents = append(idents, identValue{Namespace: slices.Clone(id.Namespace), Name: id.Name})
	}

	return encode(intentValue{
		Locations: slices.Clone(intent.Locations),
		Idents:    idents,
		Owner:     intent.Owner,
		Created:   intent.Created,
		Condemned: intent.Condemned,
	})
}

func decodeIntent(id string, kv *kvpb.KeyValue) (store.Intent, error) {
	var v intentValue
	if err := decode(kv.GetValue(), &v); err != nil {
		return store.Intent{}, err
	}

	idents := make([]store.Ident, 0, len(v.Idents))
	for _, iv := range v.Idents {
		idents = append(idents, store.Ident{Namespace: iv.Namespace, Name: iv.Name})
	}

	return store.Intent{
		ID:        id,
		Locations: v.Locations,
		Idents:    idents,
		Owner:     v.Owner,
		Created:   v.Created,
		Condemned: v.Condemned,
		Version:   store.Version(kv.GetModRevision()),
	}, nil
}

func cloneIntent(intent store.Intent) store.Intent {
	intent.Locations = slices.Clone(intent.Locations)

	idents := make([]store.Ident, 0, len(intent.Idents))
	for _, id := range intent.Idents {
		idents = append(idents, cloneIdent(id))
	}

	intent.Idents = idents

	return intent
}
