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

// newPurgeTask returns the operation that enqueues a purge of the table ptr
// points at. It runs in the transaction that drops the table.
func (s *Store) newPurgeTask(ptr store.Pointer) (*kvpb.RequestOp, error) {
	id, err := store.NewPurgeTaskID()
	if err != nil {
		return nil, err
	}

	value, err := encode(purgeValue{
		Namespace:        slices.Clone(ptr.Ident.Namespace),
		Name:             ptr.Ident.Name,
		MetadataLocation: ptr.MetadataLocation,
		Created:          time.Now().UTC(),
	})
	if err != nil {
		return nil, err
	}

	return client.OpPut(s.k.purge(id), value), nil
}

// ListPurgeTasks implements [store.Store]. Task IDs are UUIDv7, so key order
// is creation order.
func (s *Store) ListPurgeTasks(ctx context.Context, limit int) ([]store.PurgeTask, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if limit <= 0 {
		return nil, fmt.Errorf("%w: non-positive limit %d", store.ErrInvalidArgument, limit)
	}

	prefix := s.k.purges()

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Success: []*kvpb.RequestOp{client.OpRange(prefix, prefixEnd(prefix), int64(limit))},
	})
	if err != nil {
		return nil, err
	}

	kvs := resp.GetResponses()[0].GetResponseRange().GetKvs()
	tasks := make([]store.PurgeTask, 0, len(kvs))

	for _, kv := range kvs {
		task, err := purgeTask(nameOf(kv.GetKey(), prefix), kv)
		if err != nil {
			return nil, err
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

// ClaimPurgeTask implements [store.Store].
func (s *Store) ClaimPurgeTask(
	ctx context.Context,
	task store.PurgeTask,
	claim store.PurgeClaim,
) (store.PurgeTask, error) {
	if err := checkCtx(ctx); err != nil {
		return store.PurgeTask{}, err
	}

	value, err := encode(purgeValue{
		Namespace:        slices.Clone(task.Ident.Namespace),
		Name:             task.Ident.Name,
		MetadataLocation: task.MetadataLocation,
		Created:          task.Created,
		ClaimOwner:       claim.Owner,
		ClaimUntil:       claim.Until,
	})
	if err != nil {
		return store.PurgeTask{}, err
	}

	key := s.k.purge(task.ID)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{atRevision(key, int64(task.Version))},
		Success: []*kvpb.RequestOp{client.OpPut(key, value)},
	})
	if err != nil {
		return store.PurgeTask{}, err
	}

	if !resp.GetSucceeded() {
		return store.PurgeTask{}, fmt.Errorf("%w: %q", store.ErrPurgeTaskChanged, task.ID)
	}

	claimed := task
	claimed.Ident = cloneIdent(task.Ident)
	claimed.Claim = claim
	claimed.Version = store.Version(resp.GetHeader().GetRevision())

	return claimed, nil
}

// CompletePurgeTask implements [store.Store].
func (s *Store) CompletePurgeTask(ctx context.Context, task store.PurgeTask) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	key := s.k.purge(task.ID)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{atRevision(key, int64(task.Version))},
		Success: []*kvpb.RequestOp{client.OpDelete(key, nil)},
	})
	if err != nil {
		return err
	}

	if !resp.GetSucceeded() {
		return fmt.Errorf("%w: %q", store.ErrPurgeTaskChanged, task.ID)
	}

	return nil
}

func purgeTask(id string, kv *kvpb.KeyValue) (store.PurgeTask, error) {
	var v purgeValue
	if err := decode(kv.GetValue(), &v); err != nil {
		return store.PurgeTask{}, err
	}

	return store.PurgeTask{
		ID:               id,
		Ident:            store.Ident{Namespace: v.Namespace, Name: v.Name},
		MetadataLocation: v.MetadataLocation,
		Created:          v.Created,
		Claim:            store.PurgeClaim{Owner: v.ClaimOwner, Until: v.ClaimUntil},
		Version:          store.Version(kv.GetModRevision()),
	}, nil
}
