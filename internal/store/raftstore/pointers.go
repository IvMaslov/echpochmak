package raftstore

import (
	"context"
	"fmt"
	"slices"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/store"
)

// pointerKind is a table or a view. Both point at a metadata file and share
// one name space within a namespace.
type pointerKind struct {
	space       string
	errNotFound error
}

//nolint:gochecknoglobals // read-only descriptions of the two kinds.
var (
	tables = pointerKind{space: spaceTables, errNotFound: store.ErrTableNotFound}
	views  = pointerKind{space: spaceViews, errNotFound: store.ErrViewNotFound}
)

// GetTable implements [store.Store].
func (s *Store) GetTable(ctx context.Context, id store.Ident) (store.Pointer, error) {
	return s.getPointer(ctx, tables, id)
}

// ListTables implements [store.Store].
func (s *Store) ListTables(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return s.listPointers(ctx, tables, ns, req)
}

// CommitTables implements [store.Store]: one transaction for all commits.
func (s *Store) CommitTables(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
	return s.commit(ctx, tables, commits)
}

// RenameTable implements [store.Store].
func (s *Store) RenameTable(ctx context.Context, from, to store.Ident) error {
	return s.rename(ctx, tables, from, to)
}

// GetTables implements [store.Store]: one read-only transaction, so every
// pointer is read at the same revision.
func (s *Store) GetTables(ctx context.Context, ids []store.Ident) ([]store.Pointer, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := validateIdents(ids); err != nil {
		return nil, err
	}

	req := &kvpb.TxnRequest{Success: make([]*kvpb.RequestOp, 0, len(ids))}
	for _, id := range ids {
		req.Success = append(req.Success, read(s.pointerKey(tables, id)))
	}

	resp, err := s.txn(ctx, req)
	if err != nil {
		return nil, err
	}

	ptrs := make([]store.Pointer, 0, len(ids))

	for i, id := range ids {
		kv := readResult(resp, i)
		if kv == nil {
			ptrs = append(ptrs, store.Pointer{Ident: cloneIdent(id)})

			continue
		}

		p, err := pointer(id, kv)
		if err != nil {
			return nil, err
		}

		ptrs = append(ptrs, p)
	}

	return ptrs, nil
}

// DropTable implements [store.Store]. The pointer returned is exactly the one
// removed: a plain drop reads and deletes the key in one transaction, and a
// drop with purge deletes it at the revision it read, so the purge task names
// the table's final metadata file.
func (s *Store) DropTable(ctx context.Context, id store.Ident, opts store.DropOptions) (store.Pointer, error) {
	if err := checkCtx(ctx); err != nil {
		return store.Pointer{}, err
	}

	if err := id.Validate(); err != nil {
		return store.Pointer{}, err
	}

	if opts.Purge {
		return s.dropAndPurge(ctx, id)
	}

	key := s.pointerKey(tables, id)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{present(key)},
		Success: []*kvpb.RequestOp{read(key), client.OpDelete(key, nil)},
	})
	if err != nil {
		return store.Pointer{}, err
	}

	if !resp.GetSucceeded() {
		return store.Pointer{}, fmt.Errorf("%w: %s", store.ErrTableNotFound, id.String())
	}

	return pointer(id, readResult(resp, 0))
}

// dropAndPurge drops a table and enqueues its purge task in one transaction.
// The task's value must hold the metadata location, so the pointer is read
// first and deleted only at that revision; a concurrent commit makes it
// retry.
func (s *Store) dropAndPurge(ctx context.Context, id store.Ident) (store.Pointer, error) {
	key := s.pointerKey(tables, id)

	for attempt := range casRetries {
		kv, err := s.get(ctx, key)
		if err != nil {
			return store.Pointer{}, err
		}

		if kv == nil {
			return store.Pointer{}, fmt.Errorf("%w: %s", store.ErrTableNotFound, id.String())
		}

		ptr, err := pointer(id, kv)
		if err != nil {
			return store.Pointer{}, err
		}

		task, err := s.newPurgeTask(ptr)
		if err != nil {
			return store.Pointer{}, err
		}

		resp, err := s.txn(ctx, &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{atRevision(key, kv.GetModRevision())},
			Success: []*kvpb.RequestOp{client.OpDelete(key, nil), task},
		})
		if err != nil {
			return store.Pointer{}, err
		}

		if resp.GetSucceeded() {
			return ptr, nil
		}

		// A concurrent commit won: wait a little, so a busy table lets the
		// drop through, and read it again.
		if err := pause(ctx, attempt); err != nil {
			return store.Pointer{}, err
		}
	}

	return store.Pointer{}, fmt.Errorf(
		"%w: %q changed concurrently %d times",
		store.ErrUnavailable,
		id.String(),
		casRetries,
	)
}

// GetView implements [store.Store].
func (s *Store) GetView(ctx context.Context, id store.Ident) (store.Pointer, error) {
	return s.getPointer(ctx, views, id)
}

// ListViews implements [store.Store].
func (s *Store) ListViews(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return s.listPointers(ctx, views, ns, req)
}

// CommitView implements [store.Store].
func (s *Store) CommitView(ctx context.Context, commit store.Commit) (store.Pointer, error) {
	ptrs, err := s.commit(ctx, views, []store.Commit{commit})
	if err != nil {
		return store.Pointer{}, err
	}

	return ptrs[0], nil
}

// RenameView implements [store.Store].
func (s *Store) RenameView(ctx context.Context, from, to store.Ident) error {
	return s.rename(ctx, views, from, to)
}

// DropView implements [store.Store].
func (s *Store) DropView(ctx context.Context, id store.Ident) error {
	return s.drop(ctx, views, id)
}

func (s *Store) pointerKey(kind pointerKind, id store.Ident) []byte {
	return s.k.child(kind.space, id.Namespace, id.Name)
}

func (s *Store) getPointer(ctx context.Context, kind pointerKind, id store.Ident) (store.Pointer, error) {
	if err := checkCtx(ctx); err != nil {
		return store.Pointer{}, err
	}

	if err := id.Validate(); err != nil {
		return store.Pointer{}, err
	}

	kv, err := s.get(ctx, s.pointerKey(kind, id))
	if err != nil {
		return store.Pointer{}, err
	}

	if kv == nil {
		return store.Pointer{}, fmt.Errorf("%w: %s", kind.errNotFound, id.String())
	}

	return pointer(id, kv)
}

func pointer(id store.Ident, kv *kvpb.KeyValue) (store.Pointer, error) {
	var v pointerValue
	if err := decode(kv.GetValue(), &v); err != nil {
		return store.Pointer{}, err
	}

	return store.Pointer{
		Ident:            cloneIdent(id),
		MetadataLocation: v.MetadataLocation,
		Version:          store.Version(kv.GetModRevision()),
	}, nil
}

func (s *Store) listPointers(
	ctx context.Context,
	kind pointerKind,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	if err := checkCtx(ctx); err != nil {
		return store.Page[store.Ident]{}, err
	}

	if err := ns.Validate(); err != nil {
		return store.Page[store.Ident]{}, err
	}

	page, err := s.list(ctx, s.k.children(kind.space, ns), s.k.namespace(ns), req)
	if err != nil {
		return store.Page[store.Ident]{}, err
	}

	if page == nil {
		return store.Page[store.Ident]{}, fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, ns.String())
	}

	out := store.Page[store.Ident]{Items: make([]store.Ident, 0, len(page.names)), NextToken: page.next}
	for _, name := range page.names {
		out.Items = append(out.Items, store.Ident{Namespace: slices.Clone(ns), Name: name})
	}

	return out, nil
}

// commit swaps pointers of one kind in one transaction. Each commit's
// precondition is a compare; the failure branch reads every key involved, so
// the first failing commit can be reported precisely.
func (s *Store) commit(ctx context.Context, kind pointerKind, commits []store.Commit) ([]store.Pointer, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}

	if err := validateCommits(commits); err != nil {
		return nil, err
	}

	intent, err := store.CommitsIntent(commits)
	if err != nil {
		return nil, err
	}

	req := &kvpb.TxnRequest{}

	for _, c := range commits {
		key := s.pointerKey(kind, c.Ident)

		// The failure branch reads the keys the compares check, in the same
		// order; commitFailure relies on it.
		if c.IsCreate() {
			req.Compare = append(req.Compare,
				absent(s.pointerKey(tables, c.Ident)),
				absent(s.pointerKey(views, c.Ident)),
				present(s.k.namespace(c.Ident.Namespace)),
			)
			req.Failure = append(req.Failure,
				read(s.pointerKey(tables, c.Ident)),
				read(s.pointerKey(views, c.Ident)),
				read(s.k.namespace(c.Ident.Namespace)),
			)
		} else {
			req.Compare = append(req.Compare, atRevision(key, int64(c.ExpectedVersion)))
			req.Failure = append(req.Failure, read(key))
		}

		value, err := encode(pointerValue{MetadataLocation: c.MetadataLocation})
		if err != nil {
			return nil, err
		}

		req.Success = append(req.Success, client.OpPut(key, value))
	}

	// The intent comes last, so commitFailure finds it after the commits.
	if !intent.IsZero() {
		key := s.k.intent(intent.ID)
		req.Compare = append(req.Compare, atRevision(key, int64(intent.Version)))
		req.Success = append(req.Success, client.OpDelete(key, nil))
		req.Failure = append(req.Failure, read(key))
	}

	resp, err := s.txn(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.GetSucceeded() {
		return nil, commitFailure(kind, commits, intent, resp)
	}

	version := store.Version(resp.GetHeader().GetRevision())
	ptrs := make([]store.Pointer, 0, len(commits))

	for _, c := range commits {
		ptrs = append(ptrs, store.Pointer{
			Ident:            cloneIdent(c.Ident),
			MetadataLocation: c.MetadataLocation,
			Version:          version,
		})
	}

	return ptrs, nil
}

// commitFailure finds the first commit, in order, whose precondition did not
// hold, from the keys read by the failure branch; if every commit's held, the
// intent had changed.
func commitFailure(
	kind pointerKind,
	commits []store.Commit,
	intent store.IntentRef,
	resp *kvpb.TxnResponse,
) error {
	next := 0

	for _, c := range commits {
		if c.IsCreate() {
			table := readResult(resp, next)
			view := readResult(resp, next+1)
			ns := readResult(resp, next+2)
			next += createCost

			switch {
			case ns == nil:
				return fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, c.Ident.Namespace.String())
			case table != nil:
				return fmt.Errorf("%w: %s", store.ErrTableAlreadyExists, c.Ident.String())
			case view != nil:
				return fmt.Errorf("%w: %s", store.ErrViewAlreadyExists, c.Ident.String())
			}

			continue
		}

		own := readResult(resp, next)
		next += updateCost

		switch {
		case own == nil:
			return fmt.Errorf("%w: %s", kind.errNotFound, c.Ident.String())
		case store.Version(own.GetModRevision()) != c.ExpectedVersion:
			return &store.CommitConflictError{
				Ident:    cloneIdent(c.Ident),
				Expected: c.ExpectedVersion,
				Actual:   store.Version(own.GetModRevision()),
			}
		}
	}

	if !intent.IsZero() {
		kv := readResult(resp, next)
		if kv == nil || store.Version(kv.GetModRevision()) != intent.Version {
			return fmt.Errorf("%w: %q", store.ErrIntentChanged, intent.ID)
		}
	}

	// Every precondition holds in the state the failure branch read, which
	// cannot happen in one transaction; report a conflict to be retried.
	return fmt.Errorf("%w: preconditions changed during the commit", store.ErrCommitConflict)
}

// rename moves a pointer, keeping its metadata location. The source's
// revision is part of the precondition, so a concurrent commit to the source
// is not lost: the rename retries with the new location.
func (s *Store) rename(ctx context.Context, kind pointerKind, from, to store.Ident) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	if err := from.Validate(); err != nil {
		return err
	}

	if err := to.Validate(); err != nil {
		return err
	}

	fromKey := s.pointerKey(kind, from)

	for range casRetries {
		src, err := s.get(ctx, fromKey)
		if err != nil {
			return err
		}

		if src == nil {
			return fmt.Errorf("%w: %s", kind.errNotFound, from.String())
		}

		resp, err := s.txn(ctx, &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{
				atRevision(fromKey, src.GetModRevision()),
				present(s.k.namespace(to.Namespace)),
				absent(s.pointerKey(tables, to)),
				absent(s.pointerKey(views, to)),
			},
			Success: []*kvpb.RequestOp{
				client.OpDelete(fromKey, nil),
				client.OpPut(s.pointerKey(kind, to), src.GetValue()),
			},
			Failure: []*kvpb.RequestOp{
				read(fromKey),
				read(s.k.namespace(to.Namespace)),
				read(s.pointerKey(tables, to)),
				read(s.pointerKey(views, to)),
			},
		})
		if err != nil {
			return err
		}

		switch {
		case resp.GetSucceeded():
			return nil
		case readResult(resp, 0) == nil:
			return fmt.Errorf("%w: %s", kind.errNotFound, from.String())
		case readResult(resp, 1) == nil:
			return fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, to.Namespace.String())
		case readResult(resp, 2) != nil:
			return fmt.Errorf("%w: %s", store.ErrTableAlreadyExists, to.String())
		case readResult(resp, 3) != nil:
			return fmt.Errorf("%w: %s", store.ErrViewAlreadyExists, to.String())
		}
		// Only the source changed, under a concurrent commit: try again.
	}

	return fmt.Errorf("%w: %q changed concurrently %d times", store.ErrUnavailable, from.String(), casRetries)
}

// drop deletes a pointer that exists, whatever its revision.
func (s *Store) drop(ctx context.Context, kind pointerKind, id store.Ident) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	if err := id.Validate(); err != nil {
		return err
	}

	key := s.pointerKey(kind, id)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{present(key)},
		Success: []*kvpb.RequestOp{client.OpDelete(key, nil)},
	})
	if err != nil {
		return err
	}

	if !resp.GetSucceeded() {
		return fmt.Errorf("%w: %s", kind.errNotFound, id.String())
	}

	return nil
}

func validateCommits(commits []store.Commit) error {
	if len(commits) == 0 {
		return fmt.Errorf("%w: no commits", store.ErrInvalidArgument)
	}

	seen := make(map[string]bool, len(commits))
	cost := 0

	for _, c := range commits {
		if err := c.Validate(); err != nil {
			return err
		}

		cost += updateCost
		if c.IsCreate() {
			cost += createCost - updateCost
		}

		key := c.Ident.Namespace.Child(c.Ident.Name).Encode()
		if seen[key] {
			return fmt.Errorf("%w: duplicate commit for %q", store.ErrInvalidArgument, c.Ident.String())
		}

		seen[key] = true
	}

	if !commits[0].Intent.IsZero() {
		cost += intentCost
	}

	if cost > TxnBudget {
		return fmt.Errorf(
			"%w: the transaction is too large: it costs %d of %d (an update costs %d, a create %d, an intent %d)",
			store.ErrInvalidArgument,
			cost,
			TxnBudget,
			updateCost,
			createCost,
			intentCost,
		)
	}

	return nil
}

// validateIdents checks the identifiers of a batch read: valid, none twice,
// and few enough for one transaction.
func validateIdents(ids []store.Ident) error {
	if len(ids) > TxnBudget {
		return fmt.Errorf(
			"%w: %d tables exceed the limit of %d per read",
			store.ErrInvalidArgument,
			len(ids),
			TxnBudget,
		)
	}

	seen := make(map[string]bool, len(ids))

	for _, id := range ids {
		if err := id.Validate(); err != nil {
			return err
		}

		key := id.Namespace.Child(id.Name).Encode()
		if seen[key] {
			return fmt.Errorf("%w: %q requested twice", store.ErrInvalidArgument, id.String())
		}

		seen[key] = true
	}

	return nil
}

func cloneIdent(id store.Ident) store.Ident {
	return store.Ident{Namespace: slices.Clone(id.Namespace), Name: id.Name}
}
