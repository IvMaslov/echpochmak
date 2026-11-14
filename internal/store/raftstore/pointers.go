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

// DropTable implements [store.Store].
func (s *Store) DropTable(ctx context.Context, id store.Ident) error {
	return s.drop(ctx, tables, id)
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
		return store.Pointer{}, fmt.Errorf("%w: %q", kind.errNotFound, id.String())
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
		return store.Page[store.Ident]{}, fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, ns.String())
	}

	out := store.Page[store.Ident]{Items: make([]store.Ident, 0, len(page.names)), NextToken: page.next}
	for _, name := range page.names {
		out.Items = append(out.Items, store.Ident{Namespace: slices.Clone(ns), Name: name})
	}

	return out, nil
}

// Reads in the failure branch of a commit, per commit, in this order.
const (
	readTable = iota
	readView
	readNamespace
	readsPerCommit
)

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

	req := &kvpb.TxnRequest{}

	for _, c := range commits {
		key := s.pointerKey(kind, c.Ident)

		if c.IsCreate() {
			req.Compare = append(req.Compare,
				absent(s.pointerKey(tables, c.Ident)),
				absent(s.pointerKey(views, c.Ident)),
				present(s.k.namespace(c.Ident.Namespace)),
			)
		} else {
			req.Compare = append(req.Compare, atRevision(key, int64(c.ExpectedVersion)))
		}

		value, err := encode(pointerValue{MetadataLocation: c.MetadataLocation})
		if err != nil {
			return nil, err
		}

		req.Success = append(req.Success, client.OpPut(key, value))
		req.Failure = append(req.Failure,
			read(s.pointerKey(tables, c.Ident)),
			read(s.pointerKey(views, c.Ident)),
			read(s.k.namespace(c.Ident.Namespace)),
		)
	}

	resp, err := s.txn(ctx, req)
	if err != nil {
		return nil, err
	}

	if !resp.GetSucceeded() {
		return nil, commitFailure(kind, commits, resp)
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
// hold, from the keys read by the failure branch.
func commitFailure(kind pointerKind, commits []store.Commit, resp *kvpb.TxnResponse) error {
	for i, c := range commits {
		base := i * readsPerCommit
		table := readResult(resp, base+readTable)
		view := readResult(resp, base+readView)

		if c.IsCreate() {
			switch {
			case readResult(resp, base+readNamespace) == nil:
				return fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, c.Ident.Namespace.String())
			case table != nil:
				return fmt.Errorf("%w: %q", store.ErrTableAlreadyExists, c.Ident.String())
			case view != nil:
				return fmt.Errorf("%w: %q", store.ErrViewAlreadyExists, c.Ident.String())
			}

			continue
		}

		own := table
		if kind.space == spaceViews {
			own = view
		}

		switch {
		case own == nil:
			return fmt.Errorf("%w: %q", kind.errNotFound, c.Ident.String())
		case store.Version(own.GetModRevision()) != c.ExpectedVersion:
			return &store.CommitConflictError{
				Ident:    cloneIdent(c.Ident),
				Expected: c.ExpectedVersion,
				Actual:   store.Version(own.GetModRevision()),
			}
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
			return fmt.Errorf("%w: %q", kind.errNotFound, from.String())
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
			return fmt.Errorf("%w: %q", kind.errNotFound, from.String())
		case readResult(resp, 1) == nil:
			return fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, to.Namespace.String())
		case readResult(resp, 2) != nil:
			return fmt.Errorf("%w: %q", store.ErrTableAlreadyExists, to.String())
		case readResult(resp, 3) != nil:
			return fmt.Errorf("%w: %q", store.ErrViewAlreadyExists, to.String())
		}
		// Only the source changed, under a concurrent commit: try again.
	}

	return fmt.Errorf("%w: %q changed concurrently %d times", store.ErrUnavailable, from.String(), casRetries)
}

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
		return fmt.Errorf("%w: %q", kind.errNotFound, id.String())
	}

	return nil
}

func validateCommits(commits []store.Commit) error {
	if len(commits) == 0 {
		return fmt.Errorf("%w: no commits", store.ErrInvalidArgument)
	}

	if len(commits) > MaxCommitsPerTxn {
		return fmt.Errorf("%w: %d commits exceed the limit of %d per transaction",
			store.ErrInvalidArgument, len(commits), MaxCommitsPerTxn)
	}

	seen := make(map[string]bool, len(commits))

	for _, c := range commits {
		if err := c.Validate(); err != nil {
			return err
		}

		key := c.Ident.Namespace.Child(c.Ident.Name).Encode()
		if seen[key] {
			return fmt.Errorf("%w: duplicate commit for %q", store.ErrInvalidArgument, c.Ident.String())
		}

		seen[key] = true
	}

	return nil
}

func cloneIdent(id store.Ident) store.Ident {
	return store.Ident{Namespace: slices.Clone(id.Namespace), Name: id.Name}
}
