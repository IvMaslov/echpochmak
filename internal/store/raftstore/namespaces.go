package raftstore

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/store"
)

// CreateNamespace implements [store.Store].
func (s *Store) CreateNamespace(ctx context.Context, ns store.Namespace, props map[string]string) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	if err := ns.Validate(); err != nil {
		return err
	}

	if props == nil {
		props = map[string]string{}
	}

	value, err := encode(namespaceValue{Properties: props})
	if err != nil {
		return err
	}

	key := s.k.namespace(ns)
	nested := !ns.Parent().IsRoot()

	req := &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{absent(key)},
		Success: []*kvpb.RequestOp{client.OpPut(key, value)},
		Failure: []*kvpb.RequestOp{read(key)},
	}

	if nested {
		parent := s.k.namespace(ns.Parent())
		req.Compare = append(req.Compare, present(parent))
		req.Failure = append(req.Failure, read(parent))
	}

	resp, err := s.txn(ctx, req)
	if err != nil {
		return err
	}

	switch {
	case resp.GetSucceeded():
		return nil
	case readResult(resp, 0) != nil:
		return fmt.Errorf("%w: %q", store.ErrNamespaceAlreadyExists, ns.String())
	default:
		return fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, ns.Parent().String())
	}
}

// GetNamespace implements [store.Store].
func (s *Store) GetNamespace(ctx context.Context, ns store.Namespace) (store.NamespaceRecord, error) {
	if err := checkCtx(ctx); err != nil {
		return store.NamespaceRecord{}, err
	}

	if err := ns.Validate(); err != nil {
		return store.NamespaceRecord{}, err
	}

	kv, err := s.get(ctx, s.k.namespace(ns))
	if err != nil {
		return store.NamespaceRecord{}, err
	}

	if kv == nil {
		return store.NamespaceRecord{}, fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, ns.String())
	}

	return namespaceRecord(ns, kv)
}

func namespaceRecord(ns store.Namespace, kv *kvpb.KeyValue) (store.NamespaceRecord, error) {
	var v namespaceValue
	if err := decode(kv.GetValue(), &v); err != nil {
		return store.NamespaceRecord{}, err
	}

	if v.Properties == nil {
		v.Properties = map[string]string{}
	}

	return store.NamespaceRecord{
		Namespace:  slices.Clone(ns),
		Properties: v.Properties,
		Version:    store.Version(kv.GetModRevision()),
	}, nil
}

// ListNamespaces implements [store.Store].
func (s *Store) ListNamespaces(
	ctx context.Context,
	parent store.Namespace,
	req store.PageRequest,
) (store.Page[store.Namespace], error) {
	if err := checkCtx(ctx); err != nil {
		return store.Page[store.Namespace]{}, err
	}

	if !parent.IsRoot() {
		if err := parent.Validate(); err != nil {
			return store.Page[store.Namespace]{}, err
		}
	}

	var exists []byte
	if !parent.IsRoot() {
		exists = s.k.namespace(parent)
	}

	page, err := s.list(ctx, s.k.children(spaceNamespaces, parent), exists, req)
	if err != nil {
		return store.Page[store.Namespace]{}, err
	}

	if page == nil {
		return store.Page[store.Namespace]{}, fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, parent.String())
	}

	out := store.Page[store.Namespace]{Items: make([]store.Namespace, 0, len(page.names)), NextToken: page.next}
	for _, name := range page.names {
		out.Items = append(out.Items, parent.Child(name))
	}

	return out, nil
}

// UpdateNamespaceProperties implements [store.Store]. It reads the
// properties, merges the update and writes them back with compare-and-swap,
// retrying when a concurrent update wins.
func (s *Store) UpdateNamespaceProperties(
	ctx context.Context,
	ns store.Namespace,
	upd store.PropertiesUpdate,
) (store.PropertiesUpdateResult, error) {
	if err := checkCtx(ctx); err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	if err := ns.Validate(); err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	if err := upd.Validate(); err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	key := s.k.namespace(ns)

	for range casRetries {
		kv, err := s.get(ctx, key)
		if err != nil {
			return store.PropertiesUpdateResult{}, err
		}

		if kv == nil {
			return store.PropertiesUpdateResult{}, fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, ns.String())
		}

		rec, err := namespaceRecord(ns, kv)
		if err != nil {
			return store.PropertiesUpdateResult{}, err
		}

		props, result := applyUpdate(rec.Properties, upd)

		value, err := encode(namespaceValue{Properties: props})
		if err != nil {
			return store.PropertiesUpdateResult{}, err
		}

		resp, err := s.txn(ctx, &kvpb.TxnRequest{
			Compare: []*kvpb.Compare{atRevision(key, kv.GetModRevision())},
			Success: []*kvpb.RequestOp{client.OpPut(key, value)},
		})
		if err != nil {
			return store.PropertiesUpdateResult{}, err
		}

		if resp.GetSucceeded() {
			return result, nil
		}
	}

	return store.PropertiesUpdateResult{}, fmt.Errorf("%w: properties of %q changed concurrently %d times",
		store.ErrUnavailable, ns.String(), casRetries)
}

// applyUpdate returns props with upd applied, and the result to report.
func applyUpdate(
	props map[string]string,
	upd store.PropertiesUpdate,
) (map[string]string, store.PropertiesUpdateResult) {
	next := maps.Clone(props)
	result := store.PropertiesUpdateResult{
		Updated: slices.Sorted(maps.Keys(upd.Updates)),
		Removed: []string{},
		Missing: []string{},
	}

	if result.Updated == nil {
		result.Updated = []string{}
	}

	maps.Copy(next, upd.Updates)

	for _, key := range upd.Removals {
		if _, ok := next[key]; !ok {
			result.Missing = append(result.Missing, key)

			continue
		}

		delete(next, key)
		result.Removed = append(result.Removed, key)
	}

	slices.Sort(result.Removed)
	slices.Sort(result.Missing)

	return next, result
}

// DropNamespace implements [store.Store]. Emptiness is checked by three
// range counts inside the transaction that deletes the namespace, so no
// child can be created in between.
func (s *Store) DropNamespace(ctx context.Context, ns store.Namespace) error {
	if err := checkCtx(ctx); err != nil {
		return err
	}

	if err := ns.Validate(); err != nil {
		return err
	}

	key := s.k.namespace(ns)

	resp, err := s.txn(ctx, &kvpb.TxnRequest{
		Compare: []*kvpb.Compare{
			present(key),
			emptyRange(s.k.children(spaceNamespaces, ns)),
			emptyRange(s.k.children(spaceTables, ns)),
			emptyRange(s.k.children(spaceViews, ns)),
		},
		Success: []*kvpb.RequestOp{client.OpDelete(key, nil)},
		Failure: []*kvpb.RequestOp{read(key)},
	})
	if err != nil {
		return err
	}

	switch {
	case resp.GetSucceeded():
		return nil
	case readResult(resp, 0) == nil:
		return fmt.Errorf("%w: %q", store.ErrNamespaceNotFound, ns.String())
	default:
		return fmt.Errorf("%w: %q", store.ErrNamespaceNotEmpty, ns.String())
	}
}

// namePage is one page of names under a prefix.
type namePage struct {
	names []string
	next  string
}

// list returns one page of the names under prefix, after the page token. If
// exists is set, the listing is conditional on that key existing, checked
// in the same read; a nil page means it does not.
func (s *Store) list(ctx context.Context, prefix, exists []byte, req store.PageRequest) (*namePage, error) {
	after, err := store.DecodePageToken(req.Token)
	if err != nil {
		return nil, err
	}

	start := prefix
	if req.Token != "" {
		// The smallest key after the last name returned.
		start = append(append(slices.Clone(prefix), after...), nameSep...)
	}

	limit := req.Limit()

	txn := &kvpb.TxnRequest{
		Success: []*kvpb.RequestOp{client.OpRange(start, prefixEnd(prefix), int64(limit))},
	}

	if exists != nil {
		txn.Compare = []*kvpb.Compare{present(exists)}
	}

	resp, err := s.txn(ctx, txn)
	if err != nil {
		return nil, err
	}

	if !resp.GetSucceeded() {
		return nil, nil //nolint:nilnil // a nil page means the parent is missing; see the doc comment.
	}

	r := resp.GetResponses()[0].GetResponseRange()
	page := &namePage{names: make([]string, 0, len(r.GetKvs()))}

	for _, kv := range r.GetKvs() {
		page.names = append(page.names, nameOf(kv.GetKey(), prefix))
	}

	if r.GetMore() && len(page.names) > 0 {
		page.next = store.EncodePageToken(page.names[len(page.names)-1])
	}

	return page, nil
}
