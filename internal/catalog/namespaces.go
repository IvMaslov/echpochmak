package catalog

import (
	"context"
	"fmt"
	"log/slog"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// CreateNamespace creates ns with the given properties.
func (s *Service) CreateNamespace(ctx context.Context, ns store.Namespace, props map[string]string) error {
	if err := s.store.CreateNamespace(ctx, ns, props); err != nil {
		return err
	}

	s.logFor(ctx).InfoContext(ctx, "namespace created", slog.String(logger.KeyNamespace, ns.String()))

	return nil
}

// LoadNamespace returns ns and its properties.
func (s *Service) LoadNamespace(ctx context.Context, ns store.Namespace) (store.NamespaceRecord, error) {
	return s.store.GetNamespace(ctx, ns)
}

// ListNamespaces returns one page of the direct children of parent. An empty
// parent lists the top-level namespaces.
func (s *Service) ListNamespaces(
	ctx context.Context,
	parent store.Namespace,
	req store.PageRequest,
) (store.Page[store.Namespace], error) {
	return s.store.ListNamespaces(ctx, parent, req)
}

// UpdateNamespaceProperties sets and removes properties of ns atomically.
func (s *Service) UpdateNamespaceProperties(
	ctx context.Context,
	ns store.Namespace,
	upd store.PropertiesUpdate,
) (store.PropertiesUpdateResult, error) {
	for _, key := range upd.Removals {
		if _, ok := upd.Updates[key]; ok {
			return store.PropertiesUpdateResult{}, fmt.Errorf("%w: %q", ErrPropertyConflict, key)
		}
	}

	res, err := s.store.UpdateNamespaceProperties(ctx, ns, upd)
	if err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	s.logFor(ctx).InfoContext(ctx, "namespace properties updated",
		slog.String(logger.KeyNamespace, ns.String()),
		slog.Any("updated", res.Updated),
		slog.Any("removed", res.Removed),
	)

	return res, nil
}

// DropNamespace removes ns, which must be empty.
func (s *Service) DropNamespace(ctx context.Context, ns store.Namespace) error {
	if err := s.store.DropNamespace(ctx, ns); err != nil {
		return err
	}

	s.logFor(ctx).InfoContext(ctx, "namespace dropped", slog.String(logger.KeyNamespace, ns.String()))

	return nil
}

// All walks every page of a listing and returns all items. It serves clients
// that do not support pagination.
func All[T any](list func(store.PageRequest) (store.Page[T], error)) ([]T, error) {
	items := []T{}
	req := store.PageRequest{Size: store.MaxPageSize}

	for {
		page, err := list(req)
		if err != nil {
			return nil, err
		}

		items = append(items, page.Items...)

		if page.NextToken == "" {
			return items, nil
		}

		req.Token = page.NextToken
	}
}
