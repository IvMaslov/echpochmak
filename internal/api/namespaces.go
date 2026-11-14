package api

import (
	"context"
	"fmt"
	"maps"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// ListNamespaces implements [gen.StrictServerInterface].
func (s *Server) ListNamespaces(
	ctx context.Context,
	req gen.ListNamespacesRequestObject,
) (gen.ListNamespacesResponseObject, error) {
	if err := s.checkPrefix(req.Prefix); err != nil {
		return nil, err
	}

	parent := store.Namespace{}

	if req.Params.Parent != nil && *req.Params.Parent != "" {
		var err error

		parent, err = store.ParseNamespace(*req.Params.Parent)
		if err != nil {
			return nil, err
		}
	}

	list := func(p store.PageRequest) (store.Page[store.Namespace], error) {
		return s.catalog.ListNamespaces(ctx, parent, p)
	}

	page, err := paginate(req.Params.PageToken, req.Params.PageSize, list)
	if err != nil {
		return nil, err
	}

	namespaces := make([]gen.Namespace, 0, len(page.Items))
	for _, ns := range page.Items {
		namespaces = append(namespaces, gen.Namespace(ns))
	}

	return gen.ListNamespaces200JSONResponse{ListNamespacesResponseJSONResponse: gen.ListNamespacesResponseJSONResponse{
		Namespaces:    &namespaces,
		NextPageToken: nextToken(page),
	}}, nil
}

// CreateNamespace implements [gen.StrictServerInterface].
func (s *Server) CreateNamespace(
	ctx context.Context,
	req gen.CreateNamespaceRequestObject,
) (gen.CreateNamespaceResponseObject, error) {
	if err := s.checkPrefix(req.Prefix); err != nil {
		return nil, err
	}

	if req.Body == nil {
		return nil, fmt.Errorf("%w: missing request body", errBadRequest)
	}

	ns := store.Namespace(req.Body.Namespace)
	props := optionalMap(req.Body.Properties)

	if err := s.catalog.CreateNamespace(ctx, ns, props); err != nil {
		return nil, err
	}

	resp := gen.CreateNamespaceResponseJSONResponse{
		Namespace:  gen.Namespace(ns),
		Properties: &props,
	}

	return gen.CreateNamespace200JSONResponse{CreateNamespaceResponseJSONResponse: resp}, nil
}

// LoadNamespaceMetadata implements [gen.StrictServerInterface].
func (s *Server) LoadNamespaceMetadata(
	ctx context.Context,
	req gen.LoadNamespaceMetadataRequestObject,
) (gen.LoadNamespaceMetadataResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	rec, err := s.catalog.LoadNamespace(ctx, ns)
	if err != nil {
		return nil, err
	}

	resp := gen.GetNamespaceResponseJSONResponse{
		Namespace:  gen.Namespace(rec.Namespace),
		Properties: &rec.Properties,
	}

	return gen.LoadNamespaceMetadata200JSONResponse{GetNamespaceResponseJSONResponse: resp}, nil
}

// NamespaceExists implements [gen.StrictServerInterface].
func (s *Server) NamespaceExists(
	ctx context.Context,
	req gen.NamespaceExistsRequestObject,
) (gen.NamespaceExistsResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	if _, err := s.catalog.LoadNamespace(ctx, ns); err != nil {
		return nil, err
	}

	return gen.NamespaceExists204Response{}, nil
}

// UpdateProperties implements [gen.StrictServerInterface].
func (s *Server) UpdateProperties(
	ctx context.Context,
	req gen.UpdatePropertiesRequestObject,
) (gen.UpdatePropertiesResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	if req.Body == nil {
		return nil, fmt.Errorf("%w: missing request body", errBadRequest)
	}

	upd := store.PropertiesUpdate{Updates: optionalMap(req.Body.Updates)}
	if req.Body.Removals != nil {
		upd.Removals = *req.Body.Removals
	}

	res, err := s.catalog.UpdateNamespaceProperties(ctx, ns, upd)
	if err != nil {
		return nil, err
	}

	return gen.UpdateProperties200JSONResponse{
		UpdateNamespacePropertiesResponseJSONResponse: gen.UpdateNamespacePropertiesResponseJSONResponse{
			Updated: res.Updated,
			Removed: res.Removed,
			Missing: &res.Missing,
		},
	}, nil
}

// DropNamespace implements [gen.StrictServerInterface].
func (s *Server) DropNamespace(
	ctx context.Context,
	req gen.DropNamespaceRequestObject,
) (gen.DropNamespaceResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	if err := s.catalog.DropNamespace(ctx, ns); err != nil {
		return nil, err
	}

	return gen.DropNamespace204Response{}, nil
}

// paginate serves a listing. Per the spec, a client that sends no pageToken
// at all does not support pagination and gets every item; a client that sends
// one, even empty, gets pages.
func paginate[T any](
	token *gen.PageToken,
	size *gen.PageSize,
	list func(store.PageRequest) (store.Page[T], error),
) (store.Page[T], error) {
	if token == nil {
		items, err := catalog.All(list)
		if err != nil {
			return store.Page[T]{}, err
		}

		return store.Page[T]{Items: items}, nil
	}

	req := store.PageRequest{Token: *token}
	if size != nil {
		req.Size = *size
	}

	return list(req)
}

func nextToken[T any](page store.Page[T]) *gen.PageToken {
	if page.NextToken == "" {
		return nil
	}

	return &page.NextToken
}

// optionalMap copies an optional map of a generated request type, whose
// optional fields are pointers.
//
//nolint:gocritic // ptrToRefParam: generated types make optional maps pointers.
func optionalMap(m *map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}

	return maps.Clone(*m)
}
