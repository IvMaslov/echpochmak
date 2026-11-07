// Package api serves the Iceberg REST catalog API over HTTP.
//
// The transport types and routing are generated from the Iceberg OpenAPI spec
// into package gen. This package implements the generated strict server
// interface, maps errors to Iceberg error responses and adds the operational
// endpoints and middleware.
//
// Requests that carry table metadata changes (create and commit) are decoded
// from the raw body with iceberg-go instead of the generated types, which do
// not model every field (TableRequirement keeps only its type, for example).
// Responses that carry table metadata are written from the stored JSON, so
// clients get every field exactly as stored.
package api

import (
	"context"
	"fmt"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// Server implements the Iceberg REST catalog operations.
type Server struct {
	catalog *catalog.Service
	prefix  string
}

var _ gen.StrictServerInterface = (*Server)(nil)

// NewServer returns a Server for the catalog named prefix: clients put the
// prefix, returned by /v1/config, in every request path.
func NewServer(svc *catalog.Service, prefix string) *Server {
	return &Server{catalog: svc, prefix: prefix}
}

// checkPrefix rejects requests for another catalog.
func (s *Server) checkPrefix(prefix gen.Prefix) error {
	if prefix != s.prefix {
		return fmt.Errorf("%w: unknown catalog prefix %q, this server serves %q", errNotFound, prefix, s.prefix)
	}

	return nil
}

// namespace checks the prefix and parses the namespace path parameter.
func (s *Server) namespace(prefix gen.Prefix, ns gen.NamespaceParam) (store.Namespace, error) {
	if err := s.checkPrefix(prefix); err != nil {
		return nil, err
	}

	return store.ParseNamespace(ns)
}

// ident checks the prefix and parses the namespace and table path parameters.
func (s *Server) ident(prefix gen.Prefix, ns gen.NamespaceParam, name gen.Table) (store.Ident, error) {
	namespace, err := s.namespace(prefix, ns)
	if err != nil {
		return store.Ident{}, err
	}

	id := store.Ident{Namespace: namespace, Name: name}
	if err := id.Validate(); err != nil {
		return store.Ident{}, err
	}

	return id, nil
}

// GetConfig implements [gen.StrictServerInterface].
func (s *Server) GetConfig(context.Context, gen.GetConfigRequestObject) (gen.GetConfigResponseObject, error) {
	endpoints := Endpoints()

	return gen.GetConfig200JSONResponse{
		Defaults:  map[string]string{},
		Overrides: map[string]string{"prefix": s.prefix},
		Endpoints: &endpoints,
	}, nil
}

// Endpoints returns the operations this server implements, in the format of
// the endpoints field of /v1/config. Clients call only these, so features
// that are not implemented yet are skipped instead of failing.
func Endpoints() []string {
	return []string{
		"GET /v1/{prefix}/namespaces",
		"POST /v1/{prefix}/namespaces",
		"GET /v1/{prefix}/namespaces/{namespace}",
		"HEAD /v1/{prefix}/namespaces/{namespace}",
		"DELETE /v1/{prefix}/namespaces/{namespace}",
		"POST /v1/{prefix}/namespaces/{namespace}/properties",
		"GET /v1/{prefix}/namespaces/{namespace}/tables",
		"POST /v1/{prefix}/namespaces/{namespace}/tables",
		"GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
		"HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
		"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}",
		"DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}",
		"POST /v1/{prefix}/tables/rename",
		"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/metrics",
	}
}
