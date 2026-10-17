// Package api serves the Iceberg REST catalog API over HTTP.
//
// The transport types and routing are generated from the Iceberg OpenAPI spec
// into package gen. This package implements the generated strict server
// interface, maps errors to Iceberg error responses and adds the operational
// endpoints and middleware.
package api

import "echpochmak/internal/api/gen"

// Server implements the Iceberg REST catalog operations.
type Server struct{}

var _ gen.StrictServerInterface = (*Server)(nil)

// NewServer returns a Server.
func NewServer() *Server {
	return &Server{}
}
