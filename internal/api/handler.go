package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/idempotency"
	"echpochmak/internal/logger"
)

// Operational endpoints, outside the Iceberg API.
const (
	pathHealthz = "/healthz"
	pathReadyz  = "/readyz"
)

const readyTimeout = 2 * time.Second

// DefaultMaxBodyBytes is the request body limit when Options.MaxBodyBytes is
// not set.
const DefaultMaxBodyBytes = 16 << 20

// Pinger reports whether a dependency can serve requests.
type Pinger interface {
	Ping(ctx context.Context) error
}

// Options configures [NewHandler].
type Options struct {
	Logger *slog.Logger
	// Ready is checked by the readiness probe.
	Ready Pinger
	// Draining, when set and true, makes the readiness probe fail while the
	// server keeps serving, so a load balancer stops sending requests before
	// shutdown.
	Draining func() bool
	Server   *Server
	// MaxBodyBytes limits request bodies; zero means DefaultMaxBodyBytes.
	MaxBodyBytes int64
	// Idempotency, when set, enables the Idempotency-Key protocol for the
	// mutations in scope.
	Idempotency *idempotency.Guard
}

// NewHandler returns the HTTP handler of the catalog server: the Iceberg REST
// API under /v1, plus liveness and readiness probes.
func NewHandler(opts Options) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET "+pathHealthz, handleHealthz)
	mux.HandleFunc("GET "+pathReadyz, readyzHandler(opts.Ready, opts.Draining))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeErrorModel(w, r, http.StatusNotFound, typeNotFound, "no route for "+r.Method+" "+r.URL.Path)
	})

	strict := gen.NewStrictHandlerWithOptions(opts.Server, nil, gen.StrictHTTPServerOptions{
		RequestErrorHandlerFunc:  writeBadRequest,
		ResponseErrorHandlerFunc: writeError,
	})
	gen.HandlerWithOptions(strict, gen.StdHTTPServerOptions{
		BaseRouter:       mux,
		ErrorHandlerFunc: writeBadRequest,
	})

	maxBody := opts.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = DefaultMaxBodyBytes
	}

	routed := withRoute(mux)

	return withRequestLogging(opts.Logger, withRecovery(withRawBody(maxBody,
		withIdempotency(opts.Idempotency, mux, routed))))
}

// probeResponse is the body of the health and readiness probes.
type probeResponse struct {
	Status string `json:"status"`
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, probeResponse{Status: "ok"})
}

func readyzHandler(s Pinger, draining func() bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if draining != nil && draining() {
			writeJSON(w, r, http.StatusServiceUnavailable, probeResponse{Status: "draining"})

			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()

		if err := s.Ping(ctx); err != nil {
			logger.FromContext(ctx).WarnContext(ctx, "store not ready", logger.Err(err))
			writeJSON(w, r, http.StatusServiceUnavailable, probeResponse{Status: "unavailable"})

			return
		}

		writeJSON(w, r, http.StatusOK, probeResponse{Status: "ok"})
	}
}
