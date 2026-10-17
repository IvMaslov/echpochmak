package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// Operational endpoints, outside the Iceberg API.
const (
	pathHealthz = "/healthz"
	pathReadyz  = "/readyz"
)

const readyTimeout = 2 * time.Second

// Options configures [NewHandler].
type Options struct {
	Logger *slog.Logger
	Store  store.Store
	Server *Server
}

// NewHandler returns the HTTP handler of the catalog server: the Iceberg REST
// API under /v1, plus liveness and readiness probes.
func NewHandler(opts Options) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET "+pathHealthz, handleHealthz)
	mux.HandleFunc("GET "+pathReadyz, readyzHandler(opts.Store))
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

	return withRequestLogging(opts.Logger, withRecovery(mux))
}

// probeResponse is the body of the health and readiness probes.
type probeResponse struct {
	Status string `json:"status"`
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, probeResponse{Status: "ok"})
}

func readyzHandler(s store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
