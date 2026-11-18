package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"echpochmak/internal/logger"
)

// HeaderRequestID carries the request ID. A valid incoming value is kept, so
// a request can be traced across services; otherwise a new one is generated.
const HeaderRequestID = "X-Request-ID"

const maxRequestIDLen = 128

// withRequestLogging assigns a request ID, stores a logger tagged with it in
// the request context, and writes one access log line per request.
func withRequestLogging(base *slog.Logger, m *Metrics, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		id := r.Header.Get(HeaderRequestID)
		if !validRequestID(id) {
			id = newRequestID()
		}

		w.Header().Set(HeaderRequestID, id)
		trace.SpanFromContext(r.Context()).SetAttributes(attribute.String("http.request_id", id))

		log := base.With(slog.String(logger.KeyRequestID, id))
		route := &routeHolder{}
		ctx := context.WithValue(logger.WithContext(r.Context(), log), routeKey{}, route)
		r = r.WithContext(ctx)
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		pattern := route.pattern
		if pattern == "" {
			pattern = "unmatched"
		}

		elapsed := time.Since(start)
		m.observe(r, pattern, rec.status, elapsed, route.idempotency)

		level := slog.LevelInfo
		if isProbe(r) {
			level = slog.LevelDebug
		}

		attrs := []slog.Attr{
			slog.String(logger.KeyMethod, r.Method),
			slog.String(logger.KeyRoute, pattern),
			slog.String(logger.KeyPath, r.URL.Path),
			slog.Int(logger.KeyStatus, rec.status),
			slog.Duration(logger.KeyDuration, elapsed),
			slog.Int64(logger.KeyBytes, rec.bytes),
			slog.String(logger.KeyRemote, r.RemoteAddr),
		}

		if route.idempotency != "" {
			attrs = append(attrs, slog.String("idempotency", route.idempotency))
		}

		log.LogAttrs(r.Context(), level, "http request", attrs...)
	})
}

type routeKey struct{}

// routeHolder carries the matched route pattern back to the logging
// middleware. Middleware between the two may replace the request, so the
// pattern the mux sets on its request is not visible to the logger directly.
type routeHolder struct {
	pattern string
	// idempotency is what the Idempotency-Key protocol decided, if the
	// request carried a key.
	idempotency string
}

// setIdempotency records the protocol's decision for the request.
func setIdempotency(r *http.Request, outcome string) {
	if route, ok := r.Context().Value(routeKey{}).(*routeHolder); ok {
		route.idempotency = outcome
	}
}

// withRoute records the route pattern that mux matched. It must wrap the mux
// directly.
func withRoute(mux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)

		if route, ok := r.Context().Value(routeKey{}).(*routeHolder); ok {
			route.pattern = r.Pattern
		}

		// Name the span after the route template, which is only known now.
		if r.Pattern != "" {
			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Pattern)
			span.SetAttributes(attribute.String("http.route", r.Pattern))
		}
	})
}

// withRecovery turns a panic in a handler into a 500 response, so one bad
// request cannot take down the server.
func withRecovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			v := recover()
			if v == nil {
				return
			}

			// ErrAbortHandler is the documented way to abort a response;
			// net/http handles it.
			if err, ok := v.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				panic(v)
			}

			logger.FromContext(r.Context()).ErrorContext(r.Context(), "handler panicked",
				slog.String(logger.KeyPanic, fmt.Sprint(v)),
				slog.String("stack", string(debug.Stack())),
			)
			writeErrorModel(w, r, http.StatusInternalServerError, typeInternalServerError, "internal server error")
		}()

		next.ServeHTTP(w, r)
	})
}

// isProbe reports a request from monitoring, logged at debug level.
func isProbe(r *http.Request) bool {
	return r.URL.Path == pathHealthz || r.URL.Path == pathReadyz || r.URL.Path == pathMetrics
}

func validRequestID(id string) bool {
	if id == "" || len(id) > maxRequestIDLen {
		return false
	}

	for _, c := range []byte(id) {
		if c < '!' || c > '~' {
			return false
		}
	}

	return true
}

func newRequestID() string {
	var b [16]byte
	// crypto/rand.Read never returns an error; it crashes the program if the
	// system random source fails.
	_, _ = rand.Read(b[:])

	return hex.EncodeToString(b[:])
}

// responseRecorder captures the status code and body size of a response.
type responseRecorder struct {
	http.ResponseWriter

	status      int
	bytes       int64
	wroteHeader bool
}

func (rec *responseRecorder) WriteHeader(status int) {
	if !rec.wroteHeader {
		rec.status = status
		rec.wroteHeader = true
	}

	rec.ResponseWriter.WriteHeader(status)
}

func (rec *responseRecorder) Write(b []byte) (int, error) {
	rec.wroteHeader = true
	n, err := rec.ResponseWriter.Write(b)
	rec.bytes += int64(n)

	//nolint:wrapcheck // http.ResponseWriter errors are passed through unchanged.
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (rec *responseRecorder) Unwrap() http.ResponseWriter {
	return rec.ResponseWriter
}
