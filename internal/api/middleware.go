package api

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"echpochmak/internal/logger"
)

// HeaderRequestID carries the request ID. A valid incoming value is kept, so
// a request can be traced across services; otherwise a new one is generated.
const HeaderRequestID = "X-Request-ID"

const maxRequestIDLen = 128

// withRequestLogging assigns a request ID, stores a logger tagged with it in
// the request context, and writes one access log line per request.
func withRequestLogging(base *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		id := r.Header.Get(HeaderRequestID)
		if !validRequestID(id) {
			id = newRequestID()
		}

		w.Header().Set(HeaderRequestID, id)

		log := base.With(slog.String(logger.KeyRequestID, id))
		r = r.WithContext(logger.WithContext(r.Context(), log))
		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}

		next.ServeHTTP(rec, r)

		// The mux sets r.Pattern on the request it routed, which is r.
		route := r.Pattern
		if route == "" {
			route = "unmatched"
		}

		level := slog.LevelInfo
		if isProbe(r) {
			level = slog.LevelDebug
		}

		log.LogAttrs(r.Context(), level, "http request",
			slog.String(logger.KeyMethod, r.Method),
			slog.String(logger.KeyRoute, route),
			slog.String(logger.KeyPath, r.URL.Path),
			slog.Int(logger.KeyStatus, rec.status),
			slog.Duration(logger.KeyDuration, time.Since(start)),
			slog.Int64(logger.KeyBytes, rec.bytes),
			slog.String(logger.KeyRemote, r.RemoteAddr),
		)
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

func isProbe(r *http.Request) bool {
	return r.URL.Path == pathHealthz || r.URL.Path == pathReadyz
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
