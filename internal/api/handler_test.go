package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"echpochmak/internal/api"
	"echpochmak/internal/api/gen"
	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
)

func newTestHandler(t *testing.T, s store.Store) (http.Handler, *bytes.Buffer) {
	t.Helper()

	var logs bytes.Buffer

	h := api.NewHandler(api.Options{
		Logger: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Store:  s,
		Server: api.NewServer(),
	})

	return h, &logs
}

func serve(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	return rec
}

func decodeError(t *testing.T, rec *httptest.ResponseRecorder) gen.ErrorModel {
	t.Helper()

	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}

	var resp gen.IcebergErrorResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("body is not an Iceberg error response: %v\n%s", err, rec.Body.String())
	}

	return resp.Error
}

func TestUnimplementedOperations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t, memstore.New())

	tests := []struct {
		method, target, body string
	}{
		{method: http.MethodGet, target: "/v1/config"},
		{method: http.MethodGet, target: "/v1/main/namespaces"},
		{method: http.MethodPost, target: "/v1/main/namespaces", body: `{"namespace":["db"]}`},
		{method: http.MethodGet, target: "/v1/main/namespaces/db%1Fsub/tables/t"},
		{method: http.MethodPost, target: "/v1/main/transactions/commit", body: `{"table-changes":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.target, func(t *testing.T) {
			t.Parallel()

			rec := serve(h, tt.method, tt.target, tt.body)
			if rec.Code != http.StatusNotImplemented {
				t.Fatalf("status = %d, want 501; body %s", rec.Code, rec.Body.String())
			}

			e := decodeError(t, rec)
			if e.Code != http.StatusNotImplemented || e.Type != "UnsupportedOperationException" {
				t.Fatalf("error = %+v", e)
			}
		})
	}
}

func TestMalformedBodyIsBadRequest(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t, memstore.New())

	rec := serve(h, http.MethodPost, "/v1/main/namespaces", `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}

	if e := decodeError(t, rec); e.Type != "BadRequestException" {
		t.Fatalf("error type = %q", e.Type)
	}
}

func TestUnknownRoute(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t, memstore.New())

	rec := serve(h, http.MethodGet, "/nope", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}

	decodeError(t, rec)
}

func TestProbes(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t, memstore.New())

	for _, path := range []string{"/healthz", "/readyz"} {
		if rec := serve(h, http.MethodGet, path, ""); rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, rec.Code)
		}
	}
}

type failingStore struct {
	store.Store
}

func (failingStore) Ping(context.Context) error {
	return store.ErrUnavailable
}

func TestReadyzReportsStoreFailure(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t, failingStore{Store: memstore.New()})

	if rec := serve(h, http.MethodGet, "/readyz", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d, want 503", rec.Code)
	}

	if rec := serve(h, http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /healthz = %d, want 200 even when the store is down", rec.Code)
	}
}

func TestRequestID(t *testing.T) {
	t.Parallel()

	h, logs := newTestHandler(t, memstore.New())

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/v1/config", nil)
	req.Header.Set(api.HeaderRequestID, "trace-123")

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(api.HeaderRequestID); got != "trace-123" {
		t.Fatalf("response request ID = %q, want the incoming one", got)
	}

	var line map[string]any
	if err := json.Unmarshal(logs.Bytes(), &line); err != nil {
		t.Fatalf("access log is not one JSON line: %v\n%s", err, logs.String())
	}

	if line["request_id"] != "trace-123" || line["route"] != "GET /v1/config" || line["status"] != float64(501) {
		t.Fatalf("access log = %v", line)
	}

	generated := serve(h, http.MethodGet, "/healthz", "").Header().Get(api.HeaderRequestID)
	if len(generated) != 32 {
		t.Fatalf("generated request ID = %q, want 32 hex characters", generated)
	}
}

func TestErrorMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err      error
		status   int
		typeName string
	}{
		{err: store.ErrNamespaceNotFound, status: 404, typeName: "NoSuchNamespaceException"},
		{err: store.ErrTableNotFound, status: 404, typeName: "NoSuchTableException"},
		{err: store.ErrViewNotFound, status: 404, typeName: "NoSuchViewException"},
		{err: store.ErrNamespaceAlreadyExists, status: 409, typeName: "AlreadyExistsException"},
		{err: store.ErrTableAlreadyExists, status: 409, typeName: "AlreadyExistsException"},
		{err: store.ErrNamespaceNotEmpty, status: 409, typeName: "NamespaceNotEmptyException"},
		{err: &store.CommitConflictError{}, status: 409, typeName: "CommitFailedException"},
		{err: store.ErrUnavailable, status: 503, typeName: "ServiceUnavailableException"},
		{err: store.ErrInvalidArgument, status: 400, typeName: "BadRequestException"},
		{err: errors.New("boom"), status: 500, typeName: "InternalServerError"},
	}

	for _, tt := range tests {
		t.Run(tt.typeName+"/"+tt.err.Error(), func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
			api.WriteError(rec, req, tt.err)

			e := decodeError(t, rec)
			if rec.Code != tt.status || e.Code != tt.status || e.Type != tt.typeName {
				t.Fatalf("got %d %+v, want %d %s", rec.Code, e, tt.status, tt.typeName)
			}

			if tt.status == http.StatusInternalServerError && strings.Contains(e.Message, "boom") {
				t.Fatalf("internal error details leaked to the client: %q", e.Message)
			}
		})
	}
}
