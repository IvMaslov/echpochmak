package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRecoveryTurnsPanicInto500(t *testing.T) {
	t.Parallel()

	h := withRecovery(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

func TestRecoveryRepanicsOnAbort(t *testing.T) {
	t.Parallel()

	h := withRecovery(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))

	defer func() {
		if err, _ := recover().(error); !errors.Is(err, http.ErrAbortHandler) {
			t.Fatal("ErrAbortHandler was swallowed")
		}
	}()

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/", nil))
}

func TestValidRequestID(t *testing.T) {
	t.Parallel()

	tests := map[string]bool{
		"abc-123":                 true,
		"":                        false,
		"has space":               false,
		"new\nline":               false,
		string(make([]byte, 200)): false,
	}

	for id, want := range tests {
		if got := validRequestID(id); got != want {
			t.Errorf("validRequestID(%q) = %v, want %v", id, got, want)
		}
	}
}
