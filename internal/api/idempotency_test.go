package api_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/idempotency"
	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
)

// replicas returns two catalog handlers that share a catalog store and an
// idempotency store, like two replicas behind a load balancer.
func replicas(t *testing.T) (*client, *client) {
	t.Helper()

	st := memstore.New()
	records := idempotency.NewMemoryStore()
	files := fileio.NewIceberg(context.Background(), nil)
	warehouse := "mem://api-test/" + strings.ReplaceAll(t.Name(), "/", "_")

	newReplica := func(s store.Store) *client {
		log := slog.New(slog.DiscardHandler)
		svc := catalog.New(catalog.Options{Store: s, Files: files, Warehouse: warehouse, Logger: log})
		guard := idempotency.NewGuard(records, idempotency.Options{Lifetime: 30 * time.Minute, Logger: log})

		return &client{t: t, h: api.NewHandler(api.Options{
			Logger:      log,
			Ready:       svc,
			Server:      api.NewServer(svc, "main", api.WithIdempotencyLifetime(guard.Lifetime())),
			Idempotency: guard,
		})}
	}

	return newReplica(st), newReplica(st)
}

const (
	keyA = "017f22e2-79b0-7cc3-98c4-dc0c0c07398f"
	keyB = "017f22e2-79b0-7cc3-98c4-dc0c0c073990"
	keyC = "017f22e2-79b0-7cc3-98c4-dc0c0c073991"
	keyD = "017f22e2-79b0-7cc3-98c4-dc0c0c073992"
)

func withKey(c *client, key, method, target, body string) *httptest.ResponseRecorder {
	c.t.Helper()

	return c.do(method, target, body, api.HeaderIdempotencyKey, key)
}

func mustStatus(t *testing.T, rec *httptest.ResponseRecorder, status int, replayed bool) {
	t.Helper()

	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body: %s", rec.Code, status, rec.Body.String())
	}

	if got := rec.Header().Get(api.HeaderIdempotentReplay) == "true"; got != replayed {
		t.Fatalf("replayed = %v, want %v", got, replayed)
	}
}

func TestIdempotentCreateNamespaceAcrossReplicas(t *testing.T) {
	t.Parallel()

	a, b := replicas(t)
	body := `{"namespace":["db"],"properties":{"owner":"x"}}`

	first := withKey(a, keyA, http.MethodPost, "/v1/main/namespaces", body)
	mustStatus(t, first, http.StatusOK, false)

	// The retry reaches the other replica: replayed, not a 409.
	second := withKey(b, keyA, http.MethodPost, "/v1/main/namespaces", body)
	mustStatus(t, second, http.StatusOK, true)

	if second.Body.String() != first.Body.String() {
		t.Fatalf("replayed body %s, want %s", second.Body.String(), first.Body.String())
	}

	// Without the key, the same request is a new operation and conflicts.
	b.expectError(http.StatusConflict, "AlreadyExistsException", http.MethodPost, "/v1/main/namespaces", body)
}

func TestIdempotentCommitIsAppliedOnce(t *testing.T) {
	t.Parallel()

	a, b := replicas(t)
	a.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	var created tableResult

	a.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, &created)

	commit := `{"requirements":[{"type":"assert-table-uuid","uuid":"` + created.Metadata.TableUUID + `"}],
		"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`

	first := withKey(a, keyB, http.MethodPost, "/v1/main/namespaces/db/tables/t", commit)
	mustStatus(t, first, http.StatusOK, false)

	var committed tableResult
	if err := json.Unmarshal(first.Body.Bytes(), &committed); err != nil {
		t.Fatal(err)
	}

	second := withKey(b, keyB, http.MethodPost, "/v1/main/namespaces/db/tables/t", commit)
	mustStatus(t, second, http.StatusOK, true)

	var replayed tableResult
	if err := json.Unmarshal(second.Body.Bytes(), &replayed); err != nil {
		t.Fatal(err)
	}

	// Applied once: the replay shows the same metadata file, not a newer one.
	if replayed.MetadataLocation != committed.MetadataLocation {
		t.Fatalf("replay at %q, want the committed %q", replayed.MetadataLocation, committed.MetadataLocation)
	}
}

func TestIdempotentDropReplaysSuccess(t *testing.T) {
	t.Parallel()

	a, b := replicas(t)
	a.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	mustStatus(t, withKey(a, keyC, http.MethodDelete, "/v1/main/namespaces/db", ""), http.StatusNoContent, false)
	// Executed again, the drop would be a 404; replayed, it is the 204.
	mustStatus(t, withKey(b, keyC, http.MethodDelete, "/v1/main/namespaces/db", ""), http.StatusNoContent, true)
}

func TestIdempotencyKeyErrors(t *testing.T) {
	t.Parallel()

	a, _ := replicas(t)

	mustStatus(t, withKey(a, "not-a-uuid", http.MethodPost, "/v1/main/namespaces", `{"namespace":["x"]}`),
		http.StatusBadRequest, false)

	mustStatus(t, withKey(a, keyD, http.MethodPost, "/v1/main/namespaces", `{"namespace":["x"]}`),
		http.StatusOK, false)
	// The same key with another body is another operation: refused.
	mustStatus(t, withKey(a, keyD, http.MethodPost, "/v1/main/namespaces", `{"namespace":["y"]}`),
		http.StatusUnprocessableEntity, false)

	// Reads ignore the key.
	mustStatus(t, withKey(a, keyD, http.MethodGet, "/v1/main/namespaces/x", ""), http.StatusOK, false)
}

func TestConfigAdvertisesIdempotency(t *testing.T) {
	t.Parallel()

	a, _ := replicas(t)

	var cfg struct {
		Lifetime string `json:"idempotency-key-lifetime"`
	}

	a.expect(http.StatusOK, http.MethodGet, "/v1/config", "", &cfg)

	if cfg.Lifetime != "PT30M" {
		t.Fatalf("idempotency-key-lifetime = %q, want PT30M", cfg.Lifetime)
	}

	plain := newClient(t)

	var none struct {
		Lifetime *string `json:"idempotency-key-lifetime"`
	}

	plain.expect(http.StatusOK, http.MethodGet, "/v1/config", "", &none)

	if none.Lifetime != nil {
		t.Fatalf("a server without idempotency advertises %q", *none.Lifetime)
	}
}
