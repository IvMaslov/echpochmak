package idempotency_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"echpochmak/internal/idempotency"
	"echpochmak/internal/kv/kvtest"
)

// forEachStore runs fn against both store implementations.
func forEachStore(t *testing.T, fn func(t *testing.T, newStore func() idempotency.Store)) {
	t.Helper()

	t.Run("memory", func(t *testing.T) {
		t.Parallel()
		fn(t, func() idempotency.Store { return idempotency.NewMemoryStore() })
	})

	t.Run("kv", func(t *testing.T) {
		t.Parallel()

		cl := kvtest.NewGRPCCluster(t, 3).Client()

		var n atomic.Int64

		fn(t, func() idempotency.Store {
			return idempotency.NewKVStore(cl, fmt.Sprintf("test/%d/", n.Add(1)))
		})
	})
}

func TestStoreContract(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_800_000_000, 0)

	forEachStore(t, func(t *testing.T, newStore func() idempotency.Store) {
		t.Helper()

		t.Run("create once", func(t *testing.T) {
			t.Parallel()

			ctx, s := t.Context(), newStore()
			rec := idempotency.Record{Fingerprint: "a", State: idempotency.InProgress, ExpiresAt: base.Add(time.Hour)}

			first, created, err := s.Create(ctx, "k", rec)
			if err != nil || !created {
				t.Fatalf("first Create = %v, %v", created, err)
			}

			got, created, err := s.Create(ctx, "k", idempotency.Record{Fingerprint: "b"})
			if err != nil || created || got.Fingerprint != "a" || got.Version != first.Version {
				t.Fatalf("second Create = %+v, %v, %v; want the first record", got, created, err)
			}
		})

		t.Run("compare and swap", func(t *testing.T) {
			t.Parallel()

			ctx, s := t.Context(), newStore()
			rec := idempotency.Record{Fingerprint: "a", ExpiresAt: base.Add(time.Hour)}

			stored, _, err := s.Create(ctx, "k", rec)
			if err != nil {
				t.Fatal(err)
			}

			rec.State = idempotency.Final

			next, ok, err := s.Replace(ctx, "k", stored.Version, rec)
			if err != nil || !ok || next.Version == stored.Version {
				t.Fatalf("Replace at the current version = %+v, %v, %v", next, ok, err)
			}

			if _, ok, _ := s.Replace(ctx, "k", stored.Version, rec); ok {
				t.Fatal("Replace at a stale version succeeded")
			}

			if ok, _ := s.Delete(ctx, "k", stored.Version); ok {
				t.Fatal("Delete at a stale version succeeded")
			}

			if ok, err := s.Delete(ctx, "k", next.Version); err != nil || !ok {
				t.Fatalf("Delete at the current version = %v, %v", ok, err)
			}

			if _, created, _ := s.Create(ctx, "k", rec); !created {
				t.Fatal("Create after Delete did not create")
			}
		})

		t.Run("expire only expired", func(t *testing.T) {
			t.Parallel()

			ctx, s := t.Context(), newStore()

			for i, exp := range []time.Duration{-2 * time.Minute, -time.Minute, time.Minute} {
				rec := idempotency.Record{Fingerprint: "a", ExpiresAt: base.Add(exp)}
				if _, _, err := s.Create(ctx, fmt.Sprint("k", i), rec); err != nil {
					t.Fatal(err)
				}
			}

			n, err := s.Expire(ctx, base, 10)
			if err != nil || n != 2 {
				t.Fatalf("Expire = %d, %v; want 2", n, err)
			}

			if _, created, _ := s.Create(ctx, "k2", idempotency.Record{}); created {
				t.Fatal("the unexpired record was deleted")
			}

			if _, created, _ := s.Create(ctx, "k0", idempotency.Record{}); !created {
				t.Fatal("an expired record survived")
			}
		})
	})
}
