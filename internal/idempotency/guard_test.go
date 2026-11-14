package idempotency_test

import (
	"log/slog"
	"net/http"
	"sync"
	"testing"
	"time"

	"echpochmak/internal/idempotency"
)

// clock is a settable time source.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

func newGuard(s idempotency.Store, c *clock) *idempotency.Guard {
	return idempotency.NewGuard(s, idempotency.Options{
		Lifetime: 30 * time.Minute,
		Lease:    time.Minute,
		Logger:   slog.New(slog.DiscardHandler),
		Now:      c.Now,
	})
}

func TestGuard(t *testing.T) {
	t.Parallel()

	forEachStore(t, func(t *testing.T, newStore func() idempotency.Store) {
		t.Helper()

		t.Run("execute then replay", func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			c := &clock{now: time.Unix(1_800_000_000, 0)}
			g := newGuard(newStore(), c)

			d, _, ticket, err := g.Begin(ctx, "key", "fp")
			if err != nil || d != idempotency.Execute {
				t.Fatalf("first Begin = %v, %v; want Execute", d, err)
			}

			g.Finish(ctx, ticket, idempotency.Response{Status: http.StatusOK, Body: []byte("done")})

			d, rec, _, err := g.Begin(ctx, "key", "fp")
			if err != nil || d != idempotency.Replay || string(rec.Response.Body) != "done" {
				t.Fatalf("retry Begin = %v, %+v, %v; want a replay of done", d, rec, err)
			}
		})

		t.Run("409 is final", func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			g := newGuard(newStore(), &clock{now: time.Unix(1_800_000_000, 0)})

			_, _, ticket, _ := g.Begin(ctx, "key", "fp")
			g.Finish(ctx, ticket, idempotency.Response{Status: http.StatusConflict})

			if d, _, _, _ := g.Begin(ctx, "key", "fp"); d != idempotency.Replay {
				t.Fatalf("retry after 409 = %v, want Replay", d)
			}
		})

		t.Run("5xx is not final", func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			g := newGuard(newStore(), &clock{now: time.Unix(1_800_000_000, 0)})

			_, _, ticket, _ := g.Begin(ctx, "key", "fp")
			g.Finish(ctx, ticket, idempotency.Response{Status: http.StatusServiceUnavailable})

			if d, _, _, _ := g.Begin(ctx, "key", "fp"); d != idempotency.Execute {
				t.Fatalf("retry after 503 = %v, want Execute", d)
			}
		})

		t.Run("reused key", func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			g := newGuard(newStore(), &clock{now: time.Unix(1_800_000_000, 0)})

			_, _, ticket, _ := g.Begin(ctx, "key", "fp")
			g.Finish(ctx, ticket, idempotency.Response{Status: http.StatusOK})

			if d, _, _, _ := g.Begin(ctx, "key", "other request"); d != idempotency.Mismatch {
				t.Fatalf("Begin with another fingerprint = %v, want Mismatch", d)
			}
		})

		t.Run("busy, then takeover after the lease", func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			c := &clock{now: time.Unix(1_800_000_000, 0)}
			s := newStore()
			replicaA, replicaB := newGuard(s, c), newGuard(s, c)

			if d, _, _, _ := replicaA.Begin(ctx, "key", "fp"); d != idempotency.Execute {
				t.Fatal("replica A did not get the claim")
			}

			if d, _, _, _ := replicaB.Begin(ctx, "key", "fp"); d != idempotency.Busy {
				t.Fatalf("replica B during A's lease = %v, want Busy", d)
			}

			// Replica A dies without finishing; its lease runs out.
			c.Advance(2 * time.Minute)

			d, _, ticket, err := replicaB.Begin(ctx, "key", "fp")
			if err != nil || d != idempotency.Execute {
				t.Fatalf("replica B after the lease = %v, %v; want Execute", d, err)
			}

			replicaB.Finish(ctx, ticket, idempotency.Response{Status: http.StatusNoContent})

			if d, _, _, _ := replicaA.Begin(ctx, "key", "fp"); d != idempotency.Replay {
				t.Fatalf("replica A afterwards = %v, want Replay", d)
			}
		})

		t.Run("expired record is forgotten", func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			c := &clock{now: time.Unix(1_800_000_000, 0)}
			g := newGuard(newStore(), c)

			_, _, ticket, _ := g.Begin(ctx, "key", "fp")
			g.Finish(ctx, ticket, idempotency.Response{Status: http.StatusOK})

			c.Advance(31 * time.Minute)

			if d, _, _, _ := g.Begin(ctx, "key", "another"); d != idempotency.Execute {
				t.Fatalf("Begin after the lifetime = %v, want Execute", d)
			}
		})
	})
}

func TestFinalizes(t *testing.T) {
	t.Parallel()

	for status, want := range map[int]bool{
		200: true, 204: true, 400: true, 404: true, 409: true,
		408: false, 429: false, 500: false, 503: false,
	} {
		if got := idempotency.Finalizes(status); got != want {
			t.Errorf("Finalizes(%d) = %v, want %v", status, got, want)
		}
	}
}
