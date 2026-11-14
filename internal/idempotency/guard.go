// Package idempotency implements the Idempotency-Key protocol of the Iceberg
// REST spec: a mutation retried with the same key has no additional effect,
// and once finalized, its response is replayed.
//
// Records live in a [Store] shared by every catalog replica, because a retry
// may reach a different replica than the first attempt. The protocol, per
// key:
//
//  1. Claim: create an "in progress" record with a lease. Only one attempt
//     can hold the claim.
//  2. Another attempt that finds a finalized record replays it; one that
//     finds a live claim is told to retry later; one that finds an expired
//     lease takes the claim over, because the replica holding it died.
//  3. Finish: a final response (2xx, or a 4xx other than 408 and 429) is
//     stored for replay; a 5xx releases the claim, so a retry runs again.
//
// Records expire after the advertised lifetime, counted from the first
// attempt, and a janitor deletes them.
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"
)

// Defaults for [Options].
const (
	DefaultLifetime = 30 * time.Minute
	takeoverTries   = 3
	janitorBatch    = 100
)

// State is the state of a record.
type State string

// Record states.
const (
	InProgress State = "in_progress"
	Final      State = "final"
)

// Record is what the store keeps for one key.
type Record struct {
	// Fingerprint identifies the request, so a key reused for a different
	// request is detected.
	Fingerprint string    `json:"fingerprint"`
	State       State     `json:"state"`
	LeaseUntil  time.Time `json:"lease_until"`
	ExpiresAt   time.Time `json:"expires_at"`
	Response    Response  `json:"response"`

	// Version is the store's version of the record, for compare-and-swap.
	// It is not stored.
	Version int64 `json:"-"`
}

// Response is a response to replay.
type Response struct {
	Status      int    `json:"status,omitempty"`
	ContentType string `json:"content_type,omitempty"`
	ETag        string `json:"etag,omitempty"`
	Body        []byte `json:"body,omitempty"`
	// ReplayPath, when set, replaces the stored body: the replay serves a GET
	// of this path instead. Responses that carry whole table metadata are too
	// large to store; the spec allows a replay to show a newer state.
	ReplayPath string `json:"replay_path,omitempty"`
	// ReplayMetadata, when set, replaces the stored body for an unregister:
	// the replay reads the table metadata file at this location again. The
	// table is gone, so there is nothing to GET.
	ReplayMetadata string `json:"replay_metadata,omitempty"`
}

// Finalizes reports whether a response with this status is final, and so
// stored and replayed: success and deterministic client errors. Server
// errors, timeouts and rate limiting are not.
func Finalizes(status int) bool {
	switch {
	case status >= http.StatusInternalServerError:
		return false
	case status == http.StatusRequestTimeout, status == http.StatusTooManyRequests:
		return false
	default:
		return status >= http.StatusOK
	}
}

// Store keeps records. Every write is a compare-and-swap on the record's
// version, so concurrent attempts on different replicas cannot both win.
type Store interface {
	// Create stores rec under key if there is no record yet. Otherwise it
	// returns the existing record and created=false.
	Create(ctx context.Context, key string, rec Record) (stored Record, created bool, err error)
	// Replace overwrites the record if it is still at version; ok=false
	// means another write came first.
	Replace(ctx context.Context, key string, version int64, rec Record) (stored Record, ok bool, err error)
	// Delete removes the record if it is still at version.
	Delete(ctx context.Context, key string, version int64) (ok bool, err error)
	// Expire deletes up to limit records that expired before now, and
	// returns how many it deleted.
	Expire(ctx context.Context, now time.Time, limit int) (int, error)
}

// Options configures a Guard.
type Options struct {
	// Lifetime is how long a key is honoured from its first use.
	Lifetime time.Duration
	// Lease is how long a claim stays valid. It must exceed the time one
	// request can take, or a slow attempt loses its claim to a retry.
	Lease  time.Duration
	Logger *slog.Logger
	// Now returns the time; tests replace it.
	Now func() time.Time
}

// Guard runs the protocol for one catalog replica.
type Guard struct {
	store    Store
	lifetime time.Duration
	lease    time.Duration
	log      *slog.Logger
	now      func() time.Time
}

// NewGuard returns a Guard on store.
func NewGuard(store Store, opts Options) *Guard {
	if opts.Lifetime <= 0 {
		opts.Lifetime = DefaultLifetime
	}

	if opts.Lease <= 0 {
		opts.Lease = 2 * time.Minute
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Guard{store: store, lifetime: opts.Lifetime, lease: opts.Lease, log: opts.Logger, now: opts.Now}
}

// Lifetime returns how long a key is honoured.
func (g *Guard) Lifetime() time.Duration {
	return g.lifetime
}

// Decision tells the caller what to do with a request.
type Decision int

// Decisions of [Guard.Begin].
const (
	// Execute: run the request, then call [Guard.Finish] with the ticket.
	Execute Decision = iota
	// Replay: answer with the record's response.
	Replay
	// Mismatch: the key was used for a different request.
	Mismatch
	// Busy: another attempt with this key is running; retry later.
	Busy
)

// Ticket is a held claim.
type Ticket struct {
	key   string
	claim Record
}

// Begin starts a request carrying key, whose fingerprint is fp.
func (g *Guard) Begin(ctx context.Context, key, fp string) (Decision, *Record, *Ticket, error) {
	now := g.now()
	claim := Record{
		Fingerprint: fp,
		State:       InProgress,
		LeaseUntil:  now.Add(g.lease),
		ExpiresAt:   now.Add(g.lifetime),
	}

	for range takeoverTries {
		stored, created, err := g.store.Create(ctx, key, claim)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("claiming idempotency key: %w", err)
		}

		if created {
			return Execute, nil, &Ticket{key: key, claim: stored}, nil
		}

		existing := stored

		switch {
		case !now.Before(existing.ExpiresAt):
			// Expired but not yet collected: as if it did not exist.
			if _, err := g.store.Delete(ctx, key, existing.Version); err != nil {
				return 0, nil, nil, fmt.Errorf("removing an expired idempotency record: %w", err)
			}

			continue
		case existing.Fingerprint != fp:
			return Mismatch, &existing, nil, nil
		case existing.State == Final:
			return Replay, &existing, nil, nil
		case now.Before(existing.LeaseUntil):
			return Busy, &existing, nil, nil
		}

		// The lease expired: the replica holding it died. Take over, keeping
		// the lifetime of the first attempt.
		claim.ExpiresAt = existing.ExpiresAt

		taken, ok, err := g.store.Replace(ctx, key, existing.Version, claim)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("taking over an idempotency key: %w", err)
		}

		if ok {
			g.log.WarnContext(ctx, "took over an expired idempotency claim", slog.String("key", key))

			return Execute, nil, &Ticket{key: key, claim: taken}, nil
		}
	}

	// Lost every race for the claim: another attempt holds it.
	return Busy, nil, nil, nil
}

// Finish ends an executed request: it stores a final response for replay,
// or releases the claim after a server error. Failures are logged, not
// returned: the response itself is already decided.
func (g *Guard) Finish(ctx context.Context, t *Ticket, resp Response) {
	ctx = context.WithoutCancel(ctx)

	if !Finalizes(resp.Status) {
		if _, err := g.store.Delete(ctx, t.key, t.claim.Version); err != nil {
			g.log.WarnContext(ctx, "releasing an idempotency claim failed", slog.Any("error", err))
		}

		return
	}

	final := t.claim
	final.State = Final
	final.Response = resp

	if _, ok, err := g.store.Replace(ctx, t.key, t.claim.Version, final); err != nil || !ok {
		g.log.WarnContext(ctx, "finalizing an idempotency record failed",
			slog.String("key", t.key), slog.Bool("lost_claim", !ok), slog.Any("error", err))
	}
}

// RunJanitor deletes expired records every interval, with jitter, until ctx
// is done. Several replicas running it at once is harmless.
func (g *Guard) RunJanitor(ctx context.Context, interval time.Duration) {
	for {
		//nolint:gosec // G404: jitter needs no cryptographic randomness.
		wait := interval/2 + time.Duration(rand.Int64N(int64(interval)))

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		if err := g.expire(ctx); err != nil && !errors.Is(err, context.Canceled) {
			g.log.WarnContext(ctx, "expiring idempotency records failed", slog.Any("error", err))
		}
	}
}

// expire deletes every expired record, in batches.
func (g *Guard) expire(ctx context.Context) error {
	for {
		n, err := g.store.Expire(ctx, g.now(), janitorBatch)
		if err != nil {
			return err
		}

		if n < janitorBatch {
			return nil
		}
	}
}
