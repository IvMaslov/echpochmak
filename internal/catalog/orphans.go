package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"echpochmak/internal/fileio"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// Defaults for [JanitorOptions].
const (
	DefaultOrphanInterval = time.Minute
	DefaultOrphanGrace    = 2 * time.Minute

	// orphanListLimit is how many intents one page of a sweep looks at.
	orphanListLimit = 64
)

// JanitorOptions configures a [Janitor].
type JanitorOptions struct {
	Store store.Store
	// Files deletes orphaned metadata files.
	Files fileio.FileIO
	// Grace is how old an intent must be before it is condemned. It only
	// keeps the janitor from condemning commits that are slow, not
	// failed: safety does not depend on it.
	Grace time.Duration
	// Interval is the pause between sweeps, jittered.
	Interval time.Duration
	Logger   *slog.Logger
	// Now returns the current time; tests replace it.
	Now func() time.Time
	// OnSweep, if set, is called after each sweep with its result; metrics
	// use it.
	OnSweep func(SweepResult)
}

// SweepResult describes one sweep of the intents.
type SweepResult struct {
	// Pending is the number of intents seen, old or young.
	Pending int
	// Oldest is the creation time of the oldest intent seen; zero if none.
	Oldest time.Time
	// Cleaned is the number of intents whose files were deleted and which
	// were removed.
	Cleaned int
	// FilesDeleted is the number of files deleted.
	FilesDeleted int
	// Failed is the number of intents left for a later sweep after an error.
	Failed int
}

// Janitor deletes the metadata files of commit attempts that never landed.
// Such attempts leave their commit intent behind; a commit that landed removed
// its intent atomically with the pointer swap. Every replica runs one.
//
// For each intent older than the grace period, the janitor:
//
//  1. condemns it, which changes its version, so a commit still holding it
//     can no longer succeed;
//  2. deletes its files: a missing file counts as deleted;
//  3. removes it.
//
// A janitor that stops after step 1 leaves a condemned intent, which the next
// sweep finishes. See arch/phase5/plan.md (D5).
type Janitor struct {
	opts JanitorOptions
}

// NewJanitor returns a Janitor. Zero options take their defaults.
func NewJanitor(opts JanitorOptions) *Janitor {
	if opts.Grace <= 0 {
		opts.Grace = DefaultOrphanGrace
	}

	if opts.Interval <= 0 {
		opts.Interval = DefaultOrphanInterval
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Janitor{opts: opts}
}

// Run sweeps until ctx is done.
func (j *Janitor) Run(ctx context.Context) {
	for {
		j.Sweep(ctx)

		t := time.NewTimer(jitter(j.opts.Interval))

		select {
		case <-ctx.Done():
			t.Stop()

			return
		case <-t.C:
		}
	}
}

// Sweep cleans up every intent older than the grace period, and every
// condemned one, page by page, and returns what it did. It stops at the first
// page with an error, so a failing object store is not hammered.
func (j *Janitor) Sweep(ctx context.Context) SweepResult {
	var res SweepResult

	defer func() {
		if j.opts.OnSweep != nil {
			j.opts.OnSweep(res)
		}
	}()

	for {
		intents, err := j.opts.Store.ListIntents(ctx, orphanListLimit)
		if err != nil {
			if ctx.Err() == nil {
				j.opts.Logger.WarnContext(ctx, "orphans: listing commit intents failed", logger.Err(err))
			}

			return res
		}

		done := j.sweepPage(ctx, intents, &res)
		if done || len(intents) < orphanListLimit {
			return res
		}
	}
}

// sweepPage cleans up the intents of one page, oldest first, and reports
// whether the sweep should stop: it met a young intent, which means every
// later one is young too, or an error.
func (j *Janitor) sweepPage(ctx context.Context, intents []store.Intent, res *SweepResult) bool {
	cutoff := j.opts.Now().Add(-j.opts.Grace)

	for i, intent := range intents {
		res.Pending++
		if res.Oldest.IsZero() || intent.Created.Before(res.Oldest) {
			res.Oldest = intent.Created
		}

		if !intent.Condemned && intent.Created.After(cutoff) {
			// Intents are listed oldest first, so the rest are young too.
			res.Pending += len(intents) - i - 1

			return true
		}

		deleted, err := j.clean(ctx, intent)
		res.FilesDeleted += deleted

		if err != nil {
			res.Failed++

			if ctx.Err() == nil {
				j.opts.Logger.WarnContext(ctx, "orphans: cleaning a commit intent failed; retrying later",
					slog.String("intent", intent.ID),
					logger.Err(err),
				)
			}

			return true
		}

		res.Cleaned++
	}

	return false
}

// clean condemns an intent, deletes its files and removes it. It returns the
// number of files deleted. An intent that moved meanwhile was released by its
// commit or cleaned by another janitor, which is not an error.
func (j *Janitor) clean(ctx context.Context, intent store.Intent) (int, error) {
	if !intent.Condemned {
		condemned, err := j.opts.Store.CondemnIntent(ctx, intent.Ref())
		if errors.Is(err, store.ErrIntentChanged) {
			return 0, nil
		}

		if err != nil {
			return 0, fmt.Errorf("condemning: %w", err)
		}

		intent = condemned
	}

	deleted := 0

	for _, location := range intent.Locations {
		if err := j.opts.Files.Delete(ctx, location); err != nil {
			return deleted, fmt.Errorf("deleting %q: %w", location, err)
		}

		deleted++
	}

	err := j.opts.Store.ReleaseIntent(ctx, intent.Ref())
	if err != nil && !errors.Is(err, store.ErrIntentChanged) {
		return deleted, fmt.Errorf("removing: %w", err)
	}

	j.opts.Logger.InfoContext(ctx, "orphans: deleted the files of a commit that never landed",
		slog.String("intent", intent.ID),
		slog.Any("locations", intent.Locations),
		slog.String("owner", intent.Owner),
		slog.Time("created", intent.Created),
	)

	return deleted, nil
}
