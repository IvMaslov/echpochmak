package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// beginWrite records a commit intent for the metadata files at locations,
// which the caller is about to write for the entries ids. The returned
// reference goes into the commits, which remove the intent when they land.
// An intent that outlives its attempt names orphans, and the janitor deletes
// them; see arch/phase5/plan.md (D5).
func (s *Service) beginWrite(ctx context.Context, ids []store.Ident, locations []string) (store.IntentRef, error) {
	intent, err := s.store.CreateIntent(ctx, store.Intent{
		Locations: locations,
		Idents:    ids,
		Owner:     s.owner,
	})
	if err != nil {
		// Nothing was written yet, whatever happened to the intent: an
		// intent without files is harmless, and the janitor removes it.
		return store.IntentRef{}, fmt.Errorf("recording the commit intent: %w", err)
	}

	return intent.Ref(), nil
}

// abandonWrite deletes the files of an attempt that certainly did not
// commit, then releases its intent. If a file cannot be deleted, the intent
// stays, so the janitor deletes the file later.
func (s *Service) abandonWrite(ctx context.Context, intent store.IntentRef, locations []string, reason string) {
	ctx = context.WithoutCancel(ctx)
	deleted := true

	for _, location := range locations {
		deleted = s.deleteFile(ctx, location, reason) && deleted
	}

	if !deleted || intent.IsZero() {
		return
	}

	err := s.store.ReleaseIntent(ctx, intent)
	if err != nil && !errors.Is(err, store.ErrIntentChanged) {
		// The janitor finds the intent, and its files are already gone.
		s.logFor(ctx).WarnContext(ctx, "releasing a commit intent failed",
			slog.String("intent", intent.ID),
			logger.Err(err),
		)
	}
}

// writeAndCommit writes one metadata file at location for id under a commit
// intent, then runs commit with the intent. A failure of write or commit is
// classified, and cleaned up when safe, before it is returned.
func (s *Service) writeAndCommit(
	ctx context.Context,
	id store.Ident,
	location string,
	write func() error,
	commit func(intent store.IntentRef) error,
) error {
	locations := []string{location}

	intent, err := s.beginWrite(ctx, []store.Ident{id}, locations)
	if err != nil {
		return err
	}

	if err := write(); err != nil {
		s.abandonWrite(ctx, intent, locations, "writing the metadata file failed")

		return err
	}

	if err := commit(intent); err != nil {
		return s.handleFailedPointerWrite(ctx, err, intent, locations)
	}

	return nil
}
