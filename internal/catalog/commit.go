package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// requirementAssertCreate is the type of the requirement that a table must
// not exist yet.
const requirementAssertCreate = "assert-create"

// CommitInput is a set of changes to one table.
type CommitInput struct {
	Ident store.Ident
	// Requirements must hold for the current table state, or the commit
	// fails. An assert-create requirement makes the commit create the table.
	Requirements []table.Requirement
	Updates      []table.Update
}

// errRetry marks a commit attempt that lost a race with a concurrent change
// and may be retried.
var errRetry = errors.New("concurrent change")

// CommitTable applies changes to a table:
//
//  1. load the current pointer and metadata (none when creating),
//  2. check the requirements against them,
//  3. apply the updates,
//  4. write a new metadata file,
//  5. move the pointer with compare-and-swap.
//
// If the swap loses a race, the whole sequence is retried from step 1 up to
// the configured number of times. Retrying is safe because the requirements
// are checked again against the new state: a retry succeeds only if the
// client's assumptions still hold.
func (s *Service) CommitTable(ctx context.Context, in CommitInput) (Table, error) {
	if err := in.Ident.Validate(); err != nil {
		return Table{}, err
	}

	start := time.Now()

	for attempt := 0; ; attempt++ {
		t, err := s.commitOnce(ctx, in)
		if err == nil {
			s.logFor(ctx).InfoContext(ctx, "table committed",
				slog.String(logger.KeyTable, in.Ident.String()),
				slog.String(logger.KeyLocation, t.MetadataLocation),
				slog.Int("updates", len(in.Updates)),
				slog.Int(logger.KeyAttempt, attempt+1),
				slog.Duration(logger.KeyDuration, time.Since(start)),
			)

			return t, nil
		}

		if !errors.Is(err, errRetry) {
			return Table{}, err
		}

		if attempt >= s.retries {
			return Table{}, fmt.Errorf("%w: giving up after %d attempts: %w", store.ErrCommitConflict, attempt+1, err)
		}

		s.logFor(ctx).WarnContext(ctx, "commit conflict, retrying",
			slog.String(logger.KeyTable, in.Ident.String()),
			slog.Int(logger.KeyAttempt, attempt+1),
			logger.Err(err),
		)

		if err := sleep(ctx, s.backoff(attempt+1)); err != nil {
			return Table{}, err
		}
	}
}

func (s *Service) commitOnce(ctx context.Context, in CommitInput) (Table, error) {
	ptr, current, err := s.loadForCommit(ctx, in)
	if err != nil {
		return Table{}, err
	}

	if err := checkRequirements(in.Requirements, current); err != nil {
		return Table{}, err
	}

	if current != nil && len(in.Updates) == 0 {
		return s.LoadTable(ctx, in.Ident)
	}

	base := current
	if base == nil {
		base, err = emptyMetadata()
		if err != nil {
			return Table{}, err
		}
	}

	updated, err := table.UpdateTableMetadata(base, in.Updates, ptr.MetadataLocation)
	if err != nil {
		return Table{}, fmt.Errorf("%w: applying updates: %w", ErrInvalidRequest, err)
	}

	if updated.Location() == "" {
		return Table{}, fmt.Errorf("%w: the table has no location; add a set-location update", ErrInvalidRequest)
	}

	location, err := newMetadataLocation(updated, current, ptr.MetadataLocation)
	if err != nil {
		return Table{}, err
	}

	raw, err := s.writeMetadata(ctx, location, updated)
	if err != nil {
		return Table{}, err
	}

	_, err = s.store.CommitTables(ctx, []store.Commit{{
		Ident:            in.Ident,
		ExpectedVersion:  ptr.Version,
		MetadataLocation: location,
	}})
	if err != nil {
		return Table{}, s.handleFailedPointerWrite(ctx, location, err)
	}

	s.deleteExpiredMetadata(ctx, current, updated)

	return Table{Ident: in.Ident, MetadataLocation: location, MetadataJSON: raw}, nil
}

// loadForCommit returns the current pointer and metadata of the table. For a
// table that does not exist yet and a commit that creates it, both are zero.
func (s *Service) loadForCommit(ctx context.Context, in CommitInput) (store.Pointer, table.Metadata, error) {
	ptr, err := s.store.GetTable(ctx, in.Ident)

	switch {
	case err == nil:
		md, _, readErr := s.readMetadata(ctx, ptr.MetadataLocation)
		if readErr != nil {
			return store.Pointer{}, nil, readErr
		}

		return ptr, md, nil
	case errors.Is(err, store.ErrTableNotFound) && createsTable(in.Requirements):
		return store.Pointer{}, nil, nil
	default:
		return store.Pointer{}, nil, err
	}
}

// handleFailedPointerWrite classifies a failed pointer update after a new
// metadata file was written to location, and cleans up when that is safe.
func (s *Service) handleFailedPointerWrite(ctx context.Context, location string, err error) error {
	lostRace := errors.Is(err, store.ErrCommitConflict) ||
		errors.Is(err, store.ErrTableAlreadyExists) ||
		errors.Is(err, store.ErrTableNotFound)
	rejected := errors.Is(err, store.ErrViewAlreadyExists) ||
		errors.Is(err, store.ErrNamespaceNotFound) ||
		errors.Is(err, store.ErrInvalidArgument)

	switch {
	case lostRace:
		s.deleteFile(ctx, location, "commit lost a race")

		return fmt.Errorf("%w: %w", errRetry, err)
	case rejected:
		s.deleteFile(ctx, location, "commit rejected")

		return err
	default:
		// The pointer may or may not have moved: the new file must stay, and
		// the client must not clean up either.
		s.logFor(ctx).ErrorContext(ctx, "commit state unknown",
			slog.String(logger.KeyLocation, location),
			logger.Err(err),
		)

		return fmt.Errorf("%w: %w", ErrCommitStateUnknown, err)
	}
}

// deleteExpiredMetadata removes metadata files that dropped out of the
// metadata log with this commit, if the table asks for it.
func (s *Service) deleteExpiredMetadata(ctx context.Context, before, after table.Metadata) {
	enabled := after.Properties().GetBool(
		table.MetadataDeleteAfterCommitEnabledKey,
		table.MetadataDeleteAfterCommitEnabledDefault,
	)
	if !enabled {
		return
	}

	for _, location := range removedMetadataFiles(before, after) {
		s.deleteFile(ctx, location, "expired from the metadata log")
	}
}

// checkRequirements validates every requirement against current, which is
// nil when the table does not exist yet.
func checkRequirements(reqs []table.Requirement, current table.Metadata) error {
	for _, req := range reqs {
		// Only assert-create can be evaluated without metadata; any other
		// requirement on a missing table cannot hold.
		if current == nil && req.GetType() != requirementAssertCreate {
			return fmt.Errorf("%w: %s: the table does not exist", ErrRequirementFailed, req.GetType())
		}

		if err := req.Validate(current); err != nil {
			return fmt.Errorf("%w: %s: %w", ErrRequirementFailed, req.GetType(), err)
		}
	}

	return nil
}

func createsTable(reqs []table.Requirement) bool {
	for _, req := range reqs {
		if req.GetType() == requirementAssertCreate {
			return true
		}
	}

	return false
}

// emptyMetadata is the base that a create commit's updates are applied to.
func emptyMetadata() (table.Metadata, error) {
	md, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, "", nil)
	if err != nil {
		return nil, fmt.Errorf("building empty metadata: %w", err)
	}

	return md, nil
}
