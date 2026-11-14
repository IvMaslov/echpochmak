package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
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
	entity := slog.String(logger.KeyTable, in.Ident.String())

	t, attempts, err := retryOnRace(ctx, s, entity, func() (Table, error) {
		return s.commitOnce(ctx, in)
	})
	if err != nil {
		return Table{}, tableNameClash(err, in.Ident)
	}

	s.logFor(ctx).InfoContext(ctx, "table committed",
		entity,
		slog.String(logger.KeyLocation, t.MetadataLocation),
		slog.Int("updates", len(in.Updates)),
		slog.Int(logger.KeyAttempt, attempts),
		slog.Duration(logger.KeyDuration, time.Since(start)),
	)

	return t, nil
}

// retryOnRace runs once until it succeeds, fails with an error other than
// errRetry, or has been retried the configured number of times. It returns
// the number of attempts made. entity names what is committed, for logs.
func retryOnRace[T any](ctx context.Context, s *Service, entity slog.Attr, once func() (T, error)) (T, int, error) {
	var zero T

	for attempt := 0; ; attempt++ {
		v, err := once()
		if err == nil {
			return v, attempt + 1, nil
		}

		if !errors.Is(err, errRetry) {
			return zero, attempt + 1, err
		}

		if attempt >= s.retries {
			return zero, attempt + 1, fmt.Errorf(
				"%w: giving up after %d attempts: %w",
				store.ErrCommitConflict,
				attempt+1,
				err,
			)
		}

		s.logFor(ctx).WarnContext(ctx, "commit conflict, retrying",
			entity,
			slog.Int(logger.KeyAttempt, attempt+1),
			logger.Err(err),
		)

		if err := sleep(ctx, s.backoff(attempt+1)); err != nil {
			return zero, attempt + 1, err
		}
	}
}

func (s *Service) commitOnce(ctx context.Context, in CommitInput) (Table, error) {
	ptr, current, err := s.loadForCommit(ctx, in)
	if err != nil {
		return Table{}, err
	}

	plan, err := planCommit(in, ptr, current)
	if err != nil {
		return Table{}, err
	}

	if plan == nil {
		return s.LoadTable(ctx, in.Ident)
	}

	raw, err := s.writeMetadata(ctx, plan.location, plan.updated)
	if err != nil {
		return Table{}, err
	}

	if _, err := s.store.CommitTables(ctx, []store.Commit{plan.commit()}); err != nil {
		return Table{}, s.handleFailedPointerWrite(ctx, err, plan.location)
	}

	s.deleteExpiredMetadata(ctx, current, plan.updated)

	return Table{Ident: in.Ident, MetadataLocation: plan.location, MetadataJSON: raw}, nil
}

// plannedCommit is a commit whose requirements hold and whose updates are
// applied, ready for its metadata file to be written and its pointer swapped.
type plannedCommit struct {
	ident    store.Ident
	ptr      store.Pointer
	current  table.Metadata
	updated  table.Metadata
	location string
}

func (p *plannedCommit) commit() store.Commit {
	return store.Commit{Ident: p.ident, ExpectedVersion: p.ptr.Version, MetadataLocation: p.location}
}

// planCommit checks the requirements of in against current, the table's
// metadata (nil when it does not exist yet, ptr then being zero), and applies
// the updates. It returns nil if there is nothing to write: the table exists
// and no updates were requested.
func planCommit(in CommitInput, ptr store.Pointer, current table.Metadata) (*plannedCommit, error) {
	if err := checkRequirements(in.Requirements, current); err != nil {
		return nil, err
	}

	if current != nil && len(in.Updates) == 0 {
		return nil, nil //nolint:nilnil // nil means "nothing to commit"; see the doc comment.
	}

	base := current
	if base == nil {
		var err error

		base, err = emptyMetadata(in.Updates)
		if err != nil {
			return nil, err
		}
	}

	updated, err := table.UpdateTableMetadata(base, in.Updates, ptr.MetadataLocation)
	if err != nil {
		return nil, fmt.Errorf("%w: applying updates: %w", ErrInvalidRequest, err)
	}

	if updated.Location() == "" {
		return nil, fmt.Errorf("%w: the table has no location; add a set-location update", ErrInvalidRequest)
	}

	location, err := newMetadataLocation(updated, current, ptr.MetadataLocation)
	if err != nil {
		return nil, err
	}

	return &plannedCommit{
		ident:    in.Ident,
		ptr:      ptr,
		current:  current,
		updated:  updated,
		location: location,
	}, nil
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

// handleFailedPointerWrite classifies a failed pointer update after new
// metadata files were written to locations, and cleans up when that is safe.
func (s *Service) handleFailedPointerWrite(ctx context.Context, err error, locations ...string) error {
	lostRace := errors.Is(err, store.ErrCommitConflict) ||
		errors.Is(err, store.ErrTableAlreadyExists) ||
		errors.Is(err, store.ErrTableNotFound) ||
		errors.Is(err, store.ErrViewNotFound)
	rejected := errors.Is(err, store.ErrViewAlreadyExists) ||
		errors.Is(err, store.ErrNamespaceNotFound) ||
		errors.Is(err, store.ErrInvalidArgument)

	switch {
	case lostRace:
		s.deleteFiles(ctx, locations, "commit lost a race")

		return fmt.Errorf("%w: %w", errRetry, err)
	case rejected:
		s.deleteFiles(ctx, locations, "commit rejected")

		return err
	default:
		// The pointers may or may not have moved: the new files must stay,
		// and the client must not clean up either.
		s.logFor(ctx).ErrorContext(ctx, "commit state unknown",
			slog.Any("locations", locations),
			logger.Err(err),
		)

		return fmt.Errorf("%w: %w", ErrCommitStateUnknown, err)
	}
}

func (s *Service) deleteFiles(ctx context.Context, locations []string, reason string) {
	for _, location := range locations {
		s.deleteFile(ctx, location, reason)
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
// nil when the table does not exist yet. Only assert-create can hold for a
// missing table.
func checkRequirements(reqs []table.Requirement, current table.Metadata) error {
	for _, req := range reqs {
		if current == nil && req.GetType() != requirementAssertCreate {
			return requirementFailed(req, nil, nil)
		}

		if err := req.Validate(current); err != nil {
			return requirementFailed(req, current, err)
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

// emptyMetadata is the base that a create commit's updates are applied to. It
// has the format version the updates upgrade to, if any, as in Java's
// TableMetadata.buildFromEmpty: an upgrade can never go down from the
// default.
func emptyMetadata(updates []table.Update) (table.Metadata, error) {
	props := iceberg.Properties{}
	if v := requestedFormatVersion(updates); v > 0 {
		props[table.PropertyFormatVersion] = strconv.Itoa(v)
	}

	md, err := table.NewMetadata(iceberg.NewSchema(0), nil, table.UnsortedSortOrder, "", props)
	if err != nil {
		return nil, fmt.Errorf("%w: building empty metadata: %w", ErrInvalidRequest, err)
	}

	return md, nil
}

// requestedFormatVersion returns the format version of the first
// upgrade-format-version update, or 0.
func requestedFormatVersion(updates []table.Update) int {
	for _, u := range updates {
		if u.Action() != table.UpdateUpgradeFormatVersion {
			continue
		}

		raw, err := json.Marshal(u)
		if err != nil {
			return 0
		}

		var f struct {
			FormatVersion int `json:"format-version"`
		}

		if json.Unmarshal(raw, &f) != nil {
			return 0
		}

		return f.FormatVersion
	}

	return 0
}
