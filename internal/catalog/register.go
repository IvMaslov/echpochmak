package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// RegisterTableInput names existing table metadata to register in the
// catalog.
type RegisterTableInput struct {
	Ident            store.Ident
	MetadataLocation string
	// Overwrite replaces the pointer of an existing table of that name. A
	// view of that name is never replaced.
	Overwrite bool
}

// RegisterTable creates a table pointing at an existing metadata file. The
// catalog writes and deletes nothing: the file and everything it references
// belong to whatever wrote them.
func (s *Service) RegisterTable(ctx context.Context, in RegisterTableInput) (Table, error) {
	if err := in.Ident.Validate(); err != nil {
		return Table{}, err
	}

	_, raw, err := s.readMetadata(ctx, in.MetadataLocation)
	if err != nil {
		return Table{}, fmt.Errorf("%w: registering %q: %w", ErrInvalidRequest, in.MetadataLocation, err)
	}

	entity := slog.String(logger.KeyTable, in.Ident.String())

	_, _, err = retryOnRace(ctx, s, kindRegister, entity, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, s.registerOnce(ctx, in)
	})
	if err != nil {
		return Table{}, tableNameClash(err, in.Ident)
	}

	s.logFor(ctx).InfoContext(ctx, "table registered",
		entity,
		slog.String(logger.KeyLocation, in.MetadataLocation),
		slog.Bool("overwrite", in.Overwrite),
	)

	return Table{Ident: in.Ident, MetadataLocation: in.MetadataLocation, MetadataJSON: raw}, nil
}

// registerOnce creates the pointer, or with Overwrite swaps an existing one
// from the version just read.
func (s *Service) registerOnce(ctx context.Context, in RegisterTableInput) error {
	commit := store.Commit{Ident: in.Ident, MetadataLocation: in.MetadataLocation}

	if in.Overwrite {
		ptr, err := s.store.GetTable(ctx, in.Ident)

		switch {
		case err == nil:
			commit.ExpectedVersion = ptr.Version
		case !errors.Is(err, store.ErrTableNotFound):
			return err
		}
	}

	_, err := s.store.CommitTables(ctx, []store.Commit{commit})

	lostRace := errors.Is(err, store.ErrCommitConflict) || errors.Is(err, store.ErrTableNotFound)
	retryable := in.Overwrite && (lostRace || errors.Is(err, store.ErrTableAlreadyExists))

	if retryable {
		return fmt.Errorf("%w: %w", errRetry, err)
	}

	return err
}

// RegisterView creates a view pointing at an existing view metadata file.
func (s *Service) RegisterView(ctx context.Context, id store.Ident, metadataLocation string) (View, error) {
	if err := id.Validate(); err != nil {
		return View{}, err
	}

	_, raw, err := s.readViewMetadata(ctx, metadataLocation)
	if err != nil {
		return View{}, fmt.Errorf("%w: registering %q: %w", ErrInvalidRequest, metadataLocation, err)
	}

	if _, err := s.store.CommitView(ctx, store.Commit{Ident: id, MetadataLocation: metadataLocation}); err != nil {
		return View{}, viewNameClash(err, id)
	}

	s.logFor(ctx).InfoContext(ctx, "view registered",
		slog.String(logger.KeyView, id.String()),
		slog.String(logger.KeyLocation, metadataLocation),
	)

	return View{Ident: id, MetadataLocation: metadataLocation, MetadataJSON: raw}, nil
}

// UnregisterTable removes a table from the catalog and leaves every file in
// place. It returns the table as it was when removed: the store returns the
// exact pointer it deleted, so the metadata includes every commit made
// before.
func (s *Service) UnregisterTable(ctx context.Context, id store.Ident) (Table, error) {
	dropped, err := s.store.DropTable(ctx, id, store.DropOptions{})
	if err != nil {
		return Table{}, err
	}

	s.logFor(ctx).InfoContext(ctx, "table unregistered",
		slog.String(logger.KeyTable, id.String()),
		slog.String(logger.KeyLocation, dropped.MetadataLocation),
	)

	raw, err := s.readMetadataJSON(ctx, dropped.MetadataLocation)
	if err != nil {
		return Table{}, err
	}

	return Table{Ident: id, MetadataLocation: dropped.MetadataLocation, MetadataJSON: raw}, nil
}

// LoadTableMetadataFile returns the table metadata in the file at location,
// as stored. Idempotent replays of an unregister use it: the table no longer
// exists to load.
func (s *Service) LoadTableMetadataFile(ctx context.Context, location string) (json.RawMessage, error) {
	return s.readMetadataJSON(ctx, location)
}
