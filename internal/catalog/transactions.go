package catalog

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/iceberg-go/table"
	"golang.org/x/sync/errgroup"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// maxParallelWrites bounds the metadata files a transaction writes at once.
const maxParallelWrites = 8

// CommitTransaction applies changes to several tables atomically: every
// table's requirements are checked and every new metadata file is written,
// then all pointers are swapped in one store write, or none are. A lost race
// on any table retries the whole transaction, with every requirement checked
// again.
//
// A table listed with requirements but no updates is checked against the
// state read at the start of the attempt; its pointer is not written, so a
// commit to it that lands during the attempt is not detected.
func (s *Service) CommitTransaction(ctx context.Context, changes []CommitInput) error {
	if err := validateTransaction(changes); err != nil {
		return err
	}

	start := time.Now()
	entity := slog.Int("tables", len(changes))

	written, attempts, err := retryOnRace(ctx, s, kindTransaction, entity, func(ctx context.Context) (int, error) {
		return s.commitTransactionOnce(ctx, changes)
	})
	if err != nil {
		return err
	}

	s.logFor(ctx).InfoContext(ctx, "transaction committed",
		entity,
		slog.Int("tables_written", written),
		slog.Int(logger.KeyAttempt, attempts),
		slog.Duration(logger.KeyDuration, time.Since(start)),
	)

	return nil
}

func validateTransaction(changes []CommitInput) error {
	if len(changes) == 0 {
		return fmt.Errorf("%w: a transaction needs at least one table change", ErrInvalidRequest)
	}

	seen := make(map[string]bool, len(changes))

	for _, c := range changes {
		if err := c.Ident.Validate(); err != nil {
			return err
		}

		key := c.Ident.Namespace.Child(c.Ident.Name).Encode()
		if seen[key] {
			return fmt.Errorf("%w: table %q appears twice in the transaction", ErrInvalidRequest, c.Ident.String())
		}

		seen[key] = true
	}

	return nil
}

// commitTransactionOnce makes one attempt and returns the number of tables
// whose pointers it swapped.
func (s *Service) commitTransactionOnce(ctx context.Context, changes []CommitInput) (int, error) {
	plans, err := s.planTransaction(ctx, changes)
	if err != nil {
		return 0, err
	}

	if len(plans) == 0 {
		return 0, nil
	}

	locations := make([]string, 0, len(plans))
	ids := make([]store.Ident, 0, len(plans))

	for _, p := range plans {
		locations = append(locations, p.location)
		ids = append(ids, p.ident)
	}

	intent, err := s.beginWrite(ctx, ids, locations)
	if err != nil {
		return 0, err
	}

	if err := s.writeAll(ctx, plans); err != nil {
		s.abandonWrite(ctx, intent, locations, "transaction failed before its commit")

		return 0, err
	}

	commits := make([]store.Commit, 0, len(plans))
	for _, p := range plans {
		commits = append(commits, p.commit(intent))
	}

	if _, err := s.store.CommitTables(ctx, commits); err != nil {
		return 0, s.handleFailedPointerWrite(ctx, err, intent, locations)
	}

	for _, p := range plans {
		s.deleteExpiredMetadata(ctx, p.current, p.updated)
	}

	return len(plans), nil
}

// planTransaction reads every pointer in one store read, then every current
// metadata file, and plans each table's commit. Any failing requirement fails
// the whole transaction before anything is written.
func (s *Service) planTransaction(ctx context.Context, changes []CommitInput) ([]*plannedCommit, error) {
	ids := make([]store.Ident, 0, len(changes))
	for _, c := range changes {
		ids = append(ids, c.Ident)
	}

	ptrs, err := s.store.GetTables(ctx, ids)
	if err != nil {
		return nil, err
	}

	plans := make([]*plannedCommit, 0, len(changes))

	for i, c := range changes {
		ptr := ptrs[i]

		var current table.Metadata

		switch {
		case ptr.Version != 0:
			current, _, err = s.readMetadata(ctx, ptr.MetadataLocation)
			if err != nil {
				return nil, err
			}
		case !createsTable(c.Requirements):
			return nil, fmt.Errorf("%w: %s", store.ErrTableNotFound, c.Ident.String())
		}

		plan, err := planCommit(c, ptr, current)
		if err != nil {
			// The table goes after the message, which clients may match
			// from its start.
			return nil, &messageError{msg: fmt.Sprintf("%v (table %s)", err, c.Ident.String()), err: err}
		}

		if plan != nil {
			plans = append(plans, plan)
		}
	}

	return plans, nil
}

// writeAll writes the metadata files of plans, a few at a time.
func (s *Service) writeAll(ctx context.Context, plans []*plannedCommit) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxParallelWrites)

	for _, p := range plans {
		g.Go(func() error {
			_, err := s.writeMetadata(gctx, p.location, p.updated)

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("writing transaction metadata: %w", err)
	}

	return nil
}
