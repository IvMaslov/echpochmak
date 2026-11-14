package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// Table property that allows deleting the table's files; purging honours it.
const (
	gcEnabledKey     = "gc.enabled"
	gcEnabledDefault = true
)

// Table is a table as served to clients.
type Table struct {
	Ident store.Ident
	// MetadataLocation is the current metadata file, or "" for a staged table
	// that has not been committed yet.
	MetadataLocation string
	// MetadataJSON is the table metadata, exactly as stored. Serving the
	// stored bytes guarantees clients see every field, including ones this
	// server does not model.
	MetadataJSON json.RawMessage
}

// CreateTableInput describes a table to create.
type CreateTableInput struct {
	Ident store.Ident
	// Location is the table location; "" picks the default.
	Location string
	Schema   *iceberg.Schema
	// PartitionSpec may be nil for an unpartitioned table.
	PartitionSpec *iceberg.PartitionSpec
	// SortOrder may be nil for an unsorted table.
	SortOrder  *table.SortOrder
	Properties map[string]string
	// Stage returns the metadata of the new table without persisting
	// anything. The client creates the table later with a commit that
	// carries the assert-create requirement.
	Stage bool
}

// CreateTable creates a table, or stages it if in.Stage is set.
//
// The server assigns fresh field, partition and sort order IDs, as the
// Iceberg spec requires, so IDs in the input are not preserved.
func (s *Service) CreateTable(ctx context.Context, in CreateTableInput) (Table, error) {
	if err := in.Ident.Validate(); err != nil {
		return Table{}, err
	}

	if in.Schema == nil {
		return Table{}, fmt.Errorf("%w: a schema is required", ErrInvalidRequest)
	}

	ns, err := s.store.GetNamespace(ctx, in.Ident.Namespace)
	if err != nil {
		return Table{}, err
	}

	// Reject a taken name before writing anything; the commit below catches
	// the race where the name is taken in between.
	if err := s.requireNameFree(ctx, in.Ident); err != nil {
		return Table{}, tableNameClash(err, in.Ident)
	}

	md, err := s.newTableMetadata(in, ns.Properties)
	if err != nil {
		return Table{}, err
	}

	if in.Stage {
		raw, err := json.Marshal(md)
		if err != nil {
			return Table{}, fmt.Errorf("encoding metadata: %w", err)
		}

		return Table{Ident: in.Ident, MetadataJSON: raw}, nil
	}

	return s.persistNewTable(ctx, in.Ident, md)
}

func (s *Service) newTableMetadata(in CreateTableInput, nsProps map[string]string) (table.Metadata, error) {
	tableUUID := s.newUUID()

	location := strings.TrimSuffix(in.Location, "/")
	if location == "" {
		location = s.defaultTableLocation(in.Ident.Namespace, nsProps, in.Ident.Name, tableUUID.String())
	}

	sortOrder := table.UnsortedSortOrder
	if in.SortOrder != nil {
		sortOrder = *in.SortOrder
	}

	md, err := table.NewMetadataWithUUID(
		in.Schema,
		in.PartitionSpec,
		sortOrder,
		location,
		iceberg.Properties(in.Properties),
		tableUUID,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	return md, nil
}

// persistNewTable writes the first metadata file of a table and creates its
// pointer.
func (s *Service) persistNewTable(ctx context.Context, id store.Ident, md table.Metadata) (Table, error) {
	location, err := newMetadataLocation(md, nil, "")
	if err != nil {
		return Table{}, err
	}

	raw, err := s.writeMetadata(ctx, location, md)
	if err != nil {
		return Table{}, err
	}

	ptrs, err := s.store.CommitTables(ctx, []store.Commit{{Ident: id, MetadataLocation: location}})
	if err != nil {
		return Table{}, tableNameClash(s.handleFailedPointerWrite(ctx, err, location), id)
	}

	s.logFor(ctx).InfoContext(ctx, "table created",
		slog.String(logger.KeyTable, id.String()),
		slog.String(logger.KeyLocation, location),
		slog.Int64(logger.KeyVersion, int64(ptrs[0].Version)),
	)

	return Table{Ident: id, MetadataLocation: location, MetadataJSON: raw}, nil
}

// LoadTable returns the current metadata of a table.
func (s *Service) LoadTable(ctx context.Context, id store.Ident) (Table, error) {
	ptr, err := s.store.GetTable(ctx, id)
	if err != nil {
		return Table{}, err
	}

	raw, err := s.readMetadataJSON(ctx, ptr.MetadataLocation)
	if err != nil {
		return Table{}, err
	}

	return Table{Ident: id, MetadataLocation: ptr.MetadataLocation, MetadataJSON: raw}, nil
}

// TableExists reports whether a table exists. A missing table is an error,
// [store.ErrTableNotFound], so it maps to the same response as a load.
func (s *Service) TableExists(ctx context.Context, id store.Ident) error {
	_, err := s.store.GetTable(ctx, id)

	return err
}

// ListTables returns one page of the tables in ns.
func (s *Service) ListTables(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return s.store.ListTables(ctx, ns, req)
}

// RenameTable moves a table to a new identifier. Its location and files stay
// where they are.
func (s *Service) RenameTable(ctx context.Context, from, to store.Ident) error {
	if err := s.store.RenameTable(ctx, from, to); err != nil {
		return renameClash(err, from, to)
	}

	s.logFor(ctx).InfoContext(ctx, "table renamed",
		slog.String(logger.KeyTable, from.String()),
		slog.String("to", to.String()),
	)

	return nil
}

// DropTable removes a table from the catalog. With purge, its files are
// deleted afterwards by a purge worker: the drop enqueues the task in the same
// store write. A table that sets gc.enabled=false cannot be purged; the drop
// is then rejected and nothing changes.
func (s *Service) DropTable(ctx context.Context, id store.Ident, purge bool) error {
	if purge {
		if err := s.checkPurgeAllowed(ctx, id); err != nil {
			return err
		}
	}

	dropped, err := s.store.DropTable(ctx, id, store.DropOptions{Purge: purge})
	if err != nil {
		return err
	}

	s.logFor(ctx).InfoContext(ctx, "table dropped",
		slog.String(logger.KeyTable, id.String()),
		slog.String(logger.KeyLocation, dropped.MetadataLocation),
		slog.Bool("purge", purge),
	)

	return nil
}

// checkPurgeAllowed fails if the table's current metadata disables garbage
// collection.
func (s *Service) checkPurgeAllowed(ctx context.Context, id store.Ident) error {
	ptr, err := s.store.GetTable(ctx, id)
	if err != nil {
		return err
	}

	md, _, err := s.readMetadata(ctx, ptr.MetadataLocation)
	if err != nil {
		return err
	}

	if !md.Properties().GetBool(gcEnabledKey, gcEnabledDefault) {
		return fmt.Errorf("%w: cannot purge %q: the table sets %s=false", ErrInvalidRequest, id.String(), gcEnabledKey)
	}

	return nil
}

// ReportMetrics accepts a metrics report for a table. Reports are logged at
// debug level; the table must exist.
func (s *Service) ReportMetrics(ctx context.Context, id store.Ident, reportType string) error {
	if err := s.TableExists(ctx, id); err != nil {
		return err
	}

	s.logFor(ctx).DebugContext(ctx, "metrics report received",
		slog.String(logger.KeyTable, id.String()),
		slog.String("report_type", reportType),
	)

	return nil
}

// requireNameFree fails if id is taken by a table or a view.
func (s *Service) requireNameFree(ctx context.Context, id store.Ident) error {
	_, err := s.store.GetTable(ctx, id)

	switch {
	case err == nil:
		return fmt.Errorf("%w: %s", store.ErrTableAlreadyExists, id.String())
	case !errors.Is(err, store.ErrTableNotFound):
		return err
	}

	_, err = s.store.GetView(ctx, id)

	switch {
	case err == nil:
		return fmt.Errorf("%w: %s", store.ErrViewAlreadyExists, id.String())
	case !errors.Is(err, store.ErrViewNotFound):
		return err
	default:
		return nil
	}
}
