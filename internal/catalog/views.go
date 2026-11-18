package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"

	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// viewMetadataCompressionDefault is the codec of view metadata files when the
// view does not set write.metadata.compression-codec. Views default to gzip,
// unlike tables, as in the Java implementation.
const viewMetadataCompressionDefault = table.MetadataCompressionCodecGzip

// View is a view as served to clients.
type View struct {
	Ident            store.Ident
	MetadataLocation string
	// MetadataJSON is the view metadata, exactly as stored.
	MetadataJSON json.RawMessage
}

// CreateViewInput describes a view to create.
type CreateViewInput struct {
	Ident store.Ident
	// Location is the view location; "" picks the default.
	Location string
	Schema   *iceberg.Schema
	// Version is the first version of the view: its SQL representations,
	// default namespace and catalog, and summary. Its version and schema IDs
	// are assigned by the server.
	Version    *view.Version
	Properties map[string]string
}

// ReplaceViewInput is a set of changes to one view.
type ReplaceViewInput struct {
	Ident        store.Ident
	Requirements []view.Requirement
	Updates      []view.Update
}

// CreateView creates a view.
func (s *Service) CreateView(ctx context.Context, in CreateViewInput) (View, error) {
	if err := in.Ident.Validate(); err != nil {
		return View{}, err
	}

	if in.Schema == nil || in.Version == nil {
		return View{}, fmt.Errorf("%w: a view needs a schema and a view version", ErrInvalidRequest)
	}

	ns, err := s.store.GetNamespace(ctx, in.Ident.Namespace)
	if err != nil {
		return View{}, err
	}

	if err := s.requireNameFree(ctx, in.Ident); err != nil {
		return View{}, viewNameClash(err, in.Ident)
	}

	md, err := s.newViewMetadata(in, ns.Properties)
	if err != nil {
		return View{}, err
	}

	location, err := newViewMetadataLocation(md, "")
	if err != nil {
		return View{}, err
	}

	var (
		raw []byte
		ptr store.Pointer
	)

	err = s.writeAndCommit(ctx, in.Ident, location,
		func() (err error) {
			raw, err = s.writeViewMetadata(ctx, location, md)

			return err
		},
		func(intent store.IntentRef) (err error) {
			ptr, err = s.store.CommitView(ctx, store.Commit{
				Ident:            in.Ident,
				MetadataLocation: location,
				Intent:           intent,
			})

			return err
		},
	)
	if err != nil {
		return View{}, viewNameClash(err, in.Ident)
	}

	s.logFor(ctx).InfoContext(ctx, "view created",
		slog.String(logger.KeyView, in.Ident.String()),
		slog.String(logger.KeyLocation, location),
		slog.Int64(logger.KeyVersion, int64(ptr.Version)),
	)

	return View{Ident: in.Ident, MetadataLocation: location, MetadataJSON: raw}, nil
}

func (s *Service) newViewMetadata(in CreateViewInput, nsProps map[string]string) (view.Metadata, error) {
	viewUUID := s.newUUID()

	location := strings.TrimSuffix(in.Location, "/")
	if location == "" {
		location = s.defaultTableLocation(in.Ident.Namespace, nsProps, in.Ident.Name, viewUUID.String())
	}

	// The first version of a view is version 1, whatever the client sent,
	// as in the Java implementation.
	version := in.Version.Clone()
	version.VersionID = 1

	if version.TimestampMS == 0 {
		version.TimestampMS = time.Now().UnixMilli()
	}

	if version.DefaultNamespace == nil {
		version.DefaultNamespace = table.Identifier{}
	}

	b, err := view.NewMetadataBuilder()
	if err != nil {
		return nil, fmt.Errorf("building view metadata: %w", err)
	}

	res, err := b.SetUUID(viewUUID).
		SetLoc(location).
		SetProperties(iceberg.Properties(in.Properties)).
		SetCurrentVersion(version, in.Schema).
		Build()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	return res.Metadata, nil
}

// LoadView returns the current metadata of a view.
func (s *Service) LoadView(ctx context.Context, id store.Ident) (View, error) {
	ptr, err := s.store.GetView(ctx, id)
	if err != nil {
		return View{}, err
	}

	raw, err := s.readMetadataJSON(ctx, ptr.MetadataLocation)
	if err != nil {
		return View{}, err
	}

	return View{Ident: id, MetadataLocation: ptr.MetadataLocation, MetadataJSON: raw}, nil
}

// ViewExists reports whether a view exists: a missing view is
// [store.ErrViewNotFound].
func (s *Service) ViewExists(ctx context.Context, id store.Ident) error {
	_, err := s.store.GetView(ctx, id)

	return err
}

// ListViews returns one page of the views in ns.
func (s *Service) ListViews(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return s.store.ListViews(ctx, ns, req)
}

// ReplaceView applies changes to a view: load, check the requirements, apply
// the updates, write a new metadata file and swap the pointer. A lost race is
// retried like a table commit.
func (s *Service) ReplaceView(ctx context.Context, in ReplaceViewInput) (View, error) {
	if err := in.Ident.Validate(); err != nil {
		return View{}, err
	}

	entity := slog.String(logger.KeyView, in.Ident.String())

	v, attempts, err := retryOnRace(ctx, s, kindView, entity, func(ctx context.Context) (View, error) {
		return s.replaceViewOnce(ctx, in)
	})
	if err != nil {
		return View{}, err
	}

	s.logFor(ctx).InfoContext(ctx, "view replaced",
		entity,
		slog.String(logger.KeyLocation, v.MetadataLocation),
		slog.Int("updates", len(in.Updates)),
		slog.Int(logger.KeyAttempt, attempts),
	)

	return v, nil
}

func (s *Service) replaceViewOnce(ctx context.Context, in ReplaceViewInput) (View, error) {
	ptr, err := s.store.GetView(ctx, in.Ident)
	if err != nil {
		return View{}, err
	}

	current, raw, err := s.readViewMetadata(ctx, ptr.MetadataLocation)
	if err != nil {
		return View{}, err
	}

	for _, req := range in.Requirements {
		if err := req.Validate(current); err != nil {
			return View{}, viewRequirementFailed(req, current, err)
		}
	}

	b, err := view.MetadataBuilderFromBase(current)
	if err != nil {
		return View{}, fmt.Errorf("building view metadata: %w", err)
	}

	for _, upd := range in.Updates {
		if err := upd.Apply(b); err != nil {
			return View{}, fmt.Errorf("%w: applying %s: %w", ErrInvalidRequest, upd.Action(), err)
		}
	}

	if !b.HasChanges() {
		return View{Ident: in.Ident, MetadataLocation: ptr.MetadataLocation, MetadataJSON: raw}, nil
	}

	res, err := b.Build()
	if err != nil {
		return View{}, fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	location, err := newViewMetadataLocation(res.Metadata, ptr.MetadataLocation)
	if err != nil {
		return View{}, err
	}

	var written []byte

	err = s.writeAndCommit(ctx, in.Ident, location,
		func() (err error) {
			written, err = s.writeViewMetadata(ctx, location, res.Metadata)

			return err
		},
		func(intent store.IntentRef) error {
			_, err := s.store.CommitView(ctx, store.Commit{
				Ident:            in.Ident,
				ExpectedVersion:  ptr.Version,
				MetadataLocation: location,
				Intent:           intent,
			})

			return err
		},
	)
	if err != nil {
		return View{}, err
	}

	return View{Ident: in.Ident, MetadataLocation: location, MetadataJSON: written}, nil
}

// RenameView moves a view to a new identifier.
func (s *Service) RenameView(ctx context.Context, from, to store.Ident) error {
	if err := s.store.RenameView(ctx, from, to); err != nil {
		return renameClash(err, from, to)
	}

	s.logFor(ctx).InfoContext(ctx, "view renamed",
		slog.String(logger.KeyView, from.String()),
		slog.String("to", to.String()),
	)

	return nil
}

// DropView removes a view from the catalog. Its metadata files are kept: the
// spec has no purge for views.
func (s *Service) DropView(ctx context.Context, id store.Ident) error {
	if err := s.store.DropView(ctx, id); err != nil {
		return err
	}

	s.logFor(ctx).InfoContext(ctx, "view dropped", slog.String(logger.KeyView, id.String()))

	return nil
}

// readViewMetadata reads and parses the view metadata file at location.
func (s *Service) readViewMetadata(ctx context.Context, location string) (view.Metadata, []byte, error) {
	raw, err := s.readMetadataJSON(ctx, location)
	if err != nil {
		return nil, nil, err
	}

	md, err := view.ParseMetadataBytes(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing view metadata file %q: %w", location, err)
	}

	return md, raw, nil
}

// writeViewMetadata encodes md with the codec its properties ask for, writes
// it to location and returns the uncompressed JSON.
func (s *Service) writeViewMetadata(ctx context.Context, location string, md view.Metadata) ([]byte, error) {
	raw, err := json.Marshal(md)
	if err != nil {
		return nil, fmt.Errorf("encoding view metadata: %w", err)
	}

	data, err := compress(raw, viewCodec(md.Properties()))
	if err != nil {
		return nil, err
	}

	if err := s.files.Write(ctx, location, data); err != nil {
		return nil, fmt.Errorf("writing view metadata file: %w", err)
	}

	return raw, nil
}

func viewCodec(props iceberg.Properties) string {
	return props.Get(table.MetadataCompressionKey, viewMetadataCompressionDefault)
}

// newViewMetadataLocation returns the location of the next metadata file of
// md, following the current file at currentLocation ("" for a new view). It
// names files as tables do: "<version>-<uuid>[.codec].metadata.json" under
// the metadata directory, or under write.metadata.path if the view sets it.
func newViewMetadataLocation(md view.Metadata, currentLocation string) (string, error) {
	version := 0
	if currentLocation != "" {
		version = nextViewMetadataVersion(md, currentLocation)
	}

	props := maps.Clone(md.Properties())
	if props == nil {
		props = iceberg.Properties{}
	}

	props[table.MetadataCompressionKey] = viewCodec(md.Properties())

	provider, err := table.LoadLocationProvider(md.Location(), props)
	if err != nil {
		return "", fmt.Errorf("%w: view location %q: %w", ErrInvalidRequest, md.Location(), err)
	}

	loc, err := provider.NewTableMetadataFileLocation(version)
	if err != nil {
		return "", fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}

	return loc, nil
}

// nextViewMetadataVersion returns the version number for the file after the
// one at currentLocation. A name that does not follow the convention, as in a
// registered view, falls back to the number of view versions.
func nextViewMetadataVersion(md view.Metadata, currentLocation string) int {
	name := path.Base(currentLocation)

	for _, re := range []*regexp.Regexp{metadataFileName, legacyMetadataFileName} {
		if m := re.FindStringSubmatch(name); m != nil {
			if v, err := strconv.Atoi(m[1]); err == nil {
				return v + 1
			}
		}
	}

	return len(md.Versions()) + 1
}

// viewRequirementFailed returns the error for a failed view requirement, with
// the reference implementation's message for the one requirement type views
// have.
func viewRequirementFailed(req view.Requirement, current view.Metadata, cause error) error {
	raw, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrRequirementFailed, req.GetType(), cause)
	}

	var f struct {
		Type string `json:"type"`
		UUID string `json:"uuid"`
	}

	if err := json.Unmarshal(raw, &f); err != nil || f.Type != "assert-view-uuid" {
		return fmt.Errorf("%w: %s: %w", ErrRequirementFailed, req.GetType(), cause)
	}

	return withMessage(ErrRequirementFailed,
		"Requirement failed: view UUID does not match: expected %s != %s", current.ViewUUID(), f.UUID)
}
