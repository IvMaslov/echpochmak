package catalog_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

func viewVersion(sql string) *view.Version {
	return &view.Version{
		VersionID:        7, // the server assigns the first version ID
		TimestampMS:      1_700_000_000_000,
		Summary:          view.VersionSummary{"engine-name": "test"},
		Representations:  []view.Representation{view.NewRepresentation(sql, "trino")},
		DefaultNamespace: table.Identifier{"db"},
	}
}

func (f *fixture) createView(t *testing.T, name, sql string) catalog.View {
	t.Helper()

	v, err := f.svc.CreateView(t.Context(), catalog.CreateViewInput{
		Ident:      ident(name),
		Schema:     testSchema(),
		Version:    viewVersion(sql),
		Properties: map[string]string{"comment": "c"},
	})
	if err != nil {
		t.Fatalf("CreateView(%s): %v", name, err)
	}

	return v
}

func parseView(t *testing.T, v catalog.View) view.Metadata {
	t.Helper()

	md, err := view.ParseMetadataBytes(v.MetadataJSON)
	if err != nil {
		t.Fatalf("parsing view metadata of %v: %v", v.Ident, err)
	}

	return md
}

func parseViewUpdates(t *testing.T, s string) []view.Update {
	t.Helper()

	var upds view.Updates
	if err := json.Unmarshal([]byte(s), &upds); err != nil {
		t.Fatalf("parsing view updates: %v", err)
	}

	return upds
}

func parseViewRequirements(t *testing.T, s string) []view.Requirement {
	t.Helper()

	var reqs view.Requirements
	if err := json.Unmarshal([]byte(s), &reqs); err != nil {
		t.Fatalf("parsing view requirements: %v", err)
	}

	return reqs
}

func TestCreateAndLoadView(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createView(t, "v", "SELECT 1")

	if !strings.HasPrefix(created.MetadataLocation, warehouse+"/db/v-") ||
		!strings.HasSuffix(created.MetadataLocation, ".gz.metadata.json") ||
		!strings.Contains(created.MetadataLocation, "/metadata/00000-") {
		t.Fatalf("metadata location %q, want a gzip file 00000-* under the warehouse", created.MetadataLocation)
	}

	md := parseView(t, created)
	cur := md.CurrentVersion()

	switch {
	case cur.VersionID != 1:
		t.Errorf("current version ID %d, want 1", cur.VersionID)
	case cur.Representations[0].Sql != "SELECT 1" || cur.TimestampMS != 1_700_000_000_000:
		t.Errorf("current version %+v, want the requested SQL and timestamp", cur)
	case md.Properties()["comment"] != "c" || len(md.VersionLog()) != 1:
		t.Errorf("properties %v, version log %v", md.Properties(), md.VersionLog())
	}

	loaded, err := f.svc.LoadView(t.Context(), ident("v"))
	if err != nil || loaded.MetadataLocation != created.MetadataLocation {
		t.Fatalf("LoadView = %+v, %v", loaded, err)
	}

	if err := f.svc.ViewExists(t.Context(), ident("v")); err != nil {
		t.Fatalf("ViewExists: %v", err)
	}

	page, err := f.svc.ListViews(t.Context(), store.Namespace{"db"}, store.PageRequest{})
	if err != nil || len(page.Items) != 1 || page.Items[0].Name != "v" {
		t.Fatalf("ListViews = %+v, %v", page, err)
	}
}

func TestCreateViewErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)
	f.createView(t, "v", "SELECT 1")

	tests := []struct {
		name string
		in   catalog.CreateViewInput
		want error
	}{
		{
			name: "name taken by a table",
			in:   catalog.CreateViewInput{Ident: ident("t"), Schema: testSchema(), Version: viewVersion("SELECT 1")},
			want: store.ErrTableAlreadyExists,
		},
		{
			name: "name taken by a view",
			in:   catalog.CreateViewInput{Ident: ident("v"), Schema: testSchema(), Version: viewVersion("SELECT 1")},
			want: store.ErrViewAlreadyExists,
		},
		{
			name: "missing namespace",
			in: catalog.CreateViewInput{
				Ident:   store.Ident{Namespace: store.Namespace{"nope"}, Name: "v"},
				Schema:  testSchema(),
				Version: viewVersion("SELECT 1"),
			},
			want: store.ErrNamespaceNotFound,
		},
		{
			name: "no representations",
			in: catalog.CreateViewInput{Ident: ident("w"), Schema: testSchema(), Version: &view.Version{
				DefaultNamespace: table.Identifier{"db"},
			}},
			want: catalog.ErrInvalidRequest,
		},
		{
			name: "no schema",
			in:   catalog.CreateViewInput{Ident: ident("w"), Version: viewVersion("SELECT 1")},
			want: catalog.ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := f.svc.CreateView(t.Context(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("CreateView = %v, want %v", err, tt.want)
			}
		})
	}
}

// replaceUpdates is what Java's client sends for CREATE OR REPLACE VIEW.
const replaceUpdates = `[
	{"action": "add-schema", "schema": {"type": "struct", "schema-id": 0, "fields": [
		{"id": 1, "name": "x", "type": "long", "required": false}]}, "last-column-id": 1},
	{"action": "add-view-version", "view-version": {"version-id": 2, "schema-id": -1,
		"timestamp-ms": 1700000001000, "summary": {}, "default-namespace": ["db"],
		"representations": [{"type": "sql", "sql": "SELECT 2 AS x", "dialect": "trino"}]}},
	{"action": "set-current-view-version", "view-version-id": -1}
]`

func TestReplaceView(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createView(t, "v", "SELECT 1")
	viewUUID := parseView(t, created).ViewUUID().String()

	replaced, err := f.svc.ReplaceView(t.Context(), catalog.ReplaceViewInput{
		Ident:        ident("v"),
		Requirements: parseViewRequirements(t, `[{"type": "assert-view-uuid", "uuid": "`+viewUUID+`"}]`),
		Updates:      parseViewUpdates(t, replaceUpdates),
	})
	if err != nil {
		t.Fatalf("ReplaceView: %v", err)
	}

	md := parseView(t, replaced)
	if md.CurrentVersion().Representations[0].Sql != "SELECT 2 AS x" || len(md.Versions()) != 2 {
		t.Fatalf("after replace: current %+v, %d versions", md.CurrentVersion(), len(md.Versions()))
	}

	if !strings.Contains(replaced.MetadataLocation, "/metadata/00001-") {
		t.Fatalf("replaced metadata location %q, want version 00001", replaced.MetadataLocation)
	}

	loaded, err := f.svc.LoadView(t.Context(), ident("v"))
	if err != nil || loaded.MetadataLocation != replaced.MetadataLocation {
		t.Fatalf("LoadView after replace = %+v, %v", loaded, err)
	}

	// Both metadata files are kept: views have no metadata log to expire.
	if !slices.Contains(f.files.list(), created.MetadataLocation) {
		t.Fatal("the first view metadata file was deleted")
	}
}

func TestReplaceViewRequirementFails(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createView(t, "v", "SELECT 1")
	before := f.files.list()

	_, err := f.svc.ReplaceView(t.Context(), catalog.ReplaceViewInput{
		Ident: ident("v"),
		Requirements: parseViewRequirements(t,
			`[{"type": "assert-view-uuid", "uuid": "00000000-0000-0000-0000-00000000dead"}]`),
		Updates: parseViewUpdates(t, replaceUpdates),
	})
	if !errors.Is(err, catalog.ErrRequirementFailed) {
		t.Fatalf("ReplaceView = %v, want ErrRequirementFailed", err)
	}

	if !slices.Equal(f.files.list(), before) {
		t.Fatal("a failed replace wrote files")
	}
}

func TestReplaceViewWithoutChanges(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createView(t, "v", "SELECT 1")

	got, err := f.svc.ReplaceView(t.Context(), catalog.ReplaceViewInput{Ident: ident("v")})
	if err != nil || got.MetadataLocation != created.MetadataLocation {
		t.Fatalf("empty replace = %+v, %v; want the current metadata unchanged", got, err)
	}
}

// viewHookStore makes the next CommitView lose a race once.
type viewHookStore struct {
	store.Store

	lost bool
}

func (v *viewHookStore) CommitView(ctx context.Context, c store.Commit) (store.Pointer, error) {
	if !v.lost && !c.IsCreate() {
		v.lost = true

		return store.Pointer{}, &store.CommitConflictError{Ident: c.Ident, Expected: c.ExpectedVersion}
	}

	return v.Store.CommitView(ctx, c)
}

func TestReplaceViewRetriesLostRace(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 2)
	f.createView(t, "v", "SELECT 1")

	hooked := &viewHookStore{Store: f.store}
	svc := catalog.New(catalog.Options{
		Store:         hooked,
		Files:         f.files,
		Warehouse:     warehouse,
		CommitRetries: 2,
		Logger:        slog.New(slog.DiscardHandler),
	})

	got, err := svc.ReplaceView(t.Context(), catalog.ReplaceViewInput{
		Ident:   ident("v"),
		Updates: parseViewUpdates(t, replaceUpdates),
	})
	if err != nil || !hooked.lost {
		t.Fatalf("ReplaceView after a lost race = %v (race lost: %v)", err, hooked.lost)
	}

	// The file of the attempt that lost was deleted; only the winner's stays.
	files := metadataFiles(f.files.list())
	if len(files) != 2 || !slices.Contains(files, got.MetadataLocation) {
		t.Fatalf("metadata files %q, want the first and the winning one", files)
	}
}

func TestRenameAndDropView(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	f := newFixture(t, 0)
	created := f.createView(t, "v", "SELECT 1")

	if err := f.svc.RenameView(ctx, ident("v"), ident("w")); err != nil {
		t.Fatal(err)
	}

	if err := f.svc.ViewExists(ctx, ident("v")); !errors.Is(err, store.ErrViewNotFound) {
		t.Fatalf("old name after rename: %v", err)
	}

	if err := f.svc.DropView(ctx, ident("w")); err != nil {
		t.Fatal(err)
	}

	if _, err := f.svc.LoadView(ctx, ident("w")); !errors.Is(err, store.ErrViewNotFound) {
		t.Fatalf("LoadView after drop = %v", err)
	}

	// The name is free again, and the files of the dropped view stay.
	f.createView(t, "w", "SELECT 3")

	if !slices.Contains(f.files.list(), created.MetadataLocation) {
		t.Fatal("dropping a view deleted its metadata")
	}
}
