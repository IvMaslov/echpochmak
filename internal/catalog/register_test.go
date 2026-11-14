package catalog_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// foreignMetadata writes the metadata of a table created by another catalog
// to location, as an engine would.
func foreignMetadata(t *testing.T, f *fixture, location string) {
	t.Helper()

	other := newFixture(t, 0)
	tbl := other.createTable(t, "elsewhere", map[string]string{"origin": "other"})

	if err := f.files.Write(t.Context(), location, tbl.MetadataJSON); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterTable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	// A name that does not follow the Iceberg file naming convention.
	const location = "mem://other/elsewhere/custom.json"

	foreignMetadata(t, f, location)

	in := catalog.RegisterTableInput{Ident: ident("r"), MetadataLocation: location}

	tbl, err := f.svc.RegisterTable(t.Context(), in)
	if err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	if tbl.MetadataLocation != location || parseMetadata(t, tbl).Properties()["origin"] != "other" {
		t.Fatalf("registered %+v", tbl)
	}

	// Commits continue from the registered file.
	committed, err := f.commitJSON(t, "r", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if err != nil {
		t.Fatalf("commit to a registered table: %v", err)
	}

	md := parseMetadata(t, committed)
	logged := slices.ContainsFunc(slices.Collect(md.PreviousFiles()), func(e table.MetadataLogEntry) bool {
		return e.MetadataFile == location
	})

	if md.Properties()["k"] != "v" || !logged {
		t.Fatalf("after commit: properties %v, metadata log %v", md.Properties(), slices.Collect(md.PreviousFiles()))
	}

	if !slices.Contains(f.files.list(), location) {
		t.Fatal("the registered metadata file was deleted")
	}
}

func register(name, location string) catalog.RegisterTableInput {
	return catalog.RegisterTableInput{Ident: ident(name), MetadataLocation: location}
}

func TestRegisterTableErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	foreignMetadata(t, f, "mem://other/m.json")
	f.createTable(t, "t", nil)
	f.createView(t, "v", "SELECT 1")

	if err := f.files.Write(t.Context(), "mem://other/garbage.json", []byte("{not json")); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		in   catalog.RegisterTableInput
		want error
	}{
		{name: "missing file", in: register("x", "mem://other/none.json"), want: catalog.ErrInvalidRequest},
		{name: "invalid metadata", in: register("x", "mem://other/garbage.json"), want: catalog.ErrInvalidRequest},
		{name: "taken by a table", in: register("t", "mem://other/m.json"), want: store.ErrTableAlreadyExists},
		{name: "taken by a view", in: register("v", "mem://other/m.json"), want: store.ErrViewAlreadyExists},
		{
			name: "overwrite never replaces a view",
			in: catalog.RegisterTableInput{
				Ident:            ident("v"),
				MetadataLocation: "mem://other/m.json",
				Overwrite:        true,
			},
			want: store.ErrViewAlreadyExists,
		},
		{
			name: "missing namespace",
			in: catalog.RegisterTableInput{
				Ident:            store.Ident{Namespace: store.Namespace{"nope"}, Name: "x"},
				MetadataLocation: "mem://other/m.json",
			},
			want: store.ErrNamespaceNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := f.svc.RegisterTable(t.Context(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("RegisterTable = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestRegisterTableOverwrite(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)
	foreignMetadata(t, f, "mem://other/m.json")

	tbl, err := f.svc.RegisterTable(t.Context(), catalog.RegisterTableInput{
		Ident:            ident("t"),
		MetadataLocation: "mem://other/m.json",
		Overwrite:        true,
	})
	if err != nil || tbl.MetadataLocation != "mem://other/m.json" {
		t.Fatalf("overwrite = %+v, %v", tbl, err)
	}

	// The overwritten table's files belong to whoever wrote them.
	if !slices.Contains(f.files.list(), created.MetadataLocation) {
		t.Fatal("overwriting deleted the previous metadata")
	}
}

func TestRegisterView(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createView(t, "v", "SELECT 1")

	if err := f.svc.DropView(t.Context(), ident("v")); err != nil {
		t.Fatal(err)
	}

	got, err := f.svc.RegisterView(t.Context(), ident("again"), created.MetadataLocation)
	if err != nil || got.MetadataLocation != created.MetadataLocation {
		t.Fatalf("RegisterView = %+v, %v", got, err)
	}

	_, err = f.svc.RegisterView(t.Context(), ident("again"), created.MetadataLocation)
	if !errors.Is(err, store.ErrViewAlreadyExists) {
		t.Fatalf("second RegisterView = %v, want ErrViewAlreadyExists", err)
	}

	_, err = f.svc.RegisterView(t.Context(), ident("x"), "mem://none.json")
	if !errors.Is(err, catalog.ErrInvalidRequest) {
		t.Fatalf("RegisterView of a missing file = %v, want ErrInvalidRequest", err)
	}
}

func TestUnregisterTable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	committed, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if err != nil {
		t.Fatal(err)
	}

	got, err := f.svc.UnregisterTable(t.Context(), ident("t"))
	if err != nil {
		t.Fatalf("UnregisterTable: %v", err)
	}

	if got.MetadataLocation != committed.MetadataLocation || parseMetadata(t, got).Properties()["k"] != "v" {
		t.Fatalf("unregistered %q, want the last commit %q", got.MetadataLocation, committed.MetadataLocation)
	}

	if err := f.svc.TableExists(t.Context(), ident("t")); !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("TableExists after unregister = %v", err)
	}

	tasks, err := f.store.ListPurgeTasks(t.Context(), 10)
	if err != nil || len(tasks) != 0 || !slices.Contains(f.files.list(), got.MetadataLocation) {
		t.Fatal("unregister must keep every file and queue no purge")
	}

	if _, err := f.svc.UnregisterTable(t.Context(), ident("t")); !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("second UnregisterTable = %v, want ErrTableNotFound", err)
	}

	// The same metadata can be registered again.
	if _, err := f.svc.RegisterTable(t.Context(), catalog.RegisterTableInput{
		Ident:            ident("t"),
		MetadataLocation: got.MetadataLocation,
	}); err != nil {
		t.Fatalf("registering the unregistered table again: %v", err)
	}
}
