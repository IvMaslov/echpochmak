package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

func TestCreateTableLocations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nsProps  map[string]string
		location string
		want     string
	}{
		{
			name: "warehouse default with table UUID",
			want: warehouse + "/db/t-00000000-0000-0000-0000-000000000001",
		},
		{
			name:    "namespace location",
			nsProps: map[string]string{"location": "s3://bucket/ns/"},
			want:    "s3://bucket/ns/t",
		},
		{
			name:     "explicit location wins",
			nsProps:  map[string]string{"location": "s3://bucket/ns"},
			location: "mem://elsewhere/t/",
			want:     "mem://elsewhere/t",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, 0)
			if tt.nsProps != nil {
				_, err := f.svc.UpdateNamespaceProperties(t.Context(), store.Namespace{"db"}, store.PropertiesUpdate{
					Updates: tt.nsProps,
				})
				if err != nil {
					t.Fatal(err)
				}
			}

			tbl, err := f.svc.CreateTable(t.Context(), catalog.CreateTableInput{
				Ident:    ident("t"),
				Location: tt.location,
				Schema:   testSchema(),
				Stage:    true,
			})
			if err != nil {
				t.Fatalf("CreateTable: %v", err)
			}

			if got := parseMetadata(t, tbl).Location(); got != tt.want {
				t.Fatalf("location = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestCreateTablePersists(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)

	if path.Base(path.Dir(created.MetadataLocation)) != "metadata" ||
		!strings.HasPrefix(path.Base(created.MetadataLocation), "00000-") {
		t.Fatalf("metadata location %q, want <table>/metadata/00000-<uuid>.metadata.json", created.MetadataLocation)
	}

	loaded, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}

	sameJSON := string(loaded.MetadataJSON) == string(created.MetadataJSON)
	if loaded.MetadataLocation != created.MetadataLocation || !sameJSON {
		t.Fatal("loaded table differs from the created one")
	}

	md := parseMetadata(t, loaded)
	if md.TableUUID().String() != "00000000-0000-0000-0000-000000000001" || md.CurrentSchema().NumFields() != 2 {
		t.Fatalf("unexpected metadata: uuid %s, %d fields", md.TableUUID(), md.CurrentSchema().NumFields())
	}
}

func TestStageCreatePersistsNothing(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	tbl, err := f.svc.CreateTable(t.Context(), catalog.CreateTableInput{
		Ident:  ident("t"),
		Schema: testSchema(),
		Stage:  true,
	})
	if err != nil {
		t.Fatalf("stage create: %v", err)
	}

	if tbl.MetadataLocation != "" || len(f.files.list()) != 0 {
		t.Fatalf("staged create wrote %v / location %q", f.files.list(), tbl.MetadataLocation)
	}

	if err := f.svc.TableExists(t.Context(), ident("t")); !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("TableExists after staging = %v, want ErrTableNotFound", err)
	}
}

func TestCreateTableErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "taken", nil)

	if _, err := f.svc.CommitTable(t.Context(), catalog.CommitInput{
		Ident:        ident("view"),
		Requirements: parseRequirements(t, `[{"type":"assert-create"}]`),
	}); err == nil {
		t.Fatal("commit creating a table without a location succeeded")
	}

	tests := []struct {
		name string
		in   catalog.CreateTableInput
		want error
	}{
		{
			name: "missing namespace",
			in: catalog.CreateTableInput{
				Ident:  store.Ident{Namespace: store.Namespace{"nope"}, Name: "t"},
				Schema: testSchema(),
			},
			want: store.ErrNamespaceNotFound,
		},
		{
			name: "duplicate",
			in:   catalog.CreateTableInput{Ident: ident("taken"), Schema: testSchema()},
			want: store.ErrTableAlreadyExists,
		},
		{
			name: "no schema",
			in:   catalog.CreateTableInput{Ident: ident("t")},
			want: catalog.ErrInvalidRequest,
		},
		{
			name: "bad identifier",
			in:   catalog.CreateTableInput{Ident: ident(""), Schema: testSchema()},
			want: store.ErrInvalidArgument,
		},
		{
			name: "bad compression codec",
			in: catalog.CreateTableInput{
				Ident:      ident("t"),
				Schema:     testSchema(),
				Properties: map[string]string{table.MetadataCompressionKey: "lzma"},
			},
			want: catalog.ErrInvalidRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if _, err := f.svc.CreateTable(t.Context(), tt.in); !errors.Is(err, tt.want) {
				t.Fatalf("CreateTable error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestCreateTableRejectsViewName(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	view := store.Commit{Ident: ident("v"), MetadataLocation: "mem://v.json"}
	if _, err := f.store.CommitView(t.Context(), view); err != nil {
		t.Fatal(err)
	}

	_, err := f.svc.CreateTable(t.Context(), catalog.CreateTableInput{Ident: ident("v"), Schema: testSchema()})
	if !errors.Is(err, store.ErrViewAlreadyExists) {
		t.Fatalf("CreateTable over a view = %v, want ErrViewAlreadyExists", err)
	}
}

// stagedCreateUpdates is what the Java REST client sends to create a table in
// a transaction: assert-create plus the whole initial metadata as updates.
const stagedCreateUpdates = `[
	{"action":"assign-uuid","uuid":"11111111-2222-3333-4444-555555555555"},
	{"action":"upgrade-format-version","format-version":2},
	{"action":"add-schema","schema":{"type":"struct","schema-id":0,"fields":[
		{"id":1,"name":"id","required":true,"type":"long"},
		{"id":2,"name":"name","required":false,"type":"string"}]},"last-column-id":2},
	{"action":"set-current-schema","schema-id":-1},
	{"action":"add-spec","spec":{"spec-id":0,"fields":[
		{"name":"id_bucket","transform":"bucket[4]","source-id":1,"field-id":1000}]}},
	{"action":"set-default-spec","spec-id":-1},
	{"action":"add-sort-order","sort-order":{"order-id":0,"fields":[]}},
	{"action":"set-default-sort-order","sort-order-id":-1},
	{"action":"set-location","location":"mem://warehouse/db/created"},
	{"action":"set-properties","updates":{"owner":"trino"}}
]`

func TestCommitCreatesTable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	tbl, err := f.commitJSON(t, "created", `[{"type":"assert-create"}]`, stagedCreateUpdates)
	if err != nil {
		t.Fatalf("create commit: %v", err)
	}

	md := parseMetadata(t, tbl)
	spec := md.PartitionSpec()

	switch {
	case md.TableUUID().String() != "11111111-2222-3333-4444-555555555555":
		t.Fatalf("uuid = %s", md.TableUUID())
	case md.Location() != "mem://warehouse/db/created":
		t.Fatalf("location = %s", md.Location())
	case spec.NumFields() != 1:
		t.Fatalf("partition spec = %v", md.PartitionSpec())
	case md.Properties()["owner"] != "trino":
		t.Fatalf("properties = %v", md.Properties())
	case !strings.HasPrefix(path.Base(tbl.MetadataLocation), "00000-"):
		t.Fatalf("metadata location = %s", tbl.MetadataLocation)
	}

	// The same commit again must fail: the table exists now.
	_, err = f.commitJSON(t, "created", `[{"type":"assert-create"}]`, stagedCreateUpdates)
	if !errors.Is(err, catalog.ErrRequirementFailed) {
		t.Fatalf("second create commit = %v, want ErrRequirementFailed", err)
	}
}

func TestCommitUpdatesTable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)
	uuid := parseMetadata(t, created).TableUUID()

	reqs := fmt.Sprintf(
		`[{"type":"assert-table-uuid","uuid":%q},{"type":"assert-current-schema-id","current-schema-id":0}]`,
		uuid,
	)

	updated, err := f.commitJSON(t, "t", reqs, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	md := parseMetadata(t, updated)
	if md.Properties()["k"] != "v" {
		t.Fatalf("properties = %v", md.Properties())
	}

	if !strings.HasPrefix(path.Base(updated.MetadataLocation), "00001-") {
		t.Fatalf("second metadata file %q, want version 00001", updated.MetadataLocation)
	}

	log := slices.Collect(md.PreviousFiles())
	if len(log) != 1 || log[0].MetadataFile != created.MetadataLocation {
		t.Fatalf("metadata log = %v, want the first file", log)
	}

	loaded, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil || loaded.MetadataLocation != updated.MetadataLocation {
		t.Fatalf("LoadTable = %q, %v; want the committed location", loaded.MetadataLocation, err)
	}
}

func TestCommitRequirementFailures(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	tests := map[string]string{
		"wrong uuid":         `[{"type":"assert-table-uuid","uuid":"99999999-9999-9999-9999-999999999999"}]`,
		"wrong schema":       `[{"type":"assert-current-schema-id","current-schema-id":7}]`,
		"table exists":       `[{"type":"assert-create"}]`,
		"ref must not exist": `[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":123}]`,
	}

	for name, reqs := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			_, err := f.commitJSON(t, "t", reqs, `[{"action":"set-properties","updates":{"k":"v"}}]`)
			if !errors.Is(err, catalog.ErrRequirementFailed) {
				t.Fatalf("commit = %v, want ErrRequirementFailed", err)
			}
		})
	}
}

func TestCommitInvalidUpdateWritesNothing(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)
	before := f.files.list()

	_, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-current-schema","schema-id":42}]`)
	if !errors.Is(err, catalog.ErrInvalidRequest) {
		t.Fatalf("commit = %v, want ErrInvalidRequest", err)
	}

	if !slices.Equal(f.files.list(), before) {
		t.Fatalf("files changed by a failed commit: %v -> %v", before, f.files.list())
	}
}

func TestCommitMissingTable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	_, err := f.commitJSON(t, "nope", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("commit = %v, want ErrTableNotFound", err)
	}

	createAndAssertSchema := `[{"type":"assert-create"},{"type":"assert-current-schema-id","current-schema-id":0}]`

	_, err = f.commitJSON(t, "nope", createAndAssertSchema, `[]`)
	if !errors.Is(err, catalog.ErrRequirementFailed) {
		t.Fatalf("create commit with a requirement on missing metadata = %v, want ErrRequirementFailed", err)
	}
}

func TestCommitRetriesConflicts(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 3)
	f.createTable(t, "t", nil)

	conflicts := 2
	f.store.setCommit(func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
		if conflicts > 0 {
			conflicts--

			return nil, &store.CommitConflictError{Ident: commits[0].Ident}
		}

		return f.store.Store.CommitTables(ctx, commits)
	})

	tbl, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if err != nil {
		t.Fatalf("commit after %d conflicts: %v", 2, err)
	}

	// The two losing attempts' files were cleaned up: only the first and the
	// committed metadata files remain.
	if files := metadataFiles(f.files.list()); len(files) != 2 || !slices.Contains(files, tbl.MetadataLocation) {
		t.Fatalf("metadata files = %v", files)
	}
}

func TestCommitGivesUpAfterRetries(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 2)
	f.createTable(t, "t", nil)

	attempts := 0
	f.store.setCommit(func(_ context.Context, commits []store.Commit) ([]store.Pointer, error) {
		attempts++

		return nil, &store.CommitConflictError{Ident: commits[0].Ident}
	})

	_, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if !errors.Is(err, store.ErrCommitConflict) {
		t.Fatalf("commit = %v, want ErrCommitConflict", err)
	}

	if attempts != 3 {
		t.Fatalf("attempts = %d, want 1 + 2 retries", attempts)
	}

	if files := metadataFiles(f.files.list()); len(files) != 1 {
		t.Fatalf("orphaned metadata files left behind: %v", files)
	}
}

// A concurrent commit that changes what the client asserted must make the
// retry fail instead of silently overwriting the other change.
func TestCommitRetryRechecksRequirements(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 3)
	f.createTable(t, "t", nil)

	addSchema := func(name string) string {
		return fmt.Sprintf(`[
			{"action":"add-schema","schema":{"type":"struct","schema-id":1,"fields":[
				{"id":1,"name":"id","required":true,"type":"long"},
				{"id":2,"name":"name","required":false,"type":"string"},
				{"id":3,"name":%q,"required":false,"type":"int"}]}},
			{"action":"set-current-schema","schema-id":-1}]`, name)
	}
	assertSchema0 := `[{"type":"assert-current-schema-id","current-schema-id":0}]`

	once := true
	f.store.setCommit(func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
		if once {
			once = false
			// Another writer gets in first.
			f.store.setCommit(nil)

			if _, err := f.commitJSON(t, "t", assertSchema0, addSchema("winner")); err != nil {
				return nil, fmt.Errorf("concurrent commit: %w", err)
			}
		}

		return f.store.Store.CommitTables(ctx, commits)
	})

	_, err := f.commitJSON(t, "t", assertSchema0, addSchema("loser"))
	if !errors.Is(err, catalog.ErrRequirementFailed) {
		t.Fatalf("losing commit = %v, want ErrRequirementFailed", err)
	}

	tbl, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := parseMetadata(t, tbl).CurrentSchema().FindFieldByName("winner"); !ok {
		t.Fatal("the winning commit was overwritten")
	}
}

func TestCommitStateUnknownKeepsFile(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 3)
	f.createTable(t, "t", nil)

	var written string

	f.store.setCommit(func(_ context.Context, commits []store.Commit) ([]store.Pointer, error) {
		written = commits[0].MetadataLocation

		return nil, errors.New("connection reset")
	})

	_, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if !errors.Is(err, catalog.ErrCommitStateUnknown) {
		t.Fatalf("commit = %v, want ErrCommitStateUnknown", err)
	}

	if !slices.Contains(f.files.list(), written) {
		t.Fatal("the metadata file of a commit in unknown state was deleted")
	}
}

func TestCommitConcurrentWritersAllLand(t *testing.T) {
	t.Parallel()

	const writers = 8

	f := newFixture(t, 50)
	f.createTable(t, "t", nil)

	errs := make(chan error, writers)

	for i := range writers {
		go func() {
			_, err := f.svc.CommitTable(t.Context(), catalog.CommitInput{
				Ident:   ident("t"),
				Updates: parseUpdates(t, fmt.Sprintf(`[{"action":"set-properties","updates":{"w%d":"x"}}]`, i)),
			})
			errs <- err
		}()
	}

	for range writers {
		if err := <-errs; err != nil {
			t.Fatalf("concurrent commit: %v", err)
		}
	}

	tbl, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil {
		t.Fatal(err)
	}

	props := parseMetadata(t, tbl).Properties()
	for i := range writers {
		if props[fmt.Sprintf("w%d", i)] != "x" {
			t.Fatalf("update of writer %d was lost: %v", i, props)
		}
	}
}

func TestCommitDeletesExpiredMetadata(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	first := f.createTable(t, "t", map[string]string{
		table.MetadataDeleteAfterCommitEnabledKey: "true",
		table.MetadataPreviousVersionsMaxKey:      "1",
	})

	var last catalog.Table

	for i := range 3 {
		var err error

		last, err = f.commitJSON(t, "t", `[]`, fmt.Sprintf(`[{"action":"set-properties","updates":{"n":"%d"}}]`, i))
		if err != nil {
			t.Fatal(err)
		}
	}

	files := metadataFiles(f.files.list())
	if slices.Contains(files, first.MetadataLocation) {
		t.Fatalf("expired metadata file kept: %v", files)
	}

	// The current file and the one kept in the log remain.
	if len(files) != 2 || !slices.Contains(files, last.MetadataLocation) {
		t.Fatalf("metadata files = %v, want current + 1 previous", files)
	}
}

func TestGzipMetadata(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", map[string]string{table.MetadataCompressionKey: "gzip"})

	if !strings.HasSuffix(created.MetadataLocation, ".gz.metadata.json") {
		t.Fatalf("metadata location %q, want a .gz.metadata.json file", created.MetadataLocation)
	}

	raw, _ := f.files.Read(t.Context(), created.MetadataLocation)
	if raw[0] != 0x1f || raw[1] != 0x8b {
		t.Fatal("metadata file is not gzip-compressed")
	}

	loaded, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil || string(loaded.MetadataJSON) != string(created.MetadataJSON) {
		t.Fatalf("LoadTable of a gzip file: %v", err)
	}

	if _, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`); err != nil {
		t.Fatalf("commit on a gzip table: %v", err)
	}
}

func TestLoadTableMissingMetadataFile(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)
	_ = f.files.Delete(t.Context(), created.MetadataLocation)

	if _, err := f.svc.LoadTable(t.Context(), ident("t")); !errors.Is(err, catalog.ErrMetadataMissing) {
		t.Fatalf("LoadTable = %v, want ErrMetadataMissing", err)
	}
}

func TestRenameAndDropTable(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)

	if err := f.svc.RenameTable(ctx, ident("t"), ident("renamed")); err != nil {
		t.Fatal(err)
	}

	tbl, err := f.svc.LoadTable(ctx, ident("renamed"))
	if err != nil || tbl.MetadataLocation != created.MetadataLocation {
		t.Fatalf("renamed table: %v", err)
	}

	if err := f.svc.DropTable(ctx, ident("renamed"), true); err != nil {
		t.Fatal(err)
	}

	if err := f.svc.TableExists(ctx, ident("renamed")); !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("TableExists after drop = %v", err)
	}

	// The drop only queues the purge; a purge worker deletes the files.
	if !slices.Contains(f.files.list(), created.MetadataLocation) {
		t.Fatal("drop deleted metadata files")
	}

	tasks, err := f.store.ListPurgeTasks(ctx, 10)
	if err != nil || len(tasks) != 1 || tasks[0].MetadataLocation != created.MetadataLocation {
		t.Fatalf("purge tasks = %+v (%v), want one for %q", tasks, err, created.MetadataLocation)
	}

	if err := f.svc.ReportMetrics(ctx, ident("renamed"), "scan-report"); !errors.Is(err, store.ErrTableNotFound) {
		t.Fatalf("ReportMetrics on a dropped table = %v", err)
	}
}

func TestNamespacePropertyConflict(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	_, err := f.svc.UpdateNamespaceProperties(t.Context(), store.Namespace{"db"}, store.PropertiesUpdate{
		Updates:  map[string]string{"k": "v"},
		Removals: []string{"k"},
	})
	if !errors.Is(err, catalog.ErrPropertyConflict) {
		t.Fatalf("UpdateNamespaceProperties = %v, want ErrPropertyConflict", err)
	}
}

func TestAll(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	f := newFixture(t, 0)

	for i := range store.MaxPageSize + 5 {
		if err := f.svc.CreateNamespace(ctx, store.Namespace{fmt.Sprintf("ns%04d", i)}, nil); err != nil {
			t.Fatal(err)
		}
	}

	all, err := catalog.All(func(req store.PageRequest) (store.Page[store.Namespace], error) {
		return f.svc.ListNamespaces(ctx, store.Namespace{}, req)
	})
	if err != nil {
		t.Fatal(err)
	}

	// The fixture's "db" namespace plus the ones created here.
	if len(all) != store.MaxPageSize+6 {
		t.Fatalf("All returned %d namespaces, want %d", len(all), store.MaxPageSize+6)
	}
}

// TestAppendFlow commits snapshots the way engines do on INSERT: add-snapshot
// plus set-snapshot-ref, guarded by assert-ref-snapshot-id.
func TestAppendFlow(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	appendSnapshot := func(id, parent, seq int64) string {
		parentField := ""
		if parent != 0 {
			parentField = fmt.Sprintf(`"parent-snapshot-id": %d,`, parent)
		}

		return fmt.Sprintf(`[
			{"action": "add-snapshot", "snapshot": {"snapshot-id": %d, %s "sequence-number": %d,
				"timestamp-ms": %d, "manifest-list": "mem://warehouse/db/t/metadata/snap-%d.avro",
				"summary": {"operation": "append"}, "schema-id": 0}},
			{"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": %d}
		]`, id, parentField, seq, time.Now().UnixMilli()+seq, id, id)
	}

	// First insert: main must not exist yet.
	first, err := f.commitJSON(t, "t", `[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}]`,
		appendSnapshot(101, 0, 1))
	if err != nil {
		t.Fatalf("first append: %v", err)
	}

	if snap := parseMetadata(t, first).CurrentSnapshot(); snap == nil || snap.SnapshotID != 101 {
		t.Fatalf("current snapshot after first append = %v", snap)
	}

	// Second insert on top of the first.
	second, err := f.commitJSON(t, "t", `[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":101}]`,
		appendSnapshot(102, 101, 2))
	if err != nil {
		t.Fatalf("second append: %v", err)
	}

	md := parseMetadata(t, second)
	if md.CurrentSnapshot().SnapshotID != 102 || len(md.Snapshots()) != 2 {
		t.Fatalf("after second append: current %d, %d snapshots", md.CurrentSnapshot().SnapshotID, len(md.Snapshots()))
	}

	// A writer that still believes main is at 101 must be rejected.
	_, err = f.commitJSON(t, "t", `[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":101}]`,
		appendSnapshot(103, 101, 3))
	if !errors.Is(err, catalog.ErrRequirementFailed) {
		t.Fatalf("stale append = %v, want ErrRequirementFailed", err)
	}
}
