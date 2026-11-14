package conformance_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go/view"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
)

// purgeServer is a catalog server with the purge worker of its replica,
// which tests run on demand.
type purgeServer struct {
	uri    string
	files  fileio.FileIO
	purger *catalog.Purger
}

func newPurgeServer(t *testing.T, newStore storeFactory) purgeServer {
	t.Helper()

	st := newStore(t)
	log := slog.New(slog.DiscardHandler)
	files := fileio.NewIceberg(context.Background(), nil)
	svc := catalog.New(catalog.Options{
		Store:         st,
		Files:         files,
		Warehouse:     "mem://conformance/" + strings.ReplaceAll(t.Name(), "/", "_"),
		CommitRetries: catalog.DefaultCommitRetries,
		Logger:        log,
	})

	srv := httptest.NewServer(api.NewHandler(api.Options{Logger: log, Ready: svc, Server: api.NewServer(svc, "main")}))
	t.Cleanup(srv.Close)

	return purgeServer{
		uri:    srv.URL,
		files:  files,
		purger: catalog.NewPurger(catalog.PurgerOptions{Store: st, Files: files, Owner: "test", Logger: log}),
	}
}

// TestPurgeDeletesEveryFile drops a table with data through iceberg-go's
// PurgeTable; the purge worker then deletes its data files, manifests,
// manifest lists and metadata files.
func TestPurgeDeletesEveryFile(t *testing.T) {
	t.Parallel()
	forEachBackend(t, testPurgeDeletesEveryFile)
}

func testPurgeDeletesEveryFile(t *testing.T, newStore storeFactory) {
	t.Helper()

	ctx := t.Context()
	srv := newPurgeServer(t, newStore)
	cat := newClient(t, srv.uri)
	ident := table.Identifier{"db", "doomed"}

	if err := cat.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatal(err)
	}

	tbl, err := cat.CreateTable(ctx, ident, dataSchema)
	if err != nil {
		t.Fatal(err)
	}

	for i := range 2 {
		data := rows(t, "w", 5+i)

		tbl, err = tbl.AppendTable(ctx, data, 1024, nil)
		data.Release()

		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	files := tableFiles(t, tbl)
	if len(files) < 8 {
		t.Fatalf("collected only %d files: %q", len(files), files)
	}

	if err := cat.PurgeTable(ctx, ident); err != nil {
		t.Fatalf("PurgeTable: %v", err)
	}

	srv.purger.Scan(ctx)
	srv.purger.Wait()

	for _, location := range files {
		if _, err := srv.files.Read(ctx, location); !errors.Is(err, fileio.ErrNotFound) {
			t.Errorf("file %q survived the purge (read error %v)", location, err)
		}
	}
}

// tableFiles lists every file of tbl: data files, manifests, manifest lists
// and metadata files.
func tableFiles(t *testing.T, tbl *table.Table) []string {
	t.Helper()

	fs, err := tbl.FS(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	files := []string{tbl.MetadataLocation()}

	for entry := range tbl.Metadata().PreviousFiles() {
		files = append(files, entry.MetadataFile)
	}

	for _, snap := range tbl.Metadata().Snapshots() {
		files = append(files, snap.ManifestList)

		manifests, err := snap.Manifests(fs)
		if err != nil {
			t.Fatal(err)
		}

		for _, m := range manifests {
			files = append(files, m.FilePath())

			for e, err := range m.Entries(fs, false) {
				if err != nil {
					t.Fatal(err)
				}

				files = append(files, e.DataFile().FilePath())
			}
		}
	}

	return files
}

// TestMultiTableTransaction appends to two tables and commits both appends
// with one call to the transactions endpoint.
func TestMultiTableTransaction(t *testing.T) {
	t.Parallel()
	forEachBackend(t, testMultiTableTransaction)
}

func testMultiTableTransaction(t *testing.T, newStore storeFactory) {
	t.Helper()

	ctx := t.Context()
	cat := newClient(t, newServer(t, newStore(t)))

	if err := cat.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatal(err)
	}

	idents := []table.Identifier{{"db", "orders"}, {"db", "audit"}}
	commits := make([]table.TableCommit, 0, len(idents))

	for _, id := range idents {
		tbl, err := cat.CreateTable(ctx, id, dataSchema)
		if err != nil {
			t.Fatal(err)
		}

		tx := tbl.NewTransaction()
		data := rows(t, id[1], 4)

		if err := tx.AppendTable(ctx, data, 1024, nil); err != nil {
			t.Fatal(err)
		}

		data.Release()

		commit, err := tx.TableCommit()
		if err != nil {
			t.Fatal(err)
		}

		commits = append(commits, commit)
	}

	if err := cat.CommitTransaction(ctx, commits); err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	for _, id := range idents {
		tbl, err := cat.LoadTable(ctx, id)
		if err != nil {
			t.Fatal(err)
		}

		if got := countRows(t, tbl); got[id[1]] != 4 {
			t.Fatalf("%v rows = %v, want 4 of %s", id, got, id[1])
		}
	}

	// The same commits again: their requirements no longer hold, and neither
	// table changes.
	if err := cat.CommitTransaction(ctx, commits); err == nil {
		t.Fatal("a stale transaction was committed")
	}
}

// TestViewsThroughClient runs the view lifecycle through iceberg-go.
func TestViewsThroughClient(t *testing.T) {
	t.Parallel()
	forEachBackend(t, testViewsThroughClient)
}

func testViewsThroughClient(t *testing.T, newStore storeFactory) {
	t.Helper()

	ctx := t.Context()
	cat := newClient(t, newServer(t, newStore(t)))
	ident := table.Identifier{"db", "recent"}

	if err := cat.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatal(err)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64})

	version, err := view.NewVersionFromSQL(1, 0, "SELECT id FROM db.events", table.Identifier{"db"})
	if err != nil {
		t.Fatal(err)
	}

	created, err := cat.CreateView(ctx, ident, version, schema)
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}

	loaded, err := cat.LoadView(ctx, ident)
	if err != nil || loaded.MetadataLocation() != created.MetadataLocation() {
		t.Fatalf("LoadView = %v, %v", loaded, err)
	}

	if sql := loaded.CurrentVersion().Representations[0].Sql; sql != "SELECT id FROM db.events" {
		t.Fatalf("view SQL %q", sql)
	}

	if ok, err := cat.CheckViewExists(ctx, ident); err != nil || !ok {
		t.Fatalf("CheckViewExists = %v, %v", ok, err)
	}

	renamed := table.Identifier{"db", "latest"}
	if _, err := cat.RenameView(ctx, ident, renamed); err != nil {
		t.Fatalf("RenameView: %v", err)
	}

	if err := cat.DropView(ctx, renamed); err != nil {
		t.Fatalf("DropView: %v", err)
	}

	if ok, err := cat.CheckViewExists(ctx, renamed); err != nil || ok {
		t.Fatalf("CheckViewExists after drop = %v, %v", ok, err)
	}
}

// TestRegisterThroughClient drops a table without purge and registers its
// metadata again under another name.
func TestRegisterThroughClient(t *testing.T) {
	t.Parallel()
	forEachBackend(t, testRegisterThroughClient)
}

func testRegisterThroughClient(t *testing.T, newStore storeFactory) {
	t.Helper()

	ctx := t.Context()
	cat := newClient(t, newServer(t, newStore(t)))

	if err := cat.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatal(err)
	}

	tbl, err := cat.CreateTable(ctx, table.Identifier{"db", "old"}, dataSchema)
	if err != nil {
		t.Fatal(err)
	}

	data := rows(t, "kept", 3)

	tbl, err = tbl.AppendTable(ctx, data, 1024, nil)
	data.Release()

	if err != nil {
		t.Fatal(err)
	}

	if err := cat.DropTable(ctx, table.Identifier{"db", "old"}); err != nil {
		t.Fatal(err)
	}

	registered, err := cat.RegisterTable(ctx, table.Identifier{"db", "new"}, tbl.MetadataLocation())
	if err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	if got := countRows(t, registered); got["kept"] != 3 {
		t.Fatalf("rows of the registered table = %v, want 3", got)
	}
}
