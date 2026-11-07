// Package conformance tests the catalog server end to end, in process,
// through iceberg-go's REST client: the same code path a Go engine takes.
// Metadata and data files live in iceberg-go's in-memory file system, so no
// object storage is needed.
package conformance_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/catalogtest"
	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/store/memstore"
)

// newServer starts a catalog server with an empty store and returns its URL.
func newServer(t *testing.T) string {
	t.Helper()

	log := slog.New(slog.DiscardHandler)
	svc := catalog.New(catalog.Options{
		Store:         memstore.New(),
		Files:         fileio.NewIceberg(context.Background(), nil),
		Warehouse:     "mem://conformance/" + strings.ReplaceAll(t.Name(), "/", "_"),
		CommitRetries: catalog.DefaultCommitRetries,
		Logger:        log,
	})

	srv := httptest.NewServer(api.NewHandler(api.Options{
		Logger: log,
		Ready:  svc,
		Server: api.NewServer(svc, "main"),
	}))
	t.Cleanup(srv.Close)

	return srv.URL
}

func newClient(t *testing.T, uri string) *rest.Catalog {
	t.Helper()

	cat, err := rest.NewCatalog(t.Context(), "echpochmak", uri)
	if err != nil {
		t.Fatalf("connecting the REST client: %v", err)
	}

	return cat
}

func TestCatalogConformance(t *testing.T) {
	t.Parallel()

	catalogtest.RunCatalogTests(t, catalogtest.Config{
		NewCatalog: func(t *testing.T) icebergcatalog.Catalog {
			t.Helper()

			return newClient(t, newServer(t))
		},
		SupportsNamespaceProperties: true,
	})
}

var dataSchema = iceberg.NewSchema(0,
	iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
	iceberg.NestedField{ID: 2, Name: "writer", Type: iceberg.PrimitiveTypes.String, Required: true},
)

// rows builds an Arrow table of n rows written by writer.
func rows(t *testing.T, writer string, n int) arrow.Table {
	t.Helper()

	sc, err := table.SchemaToArrowSchema(dataSchema, nil, false, false)
	if err != nil {
		t.Fatal(err)
	}

	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()

	ids, okID := b.Field(0).(*array.Int64Builder)
	writers, okWriter := b.Field(1).(*array.StringBuilder)

	if !okID || !okWriter {
		t.Fatal("unexpected Arrow builder types")
	}

	for i := range n {
		ids.Append(int64(i))
		writers.Append(writer)
	}

	rec := b.NewRecordBatch()
	defer rec.Release()

	return array.NewTableFromRecords(sc, []arrow.RecordBatch{rec})
}

func countRows(t *testing.T, tbl *table.Table) map[string]int64 {
	t.Helper()

	data, err := tbl.Scan().ToArrowTable(t.Context())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer data.Release()

	counts := map[string]int64{}
	rdr := array.NewTableReader(data, 0)

	defer rdr.Release()

	for rdr.Next() {
		col, ok := rdr.RecordBatch().Column(1).(*array.String)
		if !ok {
			t.Fatalf("writer column is %T", rdr.RecordBatch().Column(1))
		}

		for i := range col.Len() {
			counts[col.Value(i)]++
		}
	}

	return counts
}

// TestAppendAndScan writes data through the catalog and reads it back, like
// an engine running INSERT and SELECT.
func TestAppendAndScan(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	cat := newClient(t, newServer(t))
	ident := table.Identifier{"db", "events"}

	if err := cat.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	tbl, err := cat.CreateTable(ctx, ident, dataSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i, n := range []int{3, 5} {
		data := rows(t, fmt.Sprintf("batch%d", i), n)

		tbl, err = tbl.AppendTable(ctx, data, 1024, nil)
		data.Release()

		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	reloaded, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}

	if got := countRows(t, reloaded); got["batch0"] != 3 || got["batch1"] != 5 {
		t.Fatalf("rows by batch = %v, want batch0=3 batch1=5", got)
	}

	if n := len(reloaded.Metadata().Snapshots()); n != 2 {
		t.Fatalf("snapshots = %d, want 2", n)
	}
}

// TestConcurrentAppends runs writers that each append to the same table from
// their own client. Every append must land: conflicts are resolved by the
// server's retries or the client's, never by losing data.
func TestConcurrentAppends(t *testing.T) {
	t.Parallel()

	const writers = 4

	ctx := t.Context()
	uri := newServer(t)
	ident := table.Identifier{"db", "shared"}

	setup := newClient(t, uri)
	if err := setup.CreateNamespace(ctx, table.Identifier{"db"}, nil); err != nil {
		t.Fatal(err)
	}

	// Losing writers must refresh and retry on the client: their requirement
	// (the branch head they started from) no longer holds, so the server
	// rightly refuses to retry for them. iceberg-go retries only when the
	// table asks for it; Java clients retry 4 times by default.
	props := iceberg.Properties{table.CommitNumRetriesKey: "20", table.CommitMinRetryWaitMsKey: "1"}
	if _, err := setup.CreateTable(ctx, ident, dataSchema, icebergcatalog.WithProperties(props)); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup

	errs := make(chan error, writers)

	for w := range writers {
		wg.Go(func() {
			cat := newClient(t, uri)

			tbl, err := cat.LoadTable(ctx, ident)
			if err != nil {
				errs <- err

				return
			}

			data := rows(t, fmt.Sprintf("w%d", w), 10)
			defer data.Release()

			if _, err := tbl.AppendTable(ctx, data, 1024, nil); err != nil {
				errs <- fmt.Errorf("writer %d: %w", w, err)
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("append: %v", err)
	}

	tbl, err := setup.LoadTable(ctx, ident)
	if err != nil {
		t.Fatal(err)
	}

	counts := countRows(t, tbl)
	for w := range writers {
		if counts[fmt.Sprintf("w%d", w)] != 10 {
			t.Fatalf("rows by writer = %v, want 10 each", counts)
		}
	}
}
