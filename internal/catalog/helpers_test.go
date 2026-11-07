package catalog_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/google/uuid"

	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
)

const warehouse = "mem://warehouse"

// memFiles is an in-memory FileIO that tests can inspect.
type memFiles struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemFiles() *memFiles {
	return &memFiles{files: map[string][]byte{}}
}

func (m *memFiles) Read(_ context.Context, location string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, ok := m.files[location]
	if !ok {
		return nil, fmt.Errorf("%q: %w", location, fileio.ErrNotFound)
	}

	return data, nil
}

func (m *memFiles) Write(_ context.Context, location string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.files[location] = slices.Clone(data)

	return nil
}

func (m *memFiles) Delete(_ context.Context, location string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.files, location)

	return nil
}

// list returns the sorted locations of all files.
func (m *memFiles) list() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return slices.Sorted(maps.Keys(m.files))
}

// hookStore wraps a store and lets a test intercept CommitTables.
type hookStore struct {
	store.Store

	mu     sync.Mutex
	commit func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error)
}

func (h *hookStore) CommitTables(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
	h.mu.Lock()
	hook := h.commit
	h.mu.Unlock()

	if hook != nil {
		return hook(ctx, commits)
	}

	return h.Store.CommitTables(ctx, commits)
}

func (h *hookStore) setCommit(hook func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error)) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.commit = hook
}

type fixture struct {
	svc   *catalog.Service
	store *hookStore
	files *memFiles
}

func newFixture(t *testing.T, retries int) *fixture {
	t.Helper()

	st := &hookStore{Store: memstore.New()}
	files := newMemFiles()

	var (
		mu sync.Mutex
		n  int
	)

	svc := catalog.New(catalog.Options{
		Store:         st,
		Files:         files,
		Warehouse:     warehouse + "/",
		CommitRetries: retries,
		NewUUID: func() uuid.UUID {
			mu.Lock()
			defer mu.Unlock()

			n++

			return uuid.MustParse(fmt.Sprintf("00000000-0000-0000-0000-%012d", n))
		},
		Backoff: func(int) time.Duration { return 0 },
		Logger:  slog.New(slog.DiscardHandler),
	})

	if err := svc.CreateNamespace(t.Context(), store.Namespace{"db"}, nil); err != nil {
		t.Fatalf("CreateNamespace: %v", err)
	}

	return &fixture{svc: svc, store: st, files: files}
}

func ident(name string) store.Ident {
	return store.Ident{Namespace: store.Namespace{"db"}, Name: name}
}

func testSchema() *iceberg.Schema {
	return iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String},
	)
}

func (f *fixture) createTable(t *testing.T, name string, props map[string]string) catalog.Table {
	t.Helper()

	tbl, err := f.svc.CreateTable(t.Context(), catalog.CreateTableInput{
		Ident:      ident(name),
		Schema:     testSchema(),
		Properties: props,
	})
	if err != nil {
		t.Fatalf("CreateTable(%s): %v", name, err)
	}

	return tbl
}

// commitJSON commits requirements and updates given as the JSON a client
// would send.
func (f *fixture) commitJSON(t *testing.T, name, requirements, updates string) (catalog.Table, error) {
	t.Helper()

	return f.svc.CommitTable(t.Context(), catalog.CommitInput{
		Ident:        ident(name),
		Requirements: parseRequirements(t, requirements),
		Updates:      parseUpdates(t, updates),
	})
}

func parseRequirements(t *testing.T, s string) []table.Requirement {
	t.Helper()

	var reqs table.Requirements
	if err := json.Unmarshal([]byte(s), &reqs); err != nil {
		t.Fatalf("parsing requirements: %v", err)
	}

	return reqs
}

func parseUpdates(t *testing.T, s string) []table.Update {
	t.Helper()

	var upds table.Updates
	if err := json.Unmarshal([]byte(s), &upds); err != nil {
		t.Fatalf("parsing updates: %v", err)
	}

	return upds
}

func parseMetadata(t *testing.T, tbl catalog.Table) table.Metadata {
	t.Helper()

	md, err := table.ParseMetadataBytes(tbl.MetadataJSON)
	if err != nil {
		t.Fatalf("parsing metadata of %v: %v", tbl.Ident, err)
	}

	return md
}

func metadataFiles(files []string) []string {
	out := []string{}

	for _, f := range files {
		if strings.Contains(f, "/metadata/") {
			out = append(out, f)
		}
	}

	return out
}
