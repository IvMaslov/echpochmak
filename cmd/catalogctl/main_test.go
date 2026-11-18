package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/apache/iceberg-go"

	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/store"
	"echpochmak/internal/store/raftstore"
)

func TestRestoreReport(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	st := raftstore.New(c.Client(), raftstore.Options{})
	warehouse := "mem://catalogctl-test"

	svc := catalog.New(catalog.Options{
		Store:     st,
		Files:     fileio.NewIceberg(t.Context(), nil),
		Warehouse: warehouse,
		Logger:    slog.New(slog.DiscardHandler),
	})

	ctx := t.Context()
	id := store.Ident{Namespace: store.Namespace{"db"}, Name: "t"}

	if err := svc.CreateNamespace(ctx, id.Namespace, nil); err != nil {
		t.Fatal(err)
	}

	schema := iceberg.NewSchema(0, iceberg.NestedField{ID: 1, Name: "a", Type: iceberg.PrimitiveTypes.Int64})

	created, err := svc.CreateTable(ctx, catalog.CreateTableInput{Ident: id, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}

	other := store.Ident{Namespace: id.Namespace, Name: "u"}
	if _, err := svc.CreateTable(ctx, catalog.CreateTableInput{Ident: other, Schema: schema}); err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer

	args := []string{
		"-store-backend", "raft",
		"-kv-endpoints", strings.Join(c.Endpoints(), ","),
		"-s3-warehouse", warehouse + "/",
		"restore-report", "-json",
	}
	if err := run(ctx, args, func(string) string { return "" }, &out); err != nil {
		t.Fatalf("restore-report: %v", err)
	}

	if out.Len() != 0 {
		t.Fatalf("a current catalog reported %q, want nothing", out.String())
	}

	// Delete t's file: the report must name it.
	if err := fileio.NewIceberg(ctx, nil).Delete(ctx, created.MetadataLocation); err != nil {
		t.Fatal(err)
	}

	out.Reset()

	if err := run(ctx, args, func(string) string { return "" }, &out); err != nil {
		t.Fatalf("restore-report: %v", err)
	}

	var finding catalog.Finding
	if err := json.Unmarshal(out.Bytes(), &finding); err != nil {
		t.Fatalf("output %q: %v", out.String(), err)
	}

	if finding.Kind != catalog.FindingMissingMetadata || finding.Table != "db.t" {
		t.Fatalf("finding = %+v, want missing-metadata for db.t", finding)
	}
}

func TestParseTables(t *testing.T) {
	t.Parallel()

	ids, err := parseTables("db.t, a.b.c")
	if err != nil || len(ids) != 2 || ids[1].Name != "c" || len(ids[1].Namespace) != 2 {
		t.Fatalf("parseTables = %v, %v", ids, err)
	}

	if _, err := parseTables("lonely"); err == nil {
		t.Fatal("a name without a namespace was accepted")
	}
}
