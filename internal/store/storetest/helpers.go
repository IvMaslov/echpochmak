package storetest

import (
	"errors"
	"maps"
	"slices"
	"testing"

	"echpochmak/internal/store"
)

func ident(ns, name string) store.Ident {
	return store.Ident{Namespace: store.Namespace{ns}, Name: name}
}

func mustNoErr(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func mustErrIs(t *testing.T, err, target error) {
	t.Helper()

	if !errors.Is(err, target) {
		t.Fatalf("error = %v, want %v", err, target)
	}
}

func mustCreateNamespace(t *testing.T, s store.Store, ns store.Namespace, props map[string]string) {
	t.Helper()

	if err := s.CreateNamespace(t.Context(), ns, props); err != nil {
		t.Fatalf("CreateNamespace(%v): %v", ns, err)
	}
}

func mustCreateTable(t *testing.T, s store.Store, id store.Ident) store.Pointer {
	t.Helper()

	ps, err := s.CommitTables(t.Context(), []store.Commit{
		{Ident: id, MetadataLocation: "s3://warehouse/" + id.Name + "/v1.json"},
	})
	if err != nil {
		t.Fatalf("create table %v: %v", id, err)
	}

	return ps[0]
}

func mustCreateView(t *testing.T, s store.Store, id store.Ident) store.Pointer {
	t.Helper()

	p, err := s.CommitView(t.Context(), store.Commit{
		Ident:            id,
		MetadataLocation: "s3://warehouse/" + id.Name + "/v1.json",
	})
	if err != nil {
		t.Fatalf("create view %v: %v", id, err)
	}

	return p
}

func mustCommit(t *testing.T, ops pointerOps, s store.Store, c store.Commit) store.Pointer {
	t.Helper()

	p, err := ops.commit(t.Context(), s, c)
	if err != nil {
		t.Fatalf("commit %v: %v", c.Ident, err)
	}

	return p
}

func mustEqualStrings(t *testing.T, what string, got, want []string) {
	t.Helper()

	if !slices.Equal(got, want) {
		t.Fatalf("%s = %q, want %q", what, got, want)
	}
}

func mustEqualNamespaces(t *testing.T, got, want []store.Namespace) {
	t.Helper()

	if !slices.EqualFunc(got, want, store.Namespace.Equal) {
		t.Fatalf("namespaces = %v, want %v", got, want)
	}
}

func mapsEqual(a, b map[string]string) bool {
	return maps.Equal(a, b)
}

func identNames(ids []store.Ident) []string {
	names := make([]string, 0, len(ids))
	for _, id := range ids {
		names = append(names, id.Name)
	}

	return names
}

// collectIdents walks every page of a listing with the given page size and
// returns all names in order. It fails if any page is larger than size.
func collectIdents(
	t *testing.T,
	list func(store.PageRequest) (store.Page[store.Ident], error),
	size int,
) []string {
	t.Helper()

	names := []string{}
	req := store.PageRequest{Size: size}

	for range 100 {
		page, err := list(req)
		mustNoErr(t, err)

		if len(page.Items) > size {
			t.Fatalf("page has %d items, want at most %d", len(page.Items), size)
		}

		names = append(names, identNames(page.Items)...)

		if page.NextToken == "" {
			return names
		}

		req.Token = page.NextToken
	}

	t.Fatal("listing did not terminate after 100 pages")

	return nil
}
