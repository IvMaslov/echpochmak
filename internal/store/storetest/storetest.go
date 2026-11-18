// Package storetest is the contract test suite for [store.Store]. Every
// implementation runs it from its own tests:
//
//	func TestContract(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) store.Store { return memstore.New() })
//	}
//
// The suite is the specification of the store semantics: when the contract
// changes, change the tests here first.
package storetest

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	"echpochmak/internal/store"
)

// Factory returns a new, empty store. It is called once per subtest.
type Factory func(t *testing.T) store.Store

// Run runs the whole contract suite against stores returned by newStore.
func Run(t *testing.T, newStore Factory) {
	t.Helper()

	t.Run("Namespaces", func(t *testing.T) { testNamespaces(t, newStore) })
	t.Run("NamespaceProperties", func(t *testing.T) { testNamespaceProperties(t, newStore) })
	t.Run("Pagination", func(t *testing.T) { testPagination(t, newStore) })
	t.Run("Tables", func(t *testing.T) { testPointers(t, newStore, tableOps()) })
	t.Run("Views", func(t *testing.T) { testPointers(t, newStore, viewOps()) })
	t.Run("TableViewNameCollision", func(t *testing.T) { testNameCollision(t, newStore) })
	t.Run("MultiTableCommit", func(t *testing.T) { testMultiTableCommit(t, newStore) })
	t.Run("ConcurrentCommits", func(t *testing.T) { testConcurrentCommits(t, newStore) })
	t.Run("Isolation", func(t *testing.T) { testIsolation(t, newStore) })
	t.Run("CancelledContext", func(t *testing.T) { testCancelledContext(t, newStore) })
	t.Run("InvalidArguments", func(t *testing.T) { testInvalidArguments(t, newStore) })
	t.Run("GetTables", func(t *testing.T) { testGetTables(t, newStore) })
	t.Run("DropTable", func(t *testing.T) { testDropTable(t, newStore) })
	t.Run("PurgeQueue", func(t *testing.T) { testPurgeQueue(t, newStore) })
	t.Run("Intents", func(t *testing.T) { testIntents(t, newStore) })
}

func testNamespaces(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("lifecycle", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)
		ns := store.Namespace{"db"}

		mustCreateNamespace(t, s, ns, map[string]string{"owner": "alice"})

		rec, err := s.GetNamespace(ctx, ns)
		mustNoErr(t, err)

		if !rec.Namespace.Equal(ns) || rec.Properties["owner"] != "alice" || rec.Version <= 0 {
			t.Fatalf("GetNamespace = %+v, want namespace %v with owner=alice and a positive version", rec, ns)
		}

		mustNoErr(t, s.DropNamespace(ctx, ns))
		_, err = s.GetNamespace(ctx, ns)
		mustErrIs(t, err, store.ErrNamespaceNotFound)
	})

	t.Run("nil properties read back as empty map", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		rec, err := s.GetNamespace(t.Context(), store.Namespace{"db"})
		mustNoErr(t, err)

		if rec.Properties == nil {
			t.Fatal("Properties is nil, want empty map")
		}
	})

	t.Run("create existing", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		err := s.CreateNamespace(t.Context(), store.Namespace{"db"}, nil)
		mustErrIs(t, err, store.ErrNamespaceAlreadyExists)
	})

	t.Run("nested requires parent", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)

		err := s.CreateNamespace(t.Context(), store.Namespace{"a", "b"}, nil)
		mustErrIs(t, err, store.ErrNamespaceNotFound)

		mustCreateNamespace(t, s, store.Namespace{"a"}, nil)
		mustCreateNamespace(t, s, store.Namespace{"a", "b"}, nil)
	})

	t.Run("drop missing", func(t *testing.T) {
		t.Parallel()

		err := newStore(t).DropNamespace(t.Context(), store.Namespace{"nope"})
		mustErrIs(t, err, store.ErrNamespaceNotFound)
	})

	t.Run("drop non-empty", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			populate func(t *testing.T, s store.Store)
		}{
			{
				name: "child namespace",
				populate: func(t *testing.T, s store.Store) {
					t.Helper()
					mustCreateNamespace(t, s, store.Namespace{"db", "child"}, nil)
				},
			},
			{
				name: "table",
				populate: func(t *testing.T, s store.Store) {
					t.Helper()
					mustCreateTable(t, s, ident("db", "t"))
				},
			},
			{
				name: "view",
				populate: func(t *testing.T, s store.Store) {
					t.Helper()
					mustCreateView(t, s, ident("db", "v"))
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				s := newStore(t)
				mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
				tt.populate(t, s)

				err := s.DropNamespace(t.Context(), store.Namespace{"db"})
				mustErrIs(t, err, store.ErrNamespaceNotEmpty)
			})
		}
	})

	t.Run("list direct children only", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)

		for _, ns := range []store.Namespace{{"b"}, {"a"}, {"a", "y"}, {"a", "x"}, {"a", "x", "deep"}} {
			mustCreateNamespace(t, s, ns, nil)
		}

		top, err := s.ListNamespaces(ctx, store.Namespace{}, store.PageRequest{})
		mustNoErr(t, err)
		mustEqualNamespaces(t, top.Items, []store.Namespace{{"a"}, {"b"}})

		nested, err := s.ListNamespaces(ctx, store.Namespace{"a"}, store.PageRequest{})
		mustNoErr(t, err)
		mustEqualNamespaces(t, nested.Items, []store.Namespace{{"a", "x"}, {"a", "y"}})

		empty, err := s.ListNamespaces(ctx, store.Namespace{"b"}, store.PageRequest{})
		mustNoErr(t, err)

		if empty.Items == nil || len(empty.Items) != 0 || empty.NextToken != "" {
			t.Fatalf("ListNamespaces(b) = %+v, want empty non-nil page", empty)
		}

		_, err = s.ListNamespaces(ctx, store.Namespace{"missing"}, store.PageRequest{})
		mustErrIs(t, err, store.ErrNamespaceNotFound)
	})
}

func testNamespaceProperties(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("update and remove", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)
		ns := store.Namespace{"db"}
		mustCreateNamespace(t, s, ns, map[string]string{"a": "1", "b": "2"})

		before, err := s.GetNamespace(ctx, ns)
		mustNoErr(t, err)

		res, err := s.UpdateNamespaceProperties(ctx, ns, store.PropertiesUpdate{
			Updates:  map[string]string{"c": "3", "a": "10"},
			Removals: []string{"b", "missing"},
		})
		mustNoErr(t, err)

		mustEqualStrings(t, "Updated", res.Updated, []string{"a", "c"})
		mustEqualStrings(t, "Removed", res.Removed, []string{"b"})
		mustEqualStrings(t, "Missing", res.Missing, []string{"missing"})

		after, err := s.GetNamespace(ctx, ns)
		mustNoErr(t, err)

		want := map[string]string{"a": "10", "c": "3"}
		if !mapsEqual(after.Properties, want) {
			t.Fatalf("Properties = %v, want %v", after.Properties, want)
		}

		if after.Version <= before.Version {
			t.Fatalf("Version = %d after update, want > %d", after.Version, before.Version)
		}
	})

	t.Run("empty result slices are not nil", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		res, err := s.UpdateNamespaceProperties(t.Context(), store.Namespace{"db"}, store.PropertiesUpdate{})
		mustNoErr(t, err)

		if res.Updated == nil || res.Removed == nil || res.Missing == nil {
			t.Fatalf("result = %+v, want non-nil slices", res)
		}
	})

	t.Run("key both updated and removed", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		_, err := s.UpdateNamespaceProperties(t.Context(), store.Namespace{"db"}, store.PropertiesUpdate{
			Updates:  map[string]string{"k": "v"},
			Removals: []string{"k"},
		})
		mustErrIs(t, err, store.ErrInvalidArgument)
	})

	t.Run("missing namespace", func(t *testing.T) {
		t.Parallel()

		_, err := newStore(t).UpdateNamespaceProperties(
			t.Context(),
			store.Namespace{"nope"},
			store.PropertiesUpdate{},
		)
		mustErrIs(t, err, store.ErrNamespaceNotFound)
	})
}

func testPagination(t *testing.T, newStore Factory) {
	t.Parallel()

	names := []string{"a", "b", "c", "d", "e"}

	for _, size := range []int{1, 2, 3, 5, 10} {
		t.Run(fmt.Sprintf("tables page size %d", size), func(t *testing.T) {
			t.Parallel()

			s := newStore(t)
			mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

			for _, name := range slices.Backward(names) {
				mustCreateTable(t, s, ident("db", name))
			}

			got := collectIdents(t, func(req store.PageRequest) (store.Page[store.Ident], error) {
				return s.ListTables(t.Context(), store.Namespace{"db"}, req)
			}, size)
			mustEqualStrings(t, "tables", got, names)
		})
	}

	t.Run("namespaces across pages", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)

		for _, name := range names {
			mustCreateNamespace(t, s, store.Namespace{name}, nil)
		}

		var got []string

		req := store.PageRequest{Size: 2}

		for {
			page, err := s.ListNamespaces(ctx, store.Namespace{}, req)
			mustNoErr(t, err)

			for _, ns := range page.Items {
				got = append(got, ns.Name())
			}

			if page.NextToken == "" {
				break
			}

			req.Token = page.NextToken
		}

		mustEqualStrings(t, "namespaces", got, names)
	})

	t.Run("stable under concurrent inserts", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		for _, name := range []string{"b", "d", "f"} {
			mustCreateTable(t, s, ident("db", name))
		}

		first, err := s.ListTables(ctx, store.Namespace{"db"}, store.PageRequest{Size: 2})
		mustNoErr(t, err)
		mustEqualStrings(t, "first page", identNames(first.Items), []string{"b", "d"})

		// "a" sorts before the token and must not show up; "e" sorts after
		// it and must.
		mustCreateTable(t, s, ident("db", "a"))
		mustCreateTable(t, s, ident("db", "e"))

		second, err := s.ListTables(ctx, store.Namespace{"db"}, store.PageRequest{Token: first.NextToken, Size: 10})
		mustNoErr(t, err)
		mustEqualStrings(t, "second page", identNames(second.Items), []string{"e", "f"})

		if second.NextToken != "" {
			t.Fatalf("NextToken = %q on last page, want empty", second.NextToken)
		}
	})

	t.Run("malformed token", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		_, err := s.ListTables(t.Context(), store.Namespace{"db"}, store.PageRequest{Token: "!!not-a-token!!"})
		mustErrIs(t, err, store.ErrInvalidArgument)
	})
}

// pointerOps abstracts over tables and views, which share most semantics.
type pointerOps struct {
	errNotFound      error
	errAlreadyExists error
	get              func(ctx context.Context, s store.Store, id store.Ident) (store.Pointer, error)
	list             func(ctx context.Context, s store.Store, ns store.Namespace) (store.Page[store.Ident], error)
	commit           func(ctx context.Context, s store.Store, c store.Commit) (store.Pointer, error)
	rename           func(ctx context.Context, s store.Store, from, to store.Ident) error
	drop             func(ctx context.Context, s store.Store, id store.Ident) error
}

func tableOps() pointerOps {
	return pointerOps{
		errNotFound:      store.ErrTableNotFound,
		errAlreadyExists: store.ErrTableAlreadyExists,
		get: func(ctx context.Context, s store.Store, id store.Ident) (store.Pointer, error) {
			return s.GetTable(ctx, id)
		},
		list: func(ctx context.Context, s store.Store, ns store.Namespace) (store.Page[store.Ident], error) {
			return s.ListTables(ctx, ns, store.PageRequest{})
		},
		commit: func(ctx context.Context, s store.Store, c store.Commit) (store.Pointer, error) {
			ps, err := s.CommitTables(ctx, []store.Commit{c})
			if err != nil {
				return store.Pointer{}, err
			}

			return ps[0], nil
		},
		rename: func(ctx context.Context, s store.Store, from, to store.Ident) error {
			return s.RenameTable(ctx, from, to)
		},
		drop: func(ctx context.Context, s store.Store, id store.Ident) error {
			_, err := s.DropTable(ctx, id, store.DropOptions{})

			return err
		},
	}
}

func viewOps() pointerOps {
	return pointerOps{
		errNotFound:      store.ErrViewNotFound,
		errAlreadyExists: store.ErrViewAlreadyExists,
		get: func(ctx context.Context, s store.Store, id store.Ident) (store.Pointer, error) {
			return s.GetView(ctx, id)
		},
		list: func(ctx context.Context, s store.Store, ns store.Namespace) (store.Page[store.Ident], error) {
			return s.ListViews(ctx, ns, store.PageRequest{})
		},
		commit: func(ctx context.Context, s store.Store, c store.Commit) (store.Pointer, error) {
			return s.CommitView(ctx, c)
		},
		rename: func(ctx context.Context, s store.Store, from, to store.Ident) error {
			return s.RenameView(ctx, from, to)
		},
		drop: func(ctx context.Context, s store.Store, id store.Ident) error {
			return s.DropView(ctx, id)
		},
	}
}

func testPointers(t *testing.T, newStore Factory, ops pointerOps) {
	t.Parallel()

	setup := func(t *testing.T) store.Store {
		t.Helper()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		mustCreateNamespace(t, s, store.Namespace{"other"}, nil)

		return s
	}

	t.Run("create get commit drop", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := setup(t)
		id := ident("db", "t")

		created, err := ops.commit(ctx, s, store.Commit{Ident: id, MetadataLocation: "s3://w/v1.json"})
		mustNoErr(t, err)

		got, err := ops.get(ctx, s, id)
		mustNoErr(t, err)

		if !got.Ident.Equal(id) || got.MetadataLocation != "s3://w/v1.json" || got.Version != created.Version {
			t.Fatalf("get = %+v, want %+v", got, created)
		}

		updated, err := ops.commit(ctx, s, store.Commit{
			Ident:            id,
			ExpectedVersion:  created.Version,
			MetadataLocation: "s3://w/v2.json",
		})
		mustNoErr(t, err)

		if updated.Version <= created.Version || updated.MetadataLocation != "s3://w/v2.json" {
			t.Fatalf("commit = %+v, want location v2 and version > %d", updated, created.Version)
		}

		page, err := ops.list(ctx, s, store.Namespace{"db"})
		mustNoErr(t, err)
		mustEqualStrings(t, "list", identNames(page.Items), []string{"t"})

		mustNoErr(t, ops.drop(ctx, s, id))
		_, err = ops.get(ctx, s, id)
		mustErrIs(t, err, ops.errNotFound)
	})

	t.Run("create existing", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := setup(t)
		id := ident("db", "t")
		mustCommit(t, ops, s, store.Commit{Ident: id, MetadataLocation: "s3://w/v1.json"})

		_, err := ops.commit(ctx, s, store.Commit{Ident: id, MetadataLocation: "s3://w/other.json"})
		mustErrIs(t, err, ops.errAlreadyExists)
	})

	t.Run("create in missing namespace", func(t *testing.T) {
		t.Parallel()

		_, err := ops.commit(t.Context(), setup(t), store.Commit{
			Ident:            ident("missing", "t"),
			MetadataLocation: "s3://w/v1.json",
		})
		mustErrIs(t, err, store.ErrNamespaceNotFound)
	})

	t.Run("commit to missing entry", func(t *testing.T) {
		t.Parallel()

		_, err := ops.commit(t.Context(), setup(t), store.Commit{
			Ident:            ident("db", "t"),
			ExpectedVersion:  1,
			MetadataLocation: "s3://w/v2.json",
		})
		mustErrIs(t, err, ops.errNotFound)
	})

	t.Run("stale version conflicts", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := setup(t)
		id := ident("db", "t")
		v1 := mustCommit(t, ops, s, store.Commit{Ident: id, MetadataLocation: "s3://w/v1.json"})
		v2 := mustCommit(t, ops, s, store.Commit{
			Ident:            id,
			ExpectedVersion:  v1.Version,
			MetadataLocation: "s3://w/v2.json",
		})

		_, err := ops.commit(ctx, s, store.Commit{
			Ident:            id,
			ExpectedVersion:  v1.Version,
			MetadataLocation: "s3://w/v3.json",
		})
		mustErrIs(t, err, store.ErrCommitConflict)

		var conflict *store.CommitConflictError
		if !errors.As(err, &conflict) {
			t.Fatalf("error %v is not a *CommitConflictError", err)
		}

		if !conflict.Ident.Equal(id) || conflict.Expected != v1.Version || conflict.Actual != v2.Version {
			t.Fatalf("conflict = %+v, want ident %v expected %d actual %d", conflict, id, v1.Version, v2.Version)
		}

		got, err := ops.get(ctx, s, id)
		mustNoErr(t, err)

		if got.MetadataLocation != "s3://w/v2.json" {
			t.Fatalf("location = %q after failed commit, want v2", got.MetadataLocation)
		}
	})

	t.Run("recreated entry gets a new version", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := setup(t)
		id := ident("db", "t")
		first := mustCommit(t, ops, s, store.Commit{Ident: id, MetadataLocation: "s3://w/v1.json"})
		mustNoErr(t, ops.drop(ctx, s, id))
		second := mustCommit(t, ops, s, store.Commit{Ident: id, MetadataLocation: "s3://w/v1.json"})

		if second.Version <= first.Version {
			t.Fatalf("recreated version = %d, want > %d", second.Version, first.Version)
		}

		_, err := ops.commit(ctx, s, store.Commit{
			Ident:            id,
			ExpectedVersion:  first.Version,
			MetadataLocation: "s3://w/v2.json",
		})
		mustErrIs(t, err, store.ErrCommitConflict)
	})

	t.Run("drop missing", func(t *testing.T) {
		t.Parallel()

		err := ops.drop(t.Context(), setup(t), ident("db", "t"))
		mustErrIs(t, err, ops.errNotFound)
	})

	t.Run("rename", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name    string
			to      store.Ident
			wantErr error
		}{
			{name: "same namespace", to: ident("db", "renamed")},
			{name: "across namespaces", to: ident("other", "t")},
			{name: "onto existing entry", to: ident("db", "taken"), wantErr: ops.errAlreadyExists},
			{name: "into missing namespace", to: ident("missing", "t"), wantErr: store.ErrNamespaceNotFound},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				ctx := t.Context()
				s := setup(t)
				from := ident("db", "t")
				orig := mustCommit(t, ops, s, store.Commit{Ident: from, MetadataLocation: "s3://w/t.json"})
				mustCommit(t, ops, s, store.Commit{Ident: ident("db", "taken"), MetadataLocation: "s3://w/x.json"})

				err := ops.rename(ctx, s, from, tt.to)
				if tt.wantErr != nil {
					mustErrIs(t, err, tt.wantErr)

					_, err = ops.get(ctx, s, from)
					mustNoErr(t, err)

					return
				}

				mustNoErr(t, err)

				_, err = ops.get(ctx, s, from)
				mustErrIs(t, err, ops.errNotFound)

				got, err := ops.get(ctx, s, tt.to)
				mustNoErr(t, err)

				if got.MetadataLocation != orig.MetadataLocation || !got.Ident.Equal(tt.to) {
					t.Fatalf("renamed = %+v, want location %q at %v", got, orig.MetadataLocation, tt.to)
				}
			})
		}
	})

	t.Run("rename missing", func(t *testing.T) {
		t.Parallel()

		err := ops.rename(t.Context(), setup(t), ident("db", "nope"), ident("db", "t"))
		mustErrIs(t, err, ops.errNotFound)
	})

	t.Run("list missing namespace", func(t *testing.T) {
		t.Parallel()

		_, err := ops.list(t.Context(), setup(t), store.Namespace{"missing"})
		mustErrIs(t, err, store.ErrNamespaceNotFound)
	})
}

func testNameCollision(t *testing.T, newStore Factory) {
	t.Parallel()

	ctx := t.Context()
	s := newStore(t)
	mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
	mustCreateTable(t, s, ident("db", "t"))
	mustCreateView(t, s, ident("db", "v"))

	_, err := s.CommitView(ctx, store.Commit{Ident: ident("db", "t"), MetadataLocation: "s3://w/v.json"})
	mustErrIs(t, err, store.ErrTableAlreadyExists)

	_, err = s.CommitTables(ctx, []store.Commit{{Ident: ident("db", "v"), MetadataLocation: "s3://w/t.json"}})
	mustErrIs(t, err, store.ErrViewAlreadyExists)

	mustErrIs(t, s.RenameTable(ctx, ident("db", "t"), ident("db", "v")), store.ErrViewAlreadyExists)
	mustErrIs(t, s.RenameView(ctx, ident("db", "v"), ident("db", "t")), store.ErrTableAlreadyExists)
}

func testMultiTableCommit(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("all applied", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		a := mustCreateTable(t, s, ident("db", "a"))
		b := mustCreateTable(t, s, ident("db", "b"))

		got, err := s.CommitTables(ctx, []store.Commit{
			{Ident: a.Ident, ExpectedVersion: a.Version, MetadataLocation: "s3://w/a2.json"},
			{Ident: b.Ident, ExpectedVersion: b.Version, MetadataLocation: "s3://w/b2.json"},
			{Ident: ident("db", "c"), MetadataLocation: "s3://w/c1.json"},
		})
		mustNoErr(t, err)

		if len(got) != 3 {
			t.Fatalf("CommitTables returned %d pointers, want 3", len(got))
		}

		for i, name := range []string{"a", "b", "c"} {
			if got[i].Ident.Name != name {
				t.Fatalf("pointer %d is %v, want %q", i, got[i].Ident, name)
			}
		}
	})

	t.Run("none applied on conflict", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()
		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		a := mustCreateTable(t, s, ident("db", "a"))
		b := mustCreateTable(t, s, ident("db", "b"))

		_, err := s.CommitTables(ctx, []store.Commit{
			{Ident: a.Ident, ExpectedVersion: a.Version, MetadataLocation: "s3://w/a2.json"},
			{Ident: b.Ident, ExpectedVersion: b.Version + 1000, MetadataLocation: "s3://w/b2.json"},
			{Ident: ident("db", "c"), MetadataLocation: "s3://w/c1.json"},
		})
		mustErrIs(t, err, store.ErrCommitConflict)

		var conflict *store.CommitConflictError
		if !errors.As(err, &conflict) || !conflict.Ident.Equal(b.Ident) {
			t.Fatalf("error %v does not name the stale table %v", err, b.Ident)
		}

		gotA, err := s.GetTable(ctx, a.Ident)
		mustNoErr(t, err)

		if gotA.Version != a.Version || gotA.MetadataLocation != a.MetadataLocation {
			t.Fatalf("table a changed by a failed batch: %+v, want %+v", gotA, a)
		}

		_, err = s.GetTable(ctx, ident("db", "c"))
		mustErrIs(t, err, store.ErrTableNotFound)
	})

	t.Run("duplicate identifier", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		_, err := s.CommitTables(t.Context(), []store.Commit{
			{Ident: ident("db", "a"), MetadataLocation: "s3://w/1.json"},
			{Ident: ident("db", "a"), MetadataLocation: "s3://w/2.json"},
		})
		mustErrIs(t, err, store.ErrInvalidArgument)
	})

	t.Run("empty batch", func(t *testing.T) {
		t.Parallel()

		_, err := newStore(t).CommitTables(t.Context(), []store.Commit{})
		mustErrIs(t, err, store.ErrInvalidArgument)
	})
}

func testConcurrentCommits(t *testing.T, newStore Factory) {
	t.Parallel()

	const (
		writers = 8
		rounds  = 20
	)

	ctx := t.Context()
	s := newStore(t)
	mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
	current := mustCreateTable(t, s, ident("db", "t"))

	for round := range rounds {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []store.Pointer
		)

		for w := range writers {
			wg.Go(func() {
				p, err := s.CommitTables(ctx, []store.Commit{{
					Ident:            current.Ident,
					ExpectedVersion:  current.Version,
					MetadataLocation: fmt.Sprintf("s3://w/r%d-w%d.json", round, w),
				}})

				switch {
				case err == nil:
					mu.Lock()
					winners = append(winners, p[0])
					mu.Unlock()
				case !errors.Is(err, store.ErrCommitConflict):
					t.Errorf("round %d writer %d: unexpected error %v", round, w, err)
				}
			})
		}

		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("round %d: %d commits succeeded, want exactly 1", round, len(winners))
		}

		if winners[0].Version <= current.Version {
			t.Fatalf("round %d: version %d, want > %d", round, winners[0].Version, current.Version)
		}

		current = winners[0]
	}
}

func testIsolation(t *testing.T, newStore Factory) {
	t.Parallel()

	ctx := t.Context()
	s := newStore(t)
	props := map[string]string{"k": "v"}
	ns := store.Namespace{"db"}
	mustCreateNamespace(t, s, ns, props)

	// Mutating inputs and outputs must not change stored state.
	props["k"] = "changed"
	ns[0] = "changed"

	rec, err := s.GetNamespace(ctx, store.Namespace{"db"})
	mustNoErr(t, err)

	rec.Properties["k"] = "changed-again"
	rec.Namespace[0] = "changed-again"

	again, err := s.GetNamespace(ctx, store.Namespace{"db"})
	mustNoErr(t, err)

	if again.Properties["k"] != "v" || again.Namespace[0] != "db" {
		t.Fatalf("stored namespace changed through an alias: %+v", again)
	}

	id := ident("db", "t")
	mustCreateTable(t, s, id)
	id.Namespace[0] = "changed"

	if _, err := s.GetTable(ctx, ident("db", "t")); err != nil {
		t.Fatalf("stored table changed through an alias: %v", err)
	}
}

func testCancelledContext(t *testing.T, newStore Factory) {
	t.Parallel()

	s := newStore(t)
	mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	calls := map[string]error{
		"Ping":            s.Ping(ctx),
		"CreateNamespace": s.CreateNamespace(ctx, store.Namespace{"x"}, nil),
		"DropNamespace":   s.DropNamespace(ctx, store.Namespace{"db"}),
	}

	_, calls["GetNamespace"] = s.GetNamespace(ctx, store.Namespace{"db"})
	_, calls["CommitTables"] = s.CommitTables(ctx, []store.Commit{
		{Ident: ident("db", "t"), MetadataLocation: "s3://w/1.json"},
	})
	_, calls["GetTables"] = s.GetTables(ctx, []store.Ident{ident("db", "t")})
	_, calls["DropTable"] = s.DropTable(ctx, ident("db", "t"), store.DropOptions{Purge: true})
	_, calls["ListPurgeTasks"] = s.ListPurgeTasks(ctx, 10)

	for name, err := range calls {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("%s with cancelled context: error %v, want context.Canceled", name, err)
		}
	}
}

func testInvalidArguments(t *testing.T, newStore Factory) {
	t.Parallel()

	ctx := t.Context()
	s := newStore(t)
	mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

	tests := []struct {
		name string
		call func() error
	}{
		{name: "empty namespace", call: func() error {
			return s.CreateNamespace(ctx, store.Namespace{}, nil)
		}},
		{name: "empty namespace level", call: func() error {
			return s.CreateNamespace(ctx, store.Namespace{"a", ""}, nil)
		}},
		{name: "control character in namespace", call: func() error {
			return s.CreateNamespace(ctx, store.Namespace{"a\x1fb"}, nil)
		}},
		{name: "empty table name", call: func() error {
			_, err := s.GetTable(ctx, ident("db", ""))

			return err
		}},
		{name: "empty metadata location", call: func() error {
			_, err := s.CommitTables(ctx, []store.Commit{{Ident: ident("db", "t")}})

			return err
		}},
		{name: "duplicate table in a batch read", call: func() error {
			_, err := s.GetTables(ctx, []store.Ident{ident("db", "t"), ident("db", "t")})

			return err
		}},
		{name: "non-positive purge task limit", call: func() error {
			_, err := s.ListPurgeTasks(ctx, 0)

			return err
		}},
		{name: "negative expected version", call: func() error {
			_, err := s.CommitView(ctx, store.Commit{
				Ident:            ident("db", "v"),
				ExpectedVersion:  -1,
				MetadataLocation: "s3://w/1.json",
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mustErrIs(t, tt.call(), store.ErrInvalidArgument)
		})
	}
}
