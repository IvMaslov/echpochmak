package raftstore_test

import (
	"testing"

	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
	"echpochmak/internal/store/raftstore"
)

// The catalog's write path is a pointer swap: CommitTables with one commit.
// Its read path is GetTable. The raft cluster here runs over real gRPC on
// localhost, but on an in-memory filesystem, so fsync costs nothing: see
// ADR-003 for the cost of a commit on a real disk.
func BenchmarkStore(b *testing.B) {
	backends := map[string]func(b *testing.B) store.Store{
		"memory": func(*testing.B) store.Store { return memstore.New() },
		"raft": func(b *testing.B) store.Store {
			b.Helper()

			return raftstore.New(kvtest.NewGRPCCluster(b, 3).Client(), raftstore.Options{})
		},
	}

	for _, name := range []string{"memory", "raft"} {
		b.Run(name+"/commit", func(b *testing.B) {
			s := backends[name](b)
			current := setup(b, s)

			b.ResetTimer()

			for range b.N {
				ps, err := s.CommitTables(b.Context(), []store.Commit{{
					Ident:            current.Ident,
					ExpectedVersion:  current.Version,
					MetadataLocation: "s3://w/next",
				}})
				if err != nil {
					b.Fatal(err)
				}

				current = ps[0]
			}
		})

		// What the catalog does on every attempt since Phase 5: record an
		// intent, then commit with it.
		b.Run(name+"/commit-with-intent", func(b *testing.B) {
			s := backends[name](b)
			current := setup(b, s)

			b.ResetTimer()

			for range b.N {
				intent, err := s.CreateIntent(b.Context(), store.Intent{Locations: []string{"s3://w/next"}})
				if err != nil {
					b.Fatal(err)
				}

				ps, err := s.CommitTables(b.Context(), []store.Commit{{
					Ident:            current.Ident,
					ExpectedVersion:  current.Version,
					MetadataLocation: "s3://w/next",
					Intent:           intent.Ref(),
				}})
				if err != nil {
					b.Fatal(err)
				}

				current = ps[0]
			}
		})

		b.Run(name+"/get", func(b *testing.B) {
			s := backends[name](b)
			current := setup(b, s)

			b.ResetTimer()

			for range b.N {
				if _, err := s.GetTable(b.Context(), current.Ident); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func setup(b *testing.B, s store.Store) store.Pointer {
	b.Helper()

	if err := s.CreateNamespace(b.Context(), store.Namespace{"db"}, nil); err != nil {
		b.Fatal(err)
	}

	ps, err := s.CommitTables(b.Context(), []store.Commit{{Ident: ident("db", "t"), MetadataLocation: "s3://w/1"}})
	if err != nil {
		b.Fatal(err)
	}

	return ps[0]
}
