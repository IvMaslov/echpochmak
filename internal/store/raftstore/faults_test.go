package raftstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/store"
	"echpochmak/internal/store/raftstore"
)

func ident(ns, name string) store.Ident {
	return store.Ident{Namespace: store.Namespace{ns}, Name: name}
}

func newStore(t *testing.T, c *kvtest.GRPCCluster) *raftstore.Store {
	t.Helper()

	s := raftstore.New(c.Client(), raftstore.Options{})
	err := s.CreateNamespace(t.Context(), store.Namespace{"db"}, nil)
	if err != nil && !errors.Is(err, store.ErrNamespaceAlreadyExists) {
		t.Fatalf("CreateNamespace: %v", err)
	}

	return s
}

func create(t *testing.T, s store.Store, name string) store.Pointer {
	t.Helper()

	ps, err := s.CommitTables(t.Context(), []store.Commit{
		{Ident: ident("db", name), MetadataLocation: "s3://w/" + name},
	})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}

	return ps[0]
}

func TestTransactionSizeLimit(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)

	commits := func(n int) []store.Commit {
		out := make([]store.Commit, 0, n)
		for i := range n {
			out = append(out, store.Commit{Ident: ident("db", fmt.Sprintf("t%03d", i)), MetadataLocation: "s3://w/m"})
		}

		return out
	}

	// Creates need the most compares per commit, so this is the worst case.
	if _, err := s.CommitTables(t.Context(), commits(raftstore.MaxCommitsPerTxn)); err != nil {
		t.Fatalf("%d creates in one transaction: %v", raftstore.MaxCommitsPerTxn, err)
	}

	_, err := s.CommitTables(t.Context(), commits(raftstore.MaxCommitsPerTxn+1))
	if !errors.Is(err, store.ErrInvalidArgument) {
		t.Fatalf("%d commits: error %v, want ErrInvalidArgument", raftstore.MaxCommitsPerTxn+1, err)
	}
}

// When several commits of one transaction fail, the error describes the
// first one in commit order, as memstore's does.
func TestFirstFailingCommitIsReported(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)
	a := create(t, s, "a")

	missing := store.Commit{Ident: ident("db", "missing"), ExpectedVersion: 1, MetadataLocation: "s3://w/x"}
	stale := store.Commit{Ident: a.Ident, ExpectedVersion: a.Version + 1000, MetadataLocation: "s3://w/x"}
	noNamespace := store.Commit{Ident: ident("nope", "t"), MetadataLocation: "s3://w/x"}

	tests := []struct {
		name    string
		commits []store.Commit
		want    error
	}{
		{"missing table first", []store.Commit{missing, stale, noNamespace}, store.ErrTableNotFound},
		{"stale table first", []store.Commit{stale, missing, noNamespace}, store.ErrCommitConflict},
		{"missing namespace first", []store.Commit{noNamespace, stale, missing}, store.ErrNamespaceNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := s.CommitTables(t.Context(), tt.commits)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

// Two stores on separate clients stand in for two catalog replicas: in each
// round, every replica commits from the same version, and exactly one wins.
func TestConcurrentReplicas(t *testing.T) {
	t.Parallel()

	const rounds = 15

	c := kvtest.NewGRPCCluster(t, 3)
	replicas := []*raftstore.Store{newStore(t, c), newStore(t, c)}
	current := create(t, replicas[0], "contended")

	for round := range rounds {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			winners []store.Pointer
		)

		for r, s := range replicas {
			for w := range 3 {
				wg.Go(func() {
					ps, err := s.CommitTables(t.Context(), []store.Commit{{
						Ident:            current.Ident,
						ExpectedVersion:  current.Version,
						MetadataLocation: fmt.Sprintf("s3://w/r%d-%d-%d", round, r, w),
					}})

					switch {
					case err == nil:
						mu.Lock()
						winners = append(winners, ps[0])
						mu.Unlock()
					case !errors.Is(err, store.ErrCommitConflict):
						t.Errorf("round %d: unexpected error %v", round, err)
					}
				})
			}
		}

		wg.Wait()

		if len(winners) != 1 {
			t.Fatalf("round %d: %d commits won, want exactly 1", round, len(winners))
		}

		current = winners[0]
	}
}

// A commit racing the loss of the leader either succeeds or reports the
// store unavailable; the pointer then holds the old or the new location,
// never anything else.
func TestLeaderLossDuringCommits(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)
	current := create(t, s, "t")

	stopped := make(chan struct{})

	go func() {
		time.Sleep(50 * time.Millisecond)
		c.Stop(c.Leader())
		close(stopped)
	}()

	unknown := 0

	for i := range 40 {
		next := fmt.Sprintf("s3://w/v%d", i)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		ps, err := s.CommitTables(ctx, []store.Commit{{
			Ident:            current.Ident,
			ExpectedVersion:  current.Version,
			MetadataLocation: next,
		}})

		cancel()

		switch {
		case err == nil:
			current = ps[0]
		case errors.Is(err, store.ErrUnavailable):
			// The outcome is unknown: the pointer moved to next, or did not.
			unknown++

			got, getErr := s.GetTable(t.Context(), current.Ident)
			if getErr != nil {
				t.Fatalf("reading the pointer after an unknown outcome: %v", getErr)
			}

			if got.MetadataLocation != next && got.MetadataLocation != current.MetadataLocation {
				t.Fatalf("pointer at %q, want %q or %q", got.MetadataLocation, current.MetadataLocation, next)
			}

			current = got
		default:
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	<-stopped

	got, err := s.GetTable(t.Context(), current.Ident)
	if err != nil || got.MetadataLocation != current.MetadataLocation || got.Version != current.Version {
		t.Fatalf("final pointer = %+v, %v; want %+v", got, err, current)
	}

	t.Logf("%d commits ended with an unknown outcome", unknown)
}
