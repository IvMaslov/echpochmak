package raftstore_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// One transaction holds up to raftstore.TxnBudget compares: an update costs
// one, a create three.
func TestTransactionSizeLimit(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)

	const maxCreates = raftstore.TxnBudget / 3

	creates := func(first, n int) []store.Commit {
		out := make([]store.Commit, 0, n)
		for i := range n {
			id := ident("db", fmt.Sprintf("t%03d", first+i))
			out = append(out, store.Commit{Ident: id, MetadataLocation: "s3://w/m"})
		}

		return out
	}

	if _, err := s.CommitTables(t.Context(), creates(0, maxCreates)); err != nil {
		t.Fatalf("%d creates in one transaction: %v", maxCreates, err)
	}

	_, err := s.CommitTables(t.Context(), creates(maxCreates, maxCreates+1))
	if !errors.Is(err, store.ErrInvalidArgument) {
		t.Fatalf("%d creates: error %v, want ErrInvalidArgument", maxCreates+1, err)
	}

	// Enough tables to update TxnBudget+1 of them.
	for first := maxCreates; first <= raftstore.TxnBudget; first += maxCreates {
		n := min(maxCreates, raftstore.TxnBudget+1-first)
		if _, err := s.CommitTables(t.Context(), creates(first, n)); err != nil {
			t.Fatalf("creating tables %d..%d: %v", first, first+n-1, err)
		}
	}

	ids := make([]store.Ident, 0, raftstore.TxnBudget+1)
	for i := range raftstore.TxnBudget + 1 {
		ids = append(ids, ident("db", fmt.Sprintf("t%03d", i)))
	}

	if _, err := s.GetTables(t.Context(), ids); !errors.Is(err, store.ErrInvalidArgument) {
		t.Fatalf("reading %d tables: error %v, want ErrInvalidArgument", len(ids), err)
	}

	ptrs, err := s.GetTables(t.Context(), ids[:raftstore.TxnBudget])
	if err != nil {
		t.Fatalf("reading %d tables: %v", raftstore.TxnBudget, err)
	}

	updates := make([]store.Commit, 0, len(ptrs))
	for _, p := range ptrs {
		c := store.Commit{Ident: p.Ident, ExpectedVersion: p.Version, MetadataLocation: "s3://w/m2"}
		updates = append(updates, c)
	}

	if _, err := s.CommitTables(t.Context(), updates); err != nil {
		t.Fatalf("%d updates in one transaction: %v", len(updates), err)
	}

	extra := store.Commit{Ident: ids[raftstore.TxnBudget], ExpectedVersion: 1, MetadataLocation: "s3://w/m2"}
	over := append(slices.Clone(updates), extra)
	if _, err := s.CommitTables(t.Context(), over); !errors.Is(err, store.ErrInvalidArgument) {
		t.Fatalf("%d updates: error %v, want ErrInvalidArgument", len(over), err)
	}
}

// Creates and updates read different numbers of keys on failure; the error
// still names the right commit when they are mixed.
func TestMixedCommitFailureIsClassified(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)
	a := create(t, s, "a")
	create(t, s, "taken")

	_, err := s.CommitTables(t.Context(), []store.Commit{
		{Ident: ident("db", "new"), MetadataLocation: "s3://w/n"},
		{Ident: ident("db", "a"), ExpectedVersion: a.Version + 100, MetadataLocation: "s3://w/a2"},
	})

	var conflict *store.CommitConflictError
	if !errors.As(err, &conflict) || conflict.Ident.Name != "a" || conflict.Actual != a.Version {
		t.Fatalf("create then stale update: error %v, want a conflict on a at version %d", err, a.Version)
	}

	_, err = s.CommitTables(t.Context(), []store.Commit{
		{Ident: ident("db", "a"), ExpectedVersion: a.Version, MetadataLocation: "s3://w/a2"},
		{Ident: ident("db", "taken"), MetadataLocation: "s3://w/t"},
	})
	if !errors.Is(err, store.ErrTableAlreadyExists) {
		t.Fatalf("update then create of a taken name: error %v, want ErrTableAlreadyExists", err)
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

// A drop racing a stream of commits returns exactly the pointer of the last
// commit that landed before it: unregister depends on this.
func TestDropReturnsTheLastCommit(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)
	last := create(t, s, "t")
	committed := make(chan store.Pointer, 1)

	go func() {
		defer close(committed)

		cur := last

		for i := 0; ; i++ {
			ptrs, err := s.CommitTables(t.Context(), []store.Commit{{
				Ident:            cur.Ident,
				ExpectedVersion:  cur.Version,
				MetadataLocation: fmt.Sprintf("s3://w/v%d", i),
			}})
			if err != nil {
				committed <- cur

				return
			}

			cur = ptrs[0]
		}
	}()

	time.Sleep(50 * time.Millisecond)

	dropped, err := s.DropTable(t.Context(), ident("db", "t"), store.DropOptions{})
	if err != nil {
		t.Fatalf("drop: %v", err)
	}

	final := <-committed
	if dropped.Version != final.Version || dropped.MetadataLocation != final.MetadataLocation {
		t.Fatalf("dropped %+v, but the last commit was %+v", dropped, final)
	}
}

// A drop with purge under commits that keep coming still gets through, and
// its task names the final metadata file.
func TestPurgeDropUnderCommits(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	s := newStore(t, c)
	last := create(t, s, "t")
	committed := make(chan store.Pointer, 1)

	go func() {
		defer close(committed)

		cur := last

		for i := 0; ; i++ {
			ptrs, err := s.CommitTables(t.Context(), []store.Commit{{
				Ident:            cur.Ident,
				ExpectedVersion:  cur.Version,
				MetadataLocation: fmt.Sprintf("s3://w/v%d", i),
			}})
			if err != nil {
				committed <- cur

				return
			}

			cur = ptrs[0]

			// A commit also writes a metadata file to S3, which takes a few
			// milliseconds.
			time.Sleep(3 * time.Millisecond)
		}
	}()

	time.Sleep(50 * time.Millisecond)

	dropped, err := s.DropTable(t.Context(), ident("db", "t"), store.DropOptions{Purge: true})
	if err != nil {
		t.Fatalf("drop: %v", err)
	}

	final := <-committed
	if dropped.Version != final.Version || dropped.MetadataLocation != final.MetadataLocation {
		t.Fatalf("dropped %+v, but the last commit was %+v", dropped, final)
	}

	tasks, err := s.ListPurgeTasks(t.Context(), 10)
	if err != nil || len(tasks) != 1 || tasks[0].MetadataLocation != final.MetadataLocation {
		t.Fatalf("purge tasks %+v (%v), want one for %q", tasks, err, final.MetadataLocation)
	}
}
