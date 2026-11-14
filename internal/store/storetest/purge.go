package storetest

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"echpochmak/internal/store"
)

func testGetTables(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("existing and missing tables in order", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		a := mustCreateTable(t, s, ident("db", "a"))
		b := mustCreateTable(t, s, ident("db", "b"))
		mustCreateView(t, s, ident("db", "v"))

		ids := []store.Ident{ident("db", "b"), ident("db", "missing"), ident("db", "v"), ident("db", "a")}

		got, err := s.GetTables(t.Context(), ids)
		mustNoErr(t, err)

		want := []store.Pointer{b, {Ident: ids[1]}, {Ident: ids[2]}, a}
		if len(got) != len(want) {
			t.Fatalf("got %d pointers, want %d", len(got), len(want))
		}

		for i := range want {
			if !samePointer(got[i], want[i]) {
				t.Errorf("pointer %d = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("empty request", func(t *testing.T) {
		t.Parallel()

		got, err := newStore(t).GetTables(t.Context(), []store.Ident{})
		mustNoErr(t, err)

		if len(got) != 0 {
			t.Fatalf("got %d pointers, want none", len(got))
		}
	})
}

func testDropTable(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("returns the removed pointer", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		created := mustCreateTable(t, s, ident("db", "t"))

		ptrs, err := s.CommitTables(t.Context(), []store.Commit{{
			Ident:            ident("db", "t"),
			ExpectedVersion:  created.Version,
			MetadataLocation: "s3://warehouse/t/v2.json",
		}})
		mustNoErr(t, err)

		dropped, err := s.DropTable(t.Context(), ident("db", "t"), store.DropOptions{})
		mustNoErr(t, err)

		if !samePointer(dropped, ptrs[0]) {
			t.Fatalf("dropped %+v, want %+v", dropped, ptrs[0])
		}

		tasks, err := s.ListPurgeTasks(t.Context(), 10)
		mustNoErr(t, err)

		if len(tasks) != 0 {
			t.Fatalf("a drop without purge queued %d tasks", len(tasks))
		}
	})

	t.Run("purge queues a task atomically", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		created := mustCreateTable(t, s, ident("db", "t"))
		before := time.Now().Add(-time.Second)

		_, err := s.DropTable(t.Context(), ident("db", "t"), store.DropOptions{Purge: true})
		mustNoErr(t, err)

		_, err = s.GetTable(t.Context(), ident("db", "t"))
		mustErrIs(t, err, store.ErrTableNotFound)

		task := mustOneTask(t, s)
		if !task.Ident.Equal(ident("db", "t")) || task.MetadataLocation != created.MetadataLocation {
			t.Fatalf("task = %+v, want table db.t at %q", task, created.MetadataLocation)
		}

		if task.ID == "" || task.Version == 0 || task.Created.Before(before) || !task.Claimable(time.Now()) {
			t.Fatalf("task = %+v, want an unclaimed task with an ID, a version and a creation time", task)
		}
	})

	t.Run("missing table queues nothing", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)

		_, err := s.DropTable(t.Context(), ident("db", "t"), store.DropOptions{Purge: true})
		mustErrIs(t, err, store.ErrTableNotFound)

		tasks, err := s.ListPurgeTasks(t.Context(), 10)
		mustNoErr(t, err)

		if len(tasks) != 0 {
			t.Fatalf("a failed drop queued %d tasks", len(tasks))
		}
	})
}

func testPurgeQueue(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("claim renew complete", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		task := queuePurge(t, s, "t")
		now := time.Now()

		claimed, err := s.ClaimPurgeTask(t.Context(), task, store.PurgeClaim{Owner: "a", Until: now.Add(time.Minute)})
		mustNoErr(t, err)

		if claimed.Version == task.Version || claimed.Claim.Owner != "a" || claimed.Claimable(now) {
			t.Fatalf("claimed = %+v, want a new version claimed by a", claimed)
		}

		listed := mustOneTask(t, s)
		sameClaim := listed.Claim.Owner == "a" && listed.Claim.Until.Equal(claimed.Claim.Until)
		if listed.Version != claimed.Version || !sameClaim {
			t.Fatalf("listed = %+v, want %+v", listed, claimed)
		}

		renewed, err := s.ClaimPurgeTask(t.Context(), claimed, store.PurgeClaim{Owner: "a", Until: now.Add(time.Hour)})
		mustNoErr(t, err)

		mustNoErr(t, s.CompletePurgeTask(t.Context(), renewed))

		tasks, err := s.ListPurgeTasks(t.Context(), 10)
		mustNoErr(t, err)

		if len(tasks) != 0 {
			t.Fatalf("%d tasks left after completion", len(tasks))
		}
	})

	t.Run("stale version is rejected", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		task := queuePurge(t, s, "t")
		claim := store.PurgeClaim{Owner: "a", Until: time.Now().Add(time.Minute)}

		_, err := s.ClaimPurgeTask(t.Context(), task, claim)
		mustNoErr(t, err)

		// A second worker read the task before the first one claimed it.
		_, err = s.ClaimPurgeTask(t.Context(), task, store.PurgeClaim{Owner: "b", Until: claim.Until})
		mustErrIs(t, err, store.ErrPurgeTaskChanged)

		mustErrIs(t, s.CompletePurgeTask(t.Context(), task), store.ErrPurgeTaskChanged)
	})

	t.Run("completed task is gone", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		task := queuePurge(t, s, "t")
		mustNoErr(t, s.CompletePurgeTask(t.Context(), task))

		_, err := s.ClaimPurgeTask(t.Context(), task, store.PurgeClaim{Owner: "a", Until: time.Now()})
		mustErrIs(t, err, store.ErrPurgeTaskChanged)
		mustErrIs(t, s.CompletePurgeTask(t.Context(), task), store.ErrPurgeTaskChanged)
	})

	t.Run("oldest first, up to the limit", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		first := queuePurge(t, s, "a")
		second := queuePurge(t, s, "b")
		queuePurge(t, s, "c")

		tasks, err := s.ListPurgeTasks(t.Context(), 2)
		mustNoErr(t, err)

		if len(tasks) != 2 || tasks[0].ID != first.ID || tasks[1].ID != second.ID {
			t.Fatalf("tasks = %+v, want the tasks of a and b, in order", tasks)
		}
	})

	t.Run("concurrent claims: exactly one wins", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		task := queuePurge(t, s, "t")
		results := make(chan error, 8)

		for i := range cap(results) {
			go func() {
				claim := store.PurgeClaim{Owner: fmt.Sprintf("w%d", i), Until: time.Now().Add(time.Minute)}
				_, err := s.ClaimPurgeTask(t.Context(), task, claim)
				results <- err
			}()
		}

		won := 0

		for range cap(results) {
			err := <-results

			switch {
			case err == nil:
				won++
			case !errors.Is(err, store.ErrPurgeTaskChanged):
				t.Errorf("claim: unexpected error %v", err)
			}
		}

		if won != 1 {
			t.Fatalf("%d claims won, want exactly 1", won)
		}
	})
}

// queuePurge creates a table named name in namespace db, drops it with purge
// and returns its task.
func queuePurge(t *testing.T, s store.Store, name string) store.PurgeTask {
	t.Helper()

	if _, err := s.GetNamespace(t.Context(), store.Namespace{"db"}); errors.Is(err, store.ErrNamespaceNotFound) {
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
	}

	mustCreateTable(t, s, ident("db", name))

	_, err := s.DropTable(t.Context(), ident("db", name), store.DropOptions{Purge: true})
	mustNoErr(t, err)

	tasks, err := s.ListPurgeTasks(t.Context(), 100)
	mustNoErr(t, err)

	for i := range tasks {
		if tasks[i].Ident.Name == name {
			return tasks[i]
		}
	}

	t.Fatalf("no purge task for %q", name)

	return store.PurgeTask{}
}

func mustOneTask(t *testing.T, s store.Store) store.PurgeTask {
	t.Helper()

	tasks, err := s.ListPurgeTasks(t.Context(), 10)
	mustNoErr(t, err)

	if len(tasks) != 1 {
		t.Fatalf("%d purge tasks, want 1", len(tasks))
	}

	return tasks[0]
}

func samePointer(a, b store.Pointer) bool {
	return a.Ident.Equal(b.Ident) && a.MetadataLocation == b.MetadataLocation && a.Version == b.Version
}
