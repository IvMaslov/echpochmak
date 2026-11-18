package catalog_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

const setPropsUpdate = `[{"action":"set-properties","updates":{"k":"v"}}]`

// newJanitor returns a janitor whose clock runs an hour ahead, so every
// intent is past its grace period.
func newJanitor(f *fixture) *catalog.Janitor {
	return catalog.NewJanitor(catalog.JanitorOptions{
		Store:  f.store,
		Files:  f.files,
		Grace:  time.Minute,
		Logger: slog.New(slog.DiscardHandler),
		Now:    func() time.Time { return time.Now().Add(time.Hour) },
	})
}

func intents(t *testing.T, s store.Store) []store.Intent {
	t.Helper()

	got, err := s.ListIntents(t.Context(), 100)
	if err != nil {
		t.Fatalf("ListIntents: %v", err)
	}

	return got
}

func TestCommitReleasesItsIntent(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	if _, err := f.commitJSON(t, "t", `[]`, setPropsUpdate); err != nil {
		t.Fatalf("commit: %v", err)
	}

	if got := intents(t, f.store); len(got) != 0 {
		t.Fatalf("intents after create and commit = %+v, want none", got)
	}
}

// A replica that dies between writing the metadata file and the pointer swap
// leaves its intent; the janitor deletes the file.
func TestJanitorDeletesTheFileOfACommitThatNeverLanded(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	before := f.createTable(t, "t", nil)

	var written string

	f.store.setCommit(func(_ context.Context, commits []store.Commit) ([]store.Pointer, error) {
		written = commits[0].MetadataLocation

		return nil, store.OutcomeUnknown(errors.New("replica crashed"))
	})

	_, err := f.commitJSON(t, "t", `[]`, setPropsUpdate)
	if !errors.Is(err, catalog.ErrCommitStateUnknown) {
		t.Fatalf("commit = %v, want ErrCommitStateUnknown", err)
	}

	f.store.setCommit(nil)

	if !slices.Contains(f.files.list(), written) {
		t.Fatal("the file of a commit in unknown state was deleted at once")
	}

	res := newJanitor(f).Sweep(t.Context())
	if res.Cleaned != 1 || res.FilesDeleted != 1 {
		t.Fatalf("sweep = %+v, want one intent cleaned and one file deleted", res)
	}

	if slices.Contains(f.files.list(), written) {
		t.Fatal("the orphaned file survived the sweep")
	}

	after, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil || after.MetadataLocation != before.MetadataLocation {
		t.Fatalf("table after the sweep = %q, %v; want %q", after.MetadataLocation, err, before.MetadataLocation)
	}

	if got := intents(t, f.store); len(got) != 0 {
		t.Fatalf("intents after the sweep = %+v, want none", got)
	}
}

// A commit whose outcome was unknown but which landed removed its intent, so
// its file survives every sweep.
func TestJanitorKeepsTheFileOfACommitThatLanded(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	f.store.setCommit(func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
		if _, err := f.store.Store.CommitTables(ctx, commits); err != nil {
			return nil, err
		}

		return nil, store.OutcomeUnknown(errors.New("the reply was lost"))
	})

	_, err := f.commitJSON(t, "t", `[]`, setPropsUpdate)
	if !errors.Is(err, catalog.ErrCommitStateUnknown) {
		t.Fatalf("commit = %v, want ErrCommitStateUnknown", err)
	}

	f.store.setCommit(nil)

	res := newJanitor(f).Sweep(t.Context())
	if res.Pending != 0 || res.FilesDeleted != 0 {
		t.Fatalf("sweep = %+v, want nothing to do", res)
	}

	current, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil {
		t.Fatalf("LoadTable: %v", err)
	}

	if !slices.Contains(f.files.list(), current.MetadataLocation) {
		t.Fatal("the current metadata file was deleted")
	}
}

// The janitor condemns an intent while its commit is still in flight: the
// commit fails, is retried with a new file and a new intent, and succeeds.
// The condemned file never becomes current.
func TestCommitRetriesAfterItsIntentWasCondemned(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 3)
	f.createTable(t, "t", nil)

	var condemnedFile string

	f.store.setCommit(func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
		f.store.setCommit(nil) // only the first attempt is intercepted

		condemnedFile = commits[0].MetadataLocation
		if _, err := f.store.CondemnIntent(ctx, commits[0].Intent); err != nil {
			return nil, err
		}

		return f.store.Store.CommitTables(ctx, commits)
	})

	tbl, err := f.commitJSON(t, "t", `[]`, setPropsUpdate)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	if tbl.MetadataLocation == condemnedFile {
		t.Fatal("the commit landed on the condemned file")
	}

	newJanitor(f).Sweep(t.Context())

	files := f.files.list()
	if slices.Contains(files, condemnedFile) || !slices.Contains(files, tbl.MetadataLocation) {
		t.Fatalf("files %q: want %q kept and %q gone", files, tbl.MetadataLocation, condemnedFile)
	}

	if got := intents(t, f.store); len(got) != 0 {
		t.Fatalf("intents left = %+v, want none", got)
	}
}

// A janitor that stopped after condemning an intent leaves it condemned; the
// next sweep finishes it, however young it is.
func TestJanitorFinishesACondemnedIntent(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	location := warehouse + "/db/orphan.metadata.json"

	if err := f.files.Write(t.Context(), location, []byte("{}")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	intent, err := f.store.CreateIntent(t.Context(), store.Intent{Locations: []string{location}})
	if err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}

	if _, err := f.store.CondemnIntent(t.Context(), intent.Ref()); err != nil {
		t.Fatalf("CondemnIntent: %v", err)
	}

	j := catalog.NewJanitor(catalog.JanitorOptions{
		Store:  f.store,
		Files:  f.files,
		Grace:  time.Hour,
		Logger: slog.New(slog.DiscardHandler),
	})

	if res := j.Sweep(t.Context()); res.Cleaned != 1 {
		t.Fatalf("sweep = %+v, want the condemned intent cleaned", res)
	}

	if slices.Contains(f.files.list(), location) {
		t.Fatal("the condemned intent's file survived")
	}
}

func TestJanitorLeavesYoungIntents(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	if _, err := f.store.CreateIntent(t.Context(), store.Intent{Locations: []string{warehouse + "/x"}}); err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}

	j := catalog.NewJanitor(catalog.JanitorOptions{
		Store:  f.store,
		Files:  f.files,
		Grace:  time.Hour,
		Logger: slog.New(slog.DiscardHandler),
	})

	res := j.Sweep(t.Context())
	if res.Pending != 1 || res.Cleaned != 0 || res.Oldest.IsZero() {
		t.Fatalf("sweep = %+v, want one pending intent left alone", res)
	}
}

func TestJanitorKeepsTheIntentWhenADeleteFails(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	if _, err := f.store.CreateIntent(t.Context(), store.Intent{Locations: []string{warehouse + "/x"}}); err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}

	j := catalog.NewJanitor(catalog.JanitorOptions{
		Store:  f.store,
		Files:  failingDeletes{f.files},
		Grace:  time.Minute,
		Logger: slog.New(slog.DiscardHandler),
		Now:    func() time.Time { return time.Now().Add(time.Hour) },
	})

	if res := j.Sweep(t.Context()); res.Failed != 1 || res.Cleaned != 0 {
		t.Fatalf("sweep = %+v, want one failure", res)
	}

	got := intents(t, f.store)
	if len(got) != 1 || !got[0].Condemned {
		t.Fatalf("intents = %+v, want one condemned intent left for later", got)
	}
}

// A store that was certainly not reached leaves nothing behind: the file is
// deleted at once and the client sees "unavailable", not "state unknown".
func TestDefiniteStoreFailureCleansUpAtOnce(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	var written string

	f.store.setCommit(func(_ context.Context, commits []store.Commit) ([]store.Pointer, error) {
		written = commits[0].MetadataLocation

		return nil, errors.Join(store.ErrUnavailable, errors.New("no leader"))
	})

	_, err := f.commitJSON(t, "t", `[]`, setPropsUpdate)
	if !store.IsDefinitelyNotApplied(err) || errors.Is(err, catalog.ErrCommitStateUnknown) {
		t.Fatalf("commit = %v, want a definite ErrUnavailable", err)
	}

	if slices.Contains(f.files.list(), written) {
		t.Fatal("the file of a commit that certainly failed was kept")
	}

	if got := intents(t, f.store); len(got) != 0 {
		t.Fatalf("intents = %+v, want none", got)
	}
}
