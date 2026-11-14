package catalog_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

func (f *fixture) newPurger(owner string, now func() time.Time) *catalog.Purger {
	return catalog.NewPurger(catalog.PurgerOptions{
		Store:  f.store,
		Files:  f.files,
		Owner:  owner,
		Lease:  time.Minute,
		Logger: slog.New(slog.DiscardHandler),
		Now:    now,
	})
}

func (f *fixture) purgeOnce(t *testing.T, p *catalog.Purger) {
	t.Helper()

	p.Scan(t.Context())
	p.Wait()
}

func (f *fixture) tasks(t *testing.T) []store.PurgeTask {
	t.Helper()

	tasks, err := f.store.ListPurgeTasks(t.Context(), 100)
	if err != nil {
		t.Fatal(err)
	}

	return tasks
}

func TestPurgeDeletesMetadataFiles(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)

	for _, v := range []string{"1", "2"} {
		if _, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"v":"`+v+`"}}]`); err != nil {
			t.Fatal(err)
		}
	}

	other := f.createTable(t, "other", nil)

	if err := f.svc.DropTable(t.Context(), ident("t"), true); err != nil {
		t.Fatal(err)
	}

	f.purgeOnce(t, f.newPurger("w1", time.Now))

	location := strings.TrimSuffix(parseMetadata(t, created).Location(), "/") + "/"
	for _, file := range f.files.list() {
		if strings.HasPrefix(file, location) {
			t.Errorf("file %q of the purged table is left", file)
		}
	}

	if !slices.Contains(f.files.list(), other.MetadataLocation) {
		t.Fatal("the purge deleted another table's files")
	}

	if tasks := f.tasks(t); len(tasks) != 0 {
		t.Fatalf("%d tasks left after the purge", len(tasks))
	}
}

func TestPurgeKeepsFilesOutsideTheLocation(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	// A registered table whose metadata log starts outside its location.
	const foreign = "mem://other/elsewhere/custom.json"

	foreignMetadata(t, f, foreign)

	in := catalog.RegisterTableInput{Ident: ident("r"), MetadataLocation: foreign}
	if _, err := f.svc.RegisterTable(t.Context(), in); err != nil {
		t.Fatal(err)
	}

	committed, err := f.commitJSON(t, "r", `[]`, `[{"action":"set-properties","updates":{"k":"v"}}]`)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.svc.DropTable(t.Context(), ident("r"), true); err != nil {
		t.Fatal(err)
	}

	f.purgeOnce(t, f.newPurger("w1", time.Now))

	files := f.files.list()
	if !slices.Contains(files, foreign) || slices.Contains(files, committed.MetadataLocation) {
		t.Fatalf("files after purge %q: want %q kept and %q deleted", files, foreign, committed.MetadataLocation)
	}
}

func TestPurgeRespectsGCDisabled(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", map[string]string{"gc.enabled": "false"})

	if err := f.svc.DropTable(t.Context(), ident("t"), true); !errors.Is(err, catalog.ErrInvalidRequest) {
		t.Fatalf("DropTable with purge = %v, want ErrInvalidRequest", err)
	}

	if err := f.svc.TableExists(t.Context(), ident("t")); err != nil {
		t.Fatalf("the rejected drop removed the table: %v", err)
	}

	// Without purge the table can still be dropped.
	if err := f.svc.DropTable(t.Context(), ident("t"), false); err != nil {
		t.Fatal(err)
	}
}

func TestPurgeTakesOverAnExpiredClaim(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)

	if err := f.svc.DropTable(t.Context(), ident("t"), true); err != nil {
		t.Fatal(err)
	}

	// A worker claimed the task and died.
	dead, err := f.store.ClaimPurgeTask(t.Context(), f.tasks(t)[0], store.PurgeClaim{
		Owner: "dead",
		Until: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}

	// While the claim is live, nobody else runs the task.
	f.purgeOnce(t, f.newPurger("w2", time.Now))

	if !slices.Contains(f.files.list(), created.MetadataLocation) || len(f.tasks(t)) != 1 {
		t.Fatal("a live claim was taken over")
	}

	// After the lease, another worker finishes it.
	later := func() time.Time { return dead.Claim.Until.Add(time.Second) }
	f.purgeOnce(t, f.newPurger("w2", later))

	if slices.Contains(f.files.list(), created.MetadataLocation) || len(f.tasks(t)) != 0 {
		t.Fatal("the expired claim was not taken over")
	}
}

func TestPurgeOfDeletedFilesCompletes(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	created := f.createTable(t, "t", nil)

	if err := f.svc.DropTable(t.Context(), ident("t"), true); err != nil {
		t.Fatal(err)
	}

	// A previous run deleted the files, then died before completing.
	if err := f.files.Delete(t.Context(), created.MetadataLocation); err != nil {
		t.Fatal(err)
	}

	f.purgeOnce(t, f.newPurger("w1", time.Now))

	if tasks := f.tasks(t); len(tasks) != 0 {
		t.Fatalf("%d tasks left", len(tasks))
	}
}

// failingDeletes fails every file deletion.
type failingDeletes struct {
	*memFiles
}

func (failingDeletes) Delete(context.Context, string) error {
	return errors.New("access denied")
}

func TestPurgeFailureKeepsTheTask(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "t", nil)

	if err := f.svc.DropTable(t.Context(), ident("t"), true); err != nil {
		t.Fatal(err)
	}

	p := catalog.NewPurger(catalog.PurgerOptions{
		Store:  f.store,
		Files:  failingDeletes{f.files},
		Owner:  "w1",
		Logger: slog.New(slog.DiscardHandler),
	})
	f.purgeOnce(t, p)

	tasks := f.tasks(t)
	if len(tasks) != 1 || tasks[0].Claim.Owner != "w1" {
		t.Fatalf("tasks %+v, want the task kept with w1's claim, to expire and be retried", tasks)
	}
}
