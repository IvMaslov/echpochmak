package catalog_test

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

func setProps(t *testing.T, name, props string) catalog.CommitInput {
	t.Helper()

	return catalog.CommitInput{
		Ident:   ident(name),
		Updates: parseUpdates(t, `[{"action":"set-properties","updates":`+props+`}]`),
	}
}

func (f *fixture) props(t *testing.T, name string) map[string]string {
	t.Helper()

	tbl, err := f.svc.LoadTable(t.Context(), ident(name))
	if err != nil {
		t.Fatalf("LoadTable(%s): %v", name, err)
	}

	return parseMetadata(t, tbl).Properties()
}

func TestTransactionCommitsAllTables(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "a", nil)
	f.createTable(t, "b", nil)

	err := f.svc.CommitTransaction(t.Context(), []catalog.CommitInput{
		setProps(t, "a", `{"k":"a"}`),
		setProps(t, "b", `{"k":"b"}`),
	})
	if err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	if f.props(t, "a")["k"] != "a" || f.props(t, "b")["k"] != "b" {
		t.Fatalf("properties a=%v b=%v", f.props(t, "a"), f.props(t, "b"))
	}
}

func TestTransactionFailingRequirementWritesNothing(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "a", nil)
	f.createTable(t, "b", nil)
	before := f.files.list()

	stale := setProps(t, "b", `{"k":"b"}`)
	stale.Requirements = parseRequirements(t,
		`[{"type":"assert-table-uuid","uuid":"00000000-0000-0000-0000-00000000dead"}]`)

	err := f.svc.CommitTransaction(t.Context(), []catalog.CommitInput{setProps(t, "a", `{"k":"a"}`), stale})
	if !errors.Is(err, catalog.ErrRequirementFailed) {
		t.Fatalf("CommitTransaction = %v, want ErrRequirementFailed", err)
	}

	if !slices.Equal(f.files.list(), before) {
		t.Fatal("a failed transaction wrote files")
	}

	if _, ok := f.props(t, "a")["k"]; ok {
		t.Fatal("a failed transaction changed table a")
	}
}

func TestTransactionRetriesLostRace(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 3)
	f.createTable(t, "a", nil)
	f.createTable(t, "b", nil)

	lost := false
	f.store.setCommit(func(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
		if !lost {
			lost = true

			return nil, &store.CommitConflictError{Ident: commits[1].Ident}
		}

		return f.store.Store.CommitTables(ctx, commits)
	})

	err := f.svc.CommitTransaction(t.Context(), []catalog.CommitInput{
		setProps(t, "a", `{"k":"a"}`),
		setProps(t, "b", `{"k":"b"}`),
	})
	if err != nil || !lost {
		t.Fatalf("CommitTransaction after a lost race = %v (race lost: %v)", err, lost)
	}

	// Two tables, each with its first and its committed metadata file: the
	// losing attempt's two files were deleted.
	if files := metadataFiles(f.files.list()); len(files) != 4 {
		t.Fatalf("metadata files %q, want 4", files)
	}
}

func TestTransactionUnknownOutcomeKeepsFiles(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 3)
	f.createTable(t, "a", nil)
	f.createTable(t, "b", nil)

	var written []string

	f.store.setCommit(func(_ context.Context, commits []store.Commit) ([]store.Pointer, error) {
		for _, c := range commits {
			written = append(written, c.MetadataLocation)
		}

		return nil, fmt.Errorf("%w: connection reset", store.ErrUnavailable)
	})

	err := f.svc.CommitTransaction(t.Context(), []catalog.CommitInput{
		setProps(t, "a", `{"k":"a"}`),
		setProps(t, "b", `{"k":"b"}`),
	})
	if !errors.Is(err, catalog.ErrCommitStateUnknown) {
		t.Fatalf("CommitTransaction = %v, want ErrCommitStateUnknown", err)
	}

	for _, location := range written {
		if !slices.Contains(f.files.list(), location) {
			t.Fatalf("metadata file %q of a transaction in unknown state was deleted", location)
		}
	}
}

func TestTransactionCreatesTable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "a", nil)

	err := f.svc.CommitTransaction(t.Context(), []catalog.CommitInput{
		setProps(t, "a", `{"k":"a"}`),
		{
			Ident:        ident("created"),
			Requirements: parseRequirements(t, `[{"type":"assert-create"}]`),
			Updates:      parseUpdates(t, stagedCreateUpdates),
		},
	})
	if err != nil {
		t.Fatalf("CommitTransaction: %v", err)
	}

	if f.props(t, "created")["owner"] != "trino" || f.props(t, "a")["k"] != "a" {
		t.Fatal("the transaction did not create the table and update the other")
	}
}

func TestTransactionErrors(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.createTable(t, "a", nil)

	tests := []struct {
		name    string
		changes []catalog.CommitInput
		want    error
	}{
		{name: "empty", changes: []catalog.CommitInput{}, want: catalog.ErrInvalidRequest},
		{
			name:    "table twice",
			changes: []catalog.CommitInput{setProps(t, "a", `{"k":"1"}`), setProps(t, "a", `{"k":"2"}`)},
			want:    catalog.ErrInvalidRequest,
		},
		{
			name:    "missing table",
			changes: []catalog.CommitInput{setProps(t, "a", `{"k":"1"}`), setProps(t, "missing", `{"k":"2"}`)},
			want:    store.ErrTableNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if err := f.svc.CommitTransaction(t.Context(), tt.changes); !errors.Is(err, tt.want) {
				t.Fatalf("CommitTransaction = %v, want %v", err, tt.want)
			}
		})
	}
}

// Overlapping transactions on shared tables: each one lands on all of its
// tables or on none.
func TestTransactionsAreAtomicUnderConcurrency(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 50)
	f.createTable(t, "a", nil)
	f.createTable(t, "b", nil)
	f.createTable(t, "c", nil)

	const writers = 12

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		landed = map[string]bool{}
	)

	for i := range writers {
		wg.Go(func() {
			key := fmt.Sprintf("w%02d", i)
			names := []string{"a", "b"}

			if i%2 == 1 {
				names = []string{"b", "c"}
			}

			changes := make([]catalog.CommitInput, 0, len(names))
			for _, name := range names {
				changes = append(changes, setProps(t, name, `{"`+key+`":"1"}`))
			}

			if err := f.svc.CommitTransaction(t.Context(), changes); err == nil {
				mu.Lock()
				landed[key] = true
				mu.Unlock()
			}
		})
	}

	wg.Wait()

	a, b, c := f.props(t, "a"), f.props(t, "b"), f.props(t, "c")

	for i := range writers {
		key := fmt.Sprintf("w%02d", i)
		first, second := a, b

		if i%2 == 1 {
			first, second = b, c
		}

		_, inFirst := first[key]
		_, inSecond := second[key]

		if inFirst != inSecond || inFirst != landed[key] {
			t.Errorf("%s: on first table %v, on second %v, acknowledged %v", key, inFirst, inSecond, landed[key])
		}
	}

	if len(landed) == 0 {
		t.Fatalf("no transaction landed; properties %v", slices.Sorted(maps.Keys(b)))
	}
}

// A create commit builds from empty metadata of the requested format
// version: an update to version 1 must not be a downgrade from the default.
func TestCommitCreatesTableOfEveryFormatVersion(t *testing.T) {
	t.Parallel()

	for _, version := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(version), func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, 0)
			updates := strings.Replace(stagedCreateUpdates,
				`"format-version":2`, fmt.Sprintf(`"format-version":%d`, version), 1)

			tbl, err := f.commitJSON(t, "created", `[{"type":"assert-create"}]`, updates)
			if err != nil {
				t.Fatalf("create commit of format version %d: %v", version, err)
			}

			if got := parseMetadata(t, tbl).Version(); got != version {
				t.Fatalf("format version %d, want %d", got, version)
			}
		})
	}
}
