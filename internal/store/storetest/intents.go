package storetest

import (
	"errors"
	"slices"
	"testing"

	"echpochmak/internal/store"
)

func testIntents(t *testing.T, newStore Factory) {
	t.Parallel()

	t.Run("create list condemn release", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		intent := mustCreateIntent(t, s, "s3://w/db/t/metadata/1.json")

		if intent.ID == "" || intent.Version <= 0 || intent.Created.IsZero() || intent.Condemned {
			t.Fatalf("created intent = %+v, want an ID, a version, a creation time and not condemned", intent)
		}

		listed := mustIntents(t, s, 10)
		if len(listed) != 1 || listed[0].ID != intent.ID || listed[0].Version != intent.Version {
			t.Fatalf("listed = %+v, want the created intent", listed)
		}

		if !slices.Equal(listed[0].Locations, intent.Locations) || listed[0].Owner != "replica-a" {
			t.Fatalf("listed = %+v, want its locations and owner", listed[0])
		}

		condemned, err := s.CondemnIntent(t.Context(), intent.Ref())
		mustNoErr(t, err)

		if !condemned.Condemned || condemned.Version <= intent.Version {
			t.Fatalf("condemned = %+v, want condemned at a newer version", condemned)
		}

		if got := mustIntents(t, s, 10); len(got) != 1 || !got[0].Condemned {
			t.Fatalf("listed after condemning = %+v, want one condemned intent", got)
		}

		mustNoErr(t, s.ReleaseIntent(t.Context(), condemned.Ref()))

		if got := mustIntents(t, s, 10); len(got) != 0 {
			t.Fatalf("listed after release = %+v, want none", got)
		}
	})

	t.Run("stale versions are rejected", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		intent := mustCreateIntent(t, s, "s3://w/a.json")

		_, err := s.CondemnIntent(t.Context(), intent.Ref())
		mustNoErr(t, err)

		_, err = s.CondemnIntent(t.Context(), intent.Ref())
		mustErrIs(t, err, store.ErrIntentChanged)
		mustErrIs(t, s.ReleaseIntent(t.Context(), intent.Ref()), store.ErrIntentChanged)

		missing := store.IntentRef{ID: "missing", Version: 1}
		mustErrIs(t, s.ReleaseIntent(t.Context(), missing), store.ErrIntentChanged)
	})

	t.Run("oldest first, up to the limit", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		first := mustCreateIntent(t, s, "s3://w/1.json")
		second := mustCreateIntent(t, s, "s3://w/2.json")
		mustCreateIntent(t, s, "s3://w/3.json")

		got := mustIntents(t, s, 2)
		if len(got) != 2 || got[0].ID != first.ID || got[1].ID != second.ID {
			t.Fatalf("listed = %+v, want the two oldest intents in order", got)
		}
	})

	t.Run("invalid arguments", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)

		_, err := s.CreateIntent(t.Context(), store.Intent{})
		mustErrIs(t, err, store.ErrInvalidArgument)

		_, err = s.CreateIntent(t.Context(), store.Intent{Locations: []string{""}})
		mustErrIs(t, err, store.ErrInvalidArgument)

		_, err = s.ListIntents(t.Context(), 0)
		mustErrIs(t, err, store.ErrInvalidArgument)
	})

	t.Run("a table commit removes its intent", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		intent := mustCreateIntent(t, s, "s3://w/db/t/1.json")

		_, err := s.CommitTables(t.Context(), []store.Commit{{
			Ident:            ident("db", "t"),
			MetadataLocation: "s3://w/db/t/1.json",
			Intent:           intent.Ref(),
		}})
		mustNoErr(t, err)

		if got := mustIntents(t, s, 10); len(got) != 0 {
			t.Fatalf("intents after the commit = %+v, want none", got)
		}

		mustErrIs(t, s.ReleaseIntent(t.Context(), intent.Ref()), store.ErrIntentChanged)
	})

	t.Run("a view commit removes its intent", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		intent := mustCreateIntent(t, s, "s3://w/db/v/1.json")

		_, err := s.CommitView(t.Context(), store.Commit{
			Ident:            ident("db", "v"),
			MetadataLocation: "s3://w/db/v/1.json",
			Intent:           intent.Ref(),
		})
		mustNoErr(t, err)

		if got := mustIntents(t, s, 10); len(got) != 0 {
			t.Fatalf("intents after the commit = %+v, want none", got)
		}
	})

	t.Run("a condemned intent fails the commit", func(t *testing.T) {
		t.Parallel()

		testCondemnedCommit(t, newStore)
	})

	t.Run("a failing pointer is reported first, and keeps the intent", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		current := mustCreateTable(t, s, ident("db", "t"))
		intent := mustCreateIntent(t, s, "s3://w/db/t/2.json")

		_, err := s.CommitTables(t.Context(), []store.Commit{{
			Ident:            ident("db", "t"),
			ExpectedVersion:  current.Version - 1,
			MetadataLocation: "s3://w/db/t/2.json",
			Intent:           intent.Ref(),
		}})
		mustErrIs(t, err, store.ErrCommitConflict)

		if errors.Is(err, store.ErrIntentChanged) {
			t.Fatalf("error = %v, want only a conflict", err)
		}

		if got := mustIntents(t, s, 10); len(got) != 1 || got[0].Version != intent.Version {
			t.Fatalf("intents after the failed commit = %+v, want the intent unchanged", got)
		}
	})

	t.Run("one call names one intent", func(t *testing.T) {
		t.Parallel()

		s := newStore(t)
		mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
		a := mustCreateIntent(t, s, "s3://w/a.json")
		b := mustCreateIntent(t, s, "s3://w/b.json")

		_, err := s.CommitTables(t.Context(), []store.Commit{
			{Ident: ident("db", "a"), MetadataLocation: "s3://w/a.json", Intent: a.Ref()},
			{Ident: ident("db", "b"), MetadataLocation: "s3://w/b.json", Intent: b.Ref()},
		})
		mustErrIs(t, err, store.ErrInvalidArgument)
	})
}

// testCondemnedCommit checks that a commit holding an intent that was
// condemned meanwhile fails, and writes nothing.
func testCondemnedCommit(t *testing.T, newStore Factory) {
	t.Helper()

	s := newStore(t)
	mustCreateNamespace(t, s, store.Namespace{"db"}, nil)
	before := mustCreateTable(t, s, ident("db", "t"))
	intent := mustCreateIntent(t, s, "s3://w/db/t/2.json", "s3://w/db/u/1.json")

	_, err := s.CondemnIntent(t.Context(), intent.Ref())
	mustNoErr(t, err)

	_, err = s.CommitTables(t.Context(), []store.Commit{
		{
			Ident:            ident("db", "t"),
			ExpectedVersion:  before.Version,
			MetadataLocation: "s3://w/db/t/2.json",
			Intent:           intent.Ref(),
		},
		{Ident: ident("db", "u"), MetadataLocation: "s3://w/db/u/1.json", Intent: intent.Ref()},
	})
	mustErrIs(t, err, store.ErrIntentChanged)

	after, err := s.GetTable(t.Context(), ident("db", "t"))
	mustNoErr(t, err)

	if !samePointer(after, before) {
		t.Fatalf("table after the failed commit = %+v, want %+v", after, before)
	}

	_, err = s.GetTable(t.Context(), ident("db", "u"))
	mustErrIs(t, err, store.ErrTableNotFound)

	_, err = s.CommitView(t.Context(), store.Commit{
		Ident:            ident("db", "v"),
		MetadataLocation: "s3://w/db/v/1.json",
		Intent:           intent.Ref(),
	})
	mustErrIs(t, err, store.ErrIntentChanged)
}

func mustCreateIntent(t *testing.T, s store.Store, locations ...string) store.Intent {
	t.Helper()

	intent, err := s.CreateIntent(t.Context(), store.Intent{
		Locations: locations,
		Idents:    []store.Ident{ident("db", "t")},
		Owner:     "replica-a",
	})
	if err != nil {
		t.Fatalf("CreateIntent: %v", err)
	}

	return intent
}

func mustIntents(t *testing.T, s store.Store, limit int) []store.Intent {
	t.Helper()

	intents, err := s.ListIntents(t.Context(), limit)
	mustNoErr(t, err)

	return intents
}
