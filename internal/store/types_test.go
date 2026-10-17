package store_test

import (
	"errors"
	"testing"

	"echpochmak/internal/store"
)

func TestParseNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		encoded string
		want    store.Namespace
		wantErr bool
	}{
		{encoded: "db", want: store.Namespace{"db"}},
		{encoded: "a\x1fb\x1fc", want: store.Namespace{"a", "b", "c"}},
		{encoded: "", wantErr: true},
		{encoded: "a\x1f", wantErr: true},
		{encoded: "\x1fa", wantErr: true},
		{encoded: "a\x00b", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.want.String(), func(t *testing.T) {
			t.Parallel()

			got, err := store.ParseNamespace(tt.encoded)
			if tt.wantErr {
				if !errors.Is(err, store.ErrInvalidArgument) {
					t.Fatalf("ParseNamespace(%q) error = %v, want ErrInvalidArgument", tt.encoded, err)
				}

				return
			}

			if err != nil || !got.Equal(tt.want) {
				t.Fatalf("ParseNamespace(%q) = %v, %v; want %v", tt.encoded, got, err, tt.want)
			}

			if got.Encode() != tt.encoded {
				t.Fatalf("Encode() = %q, want round trip to %q", got.Encode(), tt.encoded)
			}
		})
	}
}

func TestNamespaceHierarchy(t *testing.T) {
	t.Parallel()

	ns := store.Namespace{"a", "b"}

	if !ns.Parent().Equal(store.Namespace{"a"}) || !ns.Parent().Parent().IsRoot() {
		t.Fatalf("Parent chain of %v is wrong", ns)
	}

	if ns.Name() != "b" || !ns.Child("c").Equal(store.Namespace{"a", "b", "c"}) {
		t.Fatalf("Name/Child of %v is wrong", ns)
	}

	// Child must not alias the parent's backing array.
	parent := make(store.Namespace, 1, 4)
	parent[0] = "p"
	x, y := parent.Child("x"), parent.Child("y")

	if x.Name() != "x" || y.Name() != "y" {
		t.Fatalf("Child aliases its parent: %v, %v", x, y)
	}
}

func TestIdentString(t *testing.T) {
	t.Parallel()

	id := store.Ident{Namespace: store.Namespace{"a", "b"}, Name: "t"}
	if id.String() != "a.b.t" {
		t.Fatalf("String() = %q", id.String())
	}
}

func TestPaginate(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c"}
	key := func(s string) string { return s }

	first, err := store.Paginate(items, key, store.PageRequest{Size: 2})
	if err != nil || len(first.Items) != 2 || first.NextToken == "" {
		t.Fatalf("first page = %+v, %v", first, err)
	}

	second, err := store.Paginate(items, key, store.PageRequest{Size: 2, Token: first.NextToken})
	if err != nil || len(second.Items) != 1 || second.Items[0] != "c" || second.NextToken != "" {
		t.Fatalf("second page = %+v, %v", second, err)
	}

	empty, err := store.Paginate([]string{}, key, store.PageRequest{})
	if err != nil || empty.Items == nil || len(empty.Items) != 0 {
		t.Fatalf("empty page = %+v, %v; want non-nil empty items", empty, err)
	}

	_, err = store.Paginate(items, key, store.PageRequest{Token: "%%%"})
	if !errors.Is(err, store.ErrInvalidArgument) {
		t.Fatalf("malformed token error = %v", err)
	}
}

func TestPageRequestLimit(t *testing.T) {
	t.Parallel()

	tests := map[int]int{
		0:                       store.DefaultPageSize,
		-5:                      store.DefaultPageSize,
		7:                       7,
		store.MaxPageSize + 100: store.MaxPageSize,
	}

	for size, want := range tests {
		if got := (store.PageRequest{Size: size}).Limit(); got != want {
			t.Errorf("Limit() with Size %d = %d, want %d", size, got, want)
		}
	}
}
