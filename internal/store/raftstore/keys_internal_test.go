package raftstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"echpochmak/internal/kv/client"
	"echpochmak/internal/store"
)

func TestChildPrefixesDoNotLeak(t *testing.T) {
	t.Parallel()

	k := keys{prefix: DefaultPrefix}
	a, ab, aB := store.Namespace{"a"}, store.Namespace{"ab"}, store.Namespace{"a", "b"}
	rootNS := k.children(spaceNamespaces, nil)
	aNS := k.children(spaceNamespaces, a)

	tests := []struct {
		name   string
		key    []byte
		prefix []byte
		inside bool
	}{
		{"child of root", k.namespace(a), rootNS, true},
		{"grandchild is not a child of root", k.namespace(aB), rootNS, false},
		{"child of a", k.namespace(aB), aNS, true},
		{"child of ab is not a child of a", k.namespace(ab.Child("c")), aNS, false},
		{"grandchild is not a child of a", k.namespace(aB.Child("c")), aNS, false},
		{"table of a.b is not a table of a", k.child(spaceTables, aB, "t"), k.children(spaceTables, a), false},
		{"tables and views are apart", k.child(spaceViews, a, "t"), k.children(spaceTables, a), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inRange := bytes.HasPrefix(tt.key, tt.prefix) && bytes.Compare(tt.key, prefixEnd(tt.prefix)) < 0
			if inRange != tt.inside {
				t.Fatalf("key %q in children range %q = %v, want %v", tt.key, tt.prefix, inRange, tt.inside)
			}
		})
	}
}

func TestNameOf(t *testing.T) {
	t.Parallel()

	k := keys{prefix: DefaultPrefix}
	ns := store.Namespace{"a", "b"}

	if got := nameOf(k.child(spaceTables, ns, "orders"), k.children(spaceTables, ns)); got != "orders" {
		t.Fatalf("nameOf = %q, want orders", got)
	}
}

func TestMapErr(t *testing.T) {
	t.Parallel()

	unknown := []error{store.ErrOutcomeUnknown, store.ErrUnavailable}

	tests := []struct {
		in      error
		want    []error
		notWant []error
	}{
		{
			in:   fmt.Errorf("x: %w", client.ErrOutcomeUnknown),
			want: append(slices.Clone(unknown), client.ErrOutcomeUnknown),
		},
		{
			in:      fmt.Errorf("x: %w", client.ErrUnavailable),
			want:    []error{store.ErrUnavailable},
			notWant: []error{store.ErrOutcomeUnknown},
		},
		{in: fmt.Errorf("x: %w", client.ErrInvalidArgument), want: []error{store.ErrInvalidArgument}},
		{
			in:   fmt.Errorf("%w: %w", client.ErrOutcomeUnknown, context.Canceled),
			want: append(slices.Clone(unknown), context.Canceled),
		},
		{in: context.DeadlineExceeded, want: []error{context.DeadlineExceeded}},
	}

	for _, tt := range tests {
		got := mapErr(tt.in)
		for _, want := range tt.want {
			if !errors.Is(got, want) {
				t.Errorf("mapErr(%v) = %v, does not match %v", tt.in, got, want)
			}
		}

		for _, notWant := range tt.notWant {
			if errors.Is(got, notWant) {
				t.Errorf("mapErr(%v) = %v, matches %v", tt.in, got, notWant)
			}
		}
	}
}
