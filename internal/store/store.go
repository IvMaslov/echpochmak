// Package store defines the persistence contract of the catalog: namespaces
// with their properties, and pointers from table and view identifiers to
// their current metadata file.
//
// The store never reads or writes metadata files. The catalog layer writes a
// new metadata file to object storage and then asks the store to swap the
// pointer, which the store does with compare-and-swap on [Version].
//
// Every implementation must provide:
//   - Linearizable reads and writes: once a write returns, every later read,
//     from any client, observes it.
//   - Atomic multi-table commits: [Store.CommitTables] applies all commits or
//     none.
//   - Monotonic versions: every write gets a version strictly greater than any
//     version issued before, so a version is never reused, even after an entry
//     is dropped and recreated.
//
// The contract is specified by the tests in package storetest, which every
// implementation must pass.
package store

import "context"

// Store is the persistence layer of the catalog.
//
// All methods return errors that can be matched with [errors.Is] against the
// sentinel errors of this package. Any other error is unexpected and should be
// treated as an internal failure.
type Store interface {
	// Ping reports whether the store can serve requests.
	Ping(ctx context.Context) error

	// CreateNamespace creates ns with the given properties. The parent of a
	// nested namespace must exist.
	CreateNamespace(ctx context.Context, ns Namespace, props map[string]string) error
	// GetNamespace returns ns and its properties.
	GetNamespace(ctx context.Context, ns Namespace) (NamespaceRecord, error)
	// ListNamespaces returns the direct children of parent, sorted by name.
	// An empty parent lists the top-level namespaces.
	ListNamespaces(ctx context.Context, parent Namespace, req PageRequest) (Page[Namespace], error)
	// UpdateNamespaceProperties sets and removes properties of ns atomically.
	UpdateNamespaceProperties(
		ctx context.Context,
		ns Namespace,
		upd PropertiesUpdate,
	) (PropertiesUpdateResult, error)
	// DropNamespace removes ns. It fails if ns contains namespaces, tables or
	// views.
	DropNamespace(ctx context.Context, ns Namespace) error

	// GetTable returns the pointer of a table.
	GetTable(ctx context.Context, id Ident) (Pointer, error)
	// ListTables returns the tables of ns, sorted by name.
	ListTables(ctx context.Context, ns Namespace, req PageRequest) (Page[Ident], error)
	// CommitTables applies all commits atomically and returns the new pointers
	// in the order of commits. A commit with ExpectedVersion 0 creates a table.
	CommitTables(ctx context.Context, commits []Commit) ([]Pointer, error)
	// RenameTable moves a table to a new identifier, possibly in another
	// namespace. The target must not be taken by a table or a view.
	RenameTable(ctx context.Context, from, to Ident) error
	// DropTable removes the pointer of a table. Metadata and data files are
	// left untouched.
	DropTable(ctx context.Context, id Ident) error

	// GetView returns the pointer of a view.
	GetView(ctx context.Context, id Ident) (Pointer, error)
	// ListViews returns the views of ns, sorted by name.
	ListViews(ctx context.Context, ns Namespace, req PageRequest) (Page[Ident], error)
	// CommitView applies a single commit to a view. A commit with
	// ExpectedVersion 0 creates a view.
	CommitView(ctx context.Context, commit Commit) (Pointer, error)
	// RenameView moves a view to a new identifier, possibly in another
	// namespace. The target must not be taken by a table or a view.
	RenameView(ctx context.Context, from, to Ident) error
	// DropView removes the pointer of a view.
	DropView(ctx context.Context, id Ident) error
}
