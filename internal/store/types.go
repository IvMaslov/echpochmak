package store

import (
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"
)

// NamespaceSeparator separates namespace levels in their encoded form. It is
// the unit separator used by the Iceberg REST spec in URL paths.
const NamespaceSeparator = "\x1f"

// Namespace is a hierarchical namespace, from the outermost level inwards.
// The empty namespace is the root, which contains the top-level namespaces.
type Namespace []string

// ParseNamespace decodes a namespace from its encoded form, as produced by
// [Namespace.Encode], and validates it.
func ParseNamespace(encoded string) (Namespace, error) {
	if encoded == "" {
		return nil, fmt.Errorf("%w: empty namespace", ErrInvalidArgument)
	}

	ns := Namespace(strings.Split(encoded, NamespaceSeparator))
	if err := ns.Validate(); err != nil {
		return nil, err
	}

	return ns, nil
}

// Validate reports whether ns is a valid, non-root namespace: it has at least
// one level, and no level is empty or contains control characters.
func (ns Namespace) Validate() error {
	if len(ns) == 0 {
		return fmt.Errorf("%w: empty namespace", ErrInvalidArgument)
	}

	for _, level := range ns {
		if err := validateName(level); err != nil {
			return fmt.Errorf("namespace %q: %w", ns.String(), err)
		}
	}

	return nil
}

// IsRoot reports whether ns is the root namespace.
func (ns Namespace) IsRoot() bool {
	return len(ns) == 0
}

// Parent returns the namespace that contains ns. The parent of a top-level
// namespace is the root.
func (ns Namespace) Parent() Namespace {
	if len(ns) == 0 {
		return Namespace{}
	}

	return slices.Clone(ns[:len(ns)-1])
}

// Name returns the innermost level of ns, or "" for the root.
func (ns Namespace) Name() string {
	if len(ns) == 0 {
		return ""
	}

	return ns[len(ns)-1]
}

// Child returns the namespace named name directly inside ns.
func (ns Namespace) Child(name string) Namespace {
	child := make(Namespace, 0, len(ns)+1)
	child = append(child, ns...)

	return append(child, name)
}

// Equal reports whether ns and other have the same levels.
func (ns Namespace) Equal(other Namespace) bool {
	return slices.Equal(ns, other)
}

// Encode returns the levels of ns joined by [NamespaceSeparator]. Validated
// levels never contain the separator, so the encoding is unambiguous.
func (ns Namespace) Encode() string {
	return strings.Join(ns, NamespaceSeparator)
}

// String returns the levels of ns joined by dots, for logs and messages.
func (ns Namespace) String() string {
	return strings.Join(ns, ".")
}

// Ident identifies a table or a view.
type Ident struct {
	Namespace Namespace
	Name      string
}

// Validate reports whether id has a valid namespace and name.
func (id Ident) Validate() error {
	if err := id.Namespace.Validate(); err != nil {
		return err
	}

	if err := validateName(id.Name); err != nil {
		return fmt.Errorf("identifier %q: %w", id.String(), err)
	}

	return nil
}

// Equal reports whether id and other identify the same entry.
func (id Ident) Equal(other Ident) bool {
	return id.Name == other.Name && id.Namespace.Equal(other.Namespace)
}

// String returns the namespace and name of id joined by dots.
func (id Ident) String() string {
	if id.Namespace.IsRoot() {
		return id.Name
	}

	return id.Namespace.String() + "." + id.Name
}

// Version identifies a state of a store entry. Versions are issued by the
// store and strictly increase with every write. Zero means "does not exist".
type Version int64

// NamespaceRecord is a stored namespace.
type NamespaceRecord struct {
	Namespace  Namespace
	Properties map[string]string
	Version    Version
}

// Pointer points a table or view identifier at its current metadata file.
type Pointer struct {
	Ident            Ident
	MetadataLocation string
	Version          Version
}

// Commit is a compare-and-swap of a single pointer: it succeeds only if the
// pointer is still at ExpectedVersion. ExpectedVersion 0 means the pointer
// must not exist yet, and the commit creates it.
type Commit struct {
	Ident            Ident
	ExpectedVersion  Version
	MetadataLocation string
	// Intent is the commit intent that recorded MetadataLocation before the
	// file was written; zero for none. The commit fails with
	// [ErrIntentChanged] unless the intent is still at its version, and
	// removes the intent atomically with the pointer swap. Every commit of
	// one call must name the same intent, or none.
	Intent IntentRef
}

// IsCreate reports whether c creates a new pointer.
func (c Commit) IsCreate() bool {
	return c.ExpectedVersion == 0
}

// Validate reports whether c is well-formed.
func (c Commit) Validate() error {
	if err := c.Ident.Validate(); err != nil {
		return err
	}

	if c.ExpectedVersion < 0 {
		return fmt.Errorf("%w: negative expected version for %q", ErrInvalidArgument, c.Ident.String())
	}

	if c.MetadataLocation == "" {
		return fmt.Errorf("%w: empty metadata location for %q", ErrInvalidArgument, c.Ident.String())
	}

	if !c.Intent.IsZero() && (c.Intent.ID == "" || c.Intent.Version <= 0) {
		return fmt.Errorf("%w: malformed intent for %q", ErrInvalidArgument, c.Ident.String())
	}

	return nil
}

// CommitsIntent returns the intent that commits name, which must be the same
// for all of them; zero if they name none.
func CommitsIntent(commits []Commit) (IntentRef, error) {
	if len(commits) == 0 {
		return IntentRef{}, nil
	}

	ref := commits[0].Intent

	for _, c := range commits[1:] {
		if c.Intent != ref {
			return IntentRef{}, fmt.Errorf("%w: commits of one call name different intents", ErrInvalidArgument)
		}
	}

	return ref, nil
}

// PropertiesUpdate sets and removes namespace properties. A key must not
// appear in both.
type PropertiesUpdate struct {
	Updates  map[string]string
	Removals []string
}

// Validate reports whether upd is well-formed.
func (upd PropertiesUpdate) Validate() error {
	for _, key := range upd.Removals {
		if _, ok := upd.Updates[key]; ok {
			return fmt.Errorf("%w: property %q is both updated and removed", ErrInvalidArgument, key)
		}
	}

	return nil
}

// PropertiesUpdateResult reports the outcome of a [PropertiesUpdate], as the
// Iceberg REST spec requires. All slices are sorted.
type PropertiesUpdateResult struct {
	// Updated lists the keys that were set.
	Updated []string
	// Removed lists the keys that were removed.
	Removed []string
	// Missing lists the keys requested for removal that did not exist.
	Missing []string
}

func validateName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: empty name", ErrInvalidArgument)
	}

	if strings.ContainsFunc(name, unicode.IsControl) {
		return fmt.Errorf("%w: name %q contains control characters", ErrInvalidArgument, name)
	}

	return nil
}

// DropOptions configures [Store.DropTable].
type DropOptions struct {
	// Purge enqueues a [PurgeTask] for the dropped table's files.
	Purge bool
}

// PurgeTask is a queued deletion of a dropped table's files.
type PurgeTask struct {
	// ID identifies the task; the store assigns it.
	ID string
	// Ident is the dropped table, for logs.
	Ident Ident
	// MetadataLocation is the table's last metadata file, where the purge
	// starts collecting files.
	MetadataLocation string
	// Created is when the table was dropped.
	Created time.Time
	// Claim is the current claim; zero when the task is unclaimed.
	Claim PurgeClaim
	// Version changes with every claim; claims and completion compare it.
	Version Version
}

// PurgeClaim is a worker's lease on a [PurgeTask].
type PurgeClaim struct {
	// Owner names the worker, for logs.
	Owner string
	// Until is when the lease expires and another worker may take over.
	Until time.Time
}

// Claimable reports whether the task may be claimed at now: it is unclaimed,
// or its claim has expired.
func (t PurgeTask) Claimable(now time.Time) bool {
	return t.Claim.Until.IsZero() || !now.Before(t.Claim.Until)
}

// NewPurgeTaskID returns a new task ID. IDs are UUIDv7 strings, so sorting
// them sorts tasks by creation time.
func NewPurgeTaskID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating a purge task ID: %w", err)
	}

	return id.String(), nil
}

// Intent records the metadata files that a commit attempt is about to write,
// before it writes them. A successful commit removes its intent in the same
// atomic write that moves the pointers, so an intent that outlives its
// attempt names files that no pointer leads to: orphans, which the janitor
// deletes. See arch/phase5/plan.md (D5).
type Intent struct {
	// ID identifies the intent; the store assigns it.
	ID string
	// Locations are the metadata files the attempt writes.
	Locations []string
	// Idents are the tables or views being committed, for logs.
	Idents []Ident
	// Owner names the replica that created the intent, for logs.
	Owner string
	// Created is when the store recorded the intent.
	Created time.Time
	// Condemned marks an intent that the janitor is deleting the files of. A
	// commit naming it can no longer succeed.
	Condemned bool
	// Version changes when the intent is condemned; commits, condemning and
	// releasing compare it.
	Version Version
}

// Validate reports whether i can be created: it names at least one
// location, and none is empty.
func (i Intent) Validate() error {
	if len(i.Locations) == 0 {
		return fmt.Errorf("%w: an intent needs at least one location", ErrInvalidArgument)
	}

	if slices.Contains(i.Locations, "") {
		return fmt.Errorf("%w: empty intent location", ErrInvalidArgument)
	}

	return nil
}

// Ref returns the reference that commits and releases use.
func (i Intent) Ref() IntentRef {
	return IntentRef{ID: i.ID, Version: i.Version}
}

// IntentRef names an intent at one version.
type IntentRef struct {
	ID      string
	Version Version
}

// IsZero reports whether r names no intent.
func (r IntentRef) IsZero() bool {
	return r == IntentRef{}
}

// NewIntentID returns a new intent ID: a UUIDv7 string, so sorting IDs sorts
// intents by creation time.
func NewIntentID() (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generating an intent ID: %w", err)
	}

	return id.String(), nil
}
