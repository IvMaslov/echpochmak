package store

import (
	"fmt"
	"slices"
	"strings"
	"unicode"
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

	return nil
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
