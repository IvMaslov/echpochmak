package store

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by [Store] implementations. Each one maps to a
// single Iceberg REST error type in the HTTP layer.
//
// Errors about an entry are formatted as "<sentinel>: <name>", for example
// "Table does not exist: db.t". The wording is the Java reference
// implementation's, which clients show to users and the REST Compatibility
// Kit checks; hence the capitals.
//
//nolint:staticcheck // ST1005: the wording must match the reference implementation.
var (
	ErrNamespaceNotFound      = errors.New("Namespace does not exist")
	ErrNamespaceAlreadyExists = errors.New("Namespace already exists")
	ErrNamespaceNotEmpty      = errors.New("Namespace is not empty")
	ErrTableNotFound          = errors.New("Table does not exist")
	ErrTableAlreadyExists     = errors.New("Table already exists")
	ErrViewNotFound           = errors.New("View does not exist")
	ErrViewAlreadyExists      = errors.New("View already exists")
	// ErrCommitConflict means a pointer is no longer at the expected version.
	// The error is a [*CommitConflictError] with the details.
	ErrCommitConflict = errors.New("commit conflict")
	// ErrUnavailable means the store cannot serve the request right now, for
	// example because a cluster has no leader. The request may be retried.
	// On its own it means a write was certainly not applied.
	ErrUnavailable = errors.New("store unavailable")
	// ErrOutcomeUnknown means a write may or may not have been applied: it
	// was sent, and then the store became unreachable. Errors that match it
	// also match [ErrUnavailable], so callers that only need "the store is
	// unavailable" keep working; callers that clean up after a failed write
	// must check ErrOutcomeUnknown first.
	ErrOutcomeUnknown = errors.New("store write outcome unknown")
	// ErrInvalidArgument means the request is malformed: a bad identifier,
	// page token or commit.
	ErrInvalidArgument = errors.New("invalid argument")
	// ErrPurgeTaskChanged means a purge task was claimed, renewed or completed
	// by someone else since it was read, or no longer exists.
	ErrPurgeTaskChanged = errors.New("purge task changed")
	// ErrIntentChanged means a commit intent was condemned or released since
	// it was read, or no longer exists. A commit naming it fails with this
	// error, and must be retried with a new intent.
	ErrIntentChanged = errors.New("commit intent changed")
)

// OutcomeUnknown returns an error that matches both [ErrOutcomeUnknown] and
// [ErrUnavailable], wrapping cause.
func OutcomeUnknown(cause error) error {
	return fmt.Errorf("%w: %w: %w", ErrOutcomeUnknown, ErrUnavailable, cause)
}

// IsDefinitelyNotApplied reports whether err says that a write certainly did
// not happen because the store was unavailable.
func IsDefinitelyNotApplied(err error) bool {
	return errors.Is(err, ErrUnavailable) && !errors.Is(err, ErrOutcomeUnknown)
}

// CommitConflictError reports a pointer that is not at the version a commit
// expected. It matches [ErrCommitConflict] with [errors.Is].
type CommitConflictError struct {
	Ident    Ident
	Expected Version
	Actual   Version
}

func (e *CommitConflictError) Error() string {
	return fmt.Sprintf(
		"commit conflict on %q: expected version %d, actual %d",
		e.Ident.String(),
		e.Expected,
		e.Actual,
	)
}

// Is makes the error match [ErrCommitConflict].
func (e *CommitConflictError) Is(target error) bool {
	return target == ErrCommitConflict
}
