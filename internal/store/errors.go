package store

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by [Store] implementations. Each one maps to a
// single Iceberg REST error type in the HTTP layer.
var (
	ErrNamespaceNotFound      = errors.New("namespace not found")
	ErrNamespaceAlreadyExists = errors.New("namespace already exists")
	ErrNamespaceNotEmpty      = errors.New("namespace not empty")
	ErrTableNotFound          = errors.New("table not found")
	ErrTableAlreadyExists     = errors.New("table already exists")
	ErrViewNotFound           = errors.New("view not found")
	ErrViewAlreadyExists      = errors.New("view already exists")
	// ErrCommitConflict means a pointer is no longer at the expected version.
	// The error is a [*CommitConflictError] with the details.
	ErrCommitConflict = errors.New("commit conflict")
	// ErrUnavailable means the store cannot serve the request right now, for
	// example because a cluster has no leader. The request may be retried.
	ErrUnavailable = errors.New("store unavailable")
	// ErrInvalidArgument means the request is malformed: a bad identifier,
	// page token or commit.
	ErrInvalidArgument = errors.New("invalid argument")
)

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
