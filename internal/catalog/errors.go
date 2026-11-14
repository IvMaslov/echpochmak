package catalog

import (
	"errors"
	"fmt"

	"echpochmak/internal/store"
)

// Errors returned by the catalog in addition to the [store] sentinel errors,
// which are passed through unchanged.
var (
	// ErrRequirementFailed means a commit requirement does not hold for the
	// current table state; the client must refresh and retry.
	ErrRequirementFailed = errors.New("commit requirement failed")
	// ErrInvalidRequest means the request is well-formed JSON but invalid for
	// Iceberg: a bad schema, partition spec, sort order or table update.
	ErrInvalidRequest = errors.New("invalid request")
	// ErrPropertyConflict means a namespace property is both set and removed
	// in one update.
	ErrPropertyConflict = errors.New("property is both updated and removed")
	// ErrCommitStateUnknown means the new metadata was written but the store
	// failed in a way that leaves it unknown whether the pointer moved. The
	// client must not clean up the files of this commit.
	ErrCommitStateUnknown = errors.New("commit state unknown")
	// ErrMetadataMissing means a table points at a metadata file that does not
	// exist, which indicates data loss or manual tampering.
	ErrMetadataMissing = errors.New("table metadata file is missing")
)

// messageError replaces the message of err and still matches it with
// [errors.Is], for messages worded as the Java reference implementation words
// them: clients show them, and the REST Compatibility Kit checks them.
type messageError struct {
	msg string
	err error
}

func (e *messageError) Error() string { return e.msg }

func (e *messageError) Unwrap() error { return e.err }

func withMessage(err error, format string, args ...any) error {
	return &messageError{msg: fmt.Sprintf(format, args...), err: err}
}

// tableNameClash rewords a failure to create a table at id because a view has
// the name.
func tableNameClash(err error, id store.Ident) error {
	if errors.Is(err, store.ErrViewAlreadyExists) {
		return withMessage(err, "View with same name already exists: %s", id.String())
	}

	return err
}

// viewNameClash rewords a failure to create a view at id because a table has
// the name.
func viewNameClash(err error, id store.Ident) error {
	if errors.Is(err, store.ErrTableAlreadyExists) {
		return withMessage(err, "Table with same name already exists: %s", id.String())
	}

	return err
}

// renameClash rewords a failed rename onto a taken name.
func renameClash(err error, from, to store.Ident) error {
	switch {
	case errors.Is(err, store.ErrTableAlreadyExists):
		return withMessage(err, "Cannot rename %s to %s. Table already exists", from.String(), to.String())
	case errors.Is(err, store.ErrViewAlreadyExists):
		return withMessage(err, "Cannot rename %s to %s. View already exists", from.String(), to.String())
	default:
		return err
	}
}
