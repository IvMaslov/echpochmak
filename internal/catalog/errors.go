package catalog

import "errors"

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
