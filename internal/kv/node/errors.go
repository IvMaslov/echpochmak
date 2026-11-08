package node

import (
	"errors"
	"fmt"
)

// Errors returned by a Node. They tell a caller whether a write may have been
// applied, which decides whether a retry needs the same request ID.
var (
	// ErrUnavailable means the request was certainly not applied: there is
	// no leader, the node is shutting down, or a read could not be
	// confirmed. Any retry is safe.
	ErrUnavailable = errors.New("unavailable")
	// ErrOutcomeUnknown means a write was proposed but its fate is unknown:
	// it may be applied later or never. Retry with the same request ID.
	ErrOutcomeUnknown = errors.New("outcome unknown")
	// ErrInvalidRequest means the request was rejected before it was
	// proposed, or every node rejected it when applying it.
	ErrInvalidRequest = errors.New("invalid request")
	// ErrConfChangePending means another membership change is in progress.
	ErrConfChangePending = errors.New("another membership change is in progress")
	// ErrMember means a membership change does not fit the current members:
	// adding a node that exists, promoting one that is not a learner, and so
	// on.
	ErrMember = errors.New("membership change rejected")
)

// NotLeaderError is returned by operations that only the leader can perform.
type NotLeaderError struct {
	// Leader is the current leader, or 0 if there is none.
	Leader uint64
}

func (e *NotLeaderError) Error() string {
	if e.Leader == 0 {
		return "not the leader, and no leader is known"
	}

	return fmt.Sprintf("not the leader; the leader is node %d", e.Leader)
}
