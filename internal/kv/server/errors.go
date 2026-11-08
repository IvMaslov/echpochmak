package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/statemachine"
)

// toStatus maps a node error to a gRPC status. Whether a write may have been
// applied travels in an ErrorDetail, because the code alone cannot say it:
// both an unproposed and an undecided write are UNAVAILABLE.
func toStatus(err error) error {
	var notLeader *node.NotLeaderError

	switch {
	case errors.As(err, &notLeader):
		return withDetail(codes.FailedPrecondition, err, &kvpb.ErrorDetail{LeaderId: notLeader.Leader})
	case errors.Is(err, node.ErrOutcomeUnknown):
		return withDetail(codes.Unavailable, err, &kvpb.ErrorDetail{OutcomeUnknown: true})
	case errors.Is(err, node.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, node.ErrInvalidRequest), errors.Is(err, statemachine.ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, node.ErrConfChangePending):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, node.ErrMember):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func withDetail(code codes.Code, err error, detail *kvpb.ErrorDetail) error {
	st, detailErr := status.New(code, err.Error()).WithDetails(detail)
	if detailErr != nil {
		return status.Error(code, err.Error())
	}

	return st.Err()
}
