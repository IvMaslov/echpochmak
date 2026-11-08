package server

import (
	"context"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
)

type clusterService struct {
	kvpb.UnimplementedClusterServiceServer

	n *node.Node
}

func (s *clusterService) header() *kvpb.ResponseHeader {
	return &kvpb.ResponseHeader{MemberId: s.n.ID(), RaftTerm: s.n.Term()}
}

// MemberList implements [kvpb.ClusterServiceServer].
func (s *clusterService) MemberList(context.Context, *kvpb.MemberListRequest) (*kvpb.MemberListResponse, error) {
	return &kvpb.MemberListResponse{
		Header:    s.header(),
		ClusterId: s.n.ClusterID(),
		Members:   s.n.Members(),
	}, nil
}

// MemberAdd implements [kvpb.ClusterServiceServer].
func (s *clusterService) MemberAdd(ctx context.Context, req *kvpb.MemberAddRequest) (*kvpb.MemberAddResponse, error) {
	members, err := s.n.AddMember(ctx, req.GetId(), req.GetPeerUrl())
	if err != nil {
		return nil, toStatus(err)
	}

	return &kvpb.MemberAddResponse{Header: s.header(), Members: members}, nil
}

// MemberPromote implements [kvpb.ClusterServiceServer].
func (s *clusterService) MemberPromote(
	ctx context.Context,
	req *kvpb.MemberPromoteRequest,
) (*kvpb.MemberPromoteResponse, error) {
	members, err := s.n.PromoteMember(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}

	return &kvpb.MemberPromoteResponse{Header: s.header(), Members: members}, nil
}

// MemberRemove implements [kvpb.ClusterServiceServer].
func (s *clusterService) MemberRemove(
	ctx context.Context,
	req *kvpb.MemberRemoveRequest,
) (*kvpb.MemberRemoveResponse, error) {
	members, err := s.n.RemoveMember(ctx, req.GetId())
	if err != nil {
		return nil, toStatus(err)
	}

	return &kvpb.MemberRemoveResponse{Header: s.header(), Members: members}, nil
}

// LeaderTransfer implements [kvpb.ClusterServiceServer].
func (s *clusterService) LeaderTransfer(
	ctx context.Context,
	req *kvpb.LeaderTransferRequest,
) (*kvpb.LeaderTransferResponse, error) {
	if err := s.n.TransferLeadership(ctx, req.GetTargetId()); err != nil {
		return nil, toStatus(err)
	}

	return &kvpb.LeaderTransferResponse{Header: s.header(), LeaderId: s.n.Leader()}, nil
}

// Status implements [kvpb.ClusterServiceServer].
func (s *clusterService) Status(context.Context, *kvpb.StatusRequest) (*kvpb.StatusResponse, error) {
	st := s.n.Status()

	followers := make([]*kvpb.FollowerProgress, 0, len(st.Followers))
	for _, f := range st.Followers {
		followers = append(followers, &kvpb.FollowerProgress{
			Id:      f.ID,
			Match:   f.Match,
			State:   f.State,
			Learner: f.Learner,
		})
	}

	return &kvpb.StatusResponse{
		Header:       s.header(),
		ClusterId:    st.ClusterID,
		LeaderId:     st.Leader,
		RaftState:    st.RaftState,
		CommitIndex:  st.Commit,
		AppliedIndex: st.Applied,
		Followers:    followers,
	}, nil
}
