package client

import (
	"context"
	"fmt"

	"echpochmak/internal/kv/kvpb"
)

// MemberList returns the cluster ID and its members.
func (c *Client) MemberList(ctx context.Context) (*kvpb.MemberListResponse, error) {
	var resp *kvpb.MemberListResponse

	err := c.do(ctx, true, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.cluster[i].MemberList(ctx, &kvpb.MemberListRequest{})

		return err
	})

	return resp, err
}

// MemberAdd adds a node as a learner. Membership changes are not
// deduplicated, so an attempt with an unknown outcome is not retried; check
// MemberList before trying again.
func (c *Client) MemberAdd(ctx context.Context, id uint64, peerURL string) ([]*kvpb.Member, error) {
	var resp *kvpb.MemberAddResponse

	err := c.do(ctx, false, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.cluster[i].MemberAdd(ctx, &kvpb.MemberAddRequest{Id: id, PeerUrl: peerURL})

		return err
	})

	return resp.GetMembers(), err
}

// MemberPromote makes a learner a voter.
func (c *Client) MemberPromote(ctx context.Context, id uint64) ([]*kvpb.Member, error) {
	var resp *kvpb.MemberPromoteResponse

	err := c.do(ctx, false, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.cluster[i].MemberPromote(ctx, &kvpb.MemberPromoteRequest{Id: id})

		return err
	})

	return resp.GetMembers(), err
}

// MemberRemove removes a node from the cluster.
func (c *Client) MemberRemove(ctx context.Context, id uint64) ([]*kvpb.Member, error) {
	var resp *kvpb.MemberRemoveResponse

	err := c.do(ctx, false, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.cluster[i].MemberRemove(ctx, &kvpb.MemberRemoveRequest{Id: id})

		return err
	})

	return resp.GetMembers(), err
}

// LeaderTransfer hands leadership to target, or with 0 to the most
// up-to-date voter, and returns the new leader.
func (c *Client) LeaderTransfer(ctx context.Context, target uint64) (uint64, error) {
	var resp *kvpb.LeaderTransferResponse

	err := c.do(ctx, true, func(ctx context.Context, i int) error {
		var err error

		resp, err = c.cluster[i].LeaderTransfer(ctx, &kvpb.LeaderTransferRequest{TargetId: target})

		return err
	})

	return resp.GetLeaderId(), err
}

// Status returns the status of every node that answers, by endpoint.
// Nodes that do not answer are reported in the error map.
func (c *Client) Status(ctx context.Context) (map[string]*kvpb.StatusResponse, map[string]error) {
	statuses := map[string]*kvpb.StatusResponse{}
	errs := map[string]error{}

	for i, ep := range c.cfg.Endpoints {
		actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
		resp, err := c.cluster[i].Status(actx, &kvpb.StatusRequest{})

		cancel()

		if err != nil {
			errs[ep] = fmt.Errorf("status of %s: %w", ep, err)

			continue
		}

		statuses[ep] = resp
	}

	return statuses, errs
}
