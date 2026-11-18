// Package server serves the client API of a KV node over gRPC: the KV
// service, the cluster service, the maintenance service and the standard
// health service.
package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/node"
	"echpochmak/internal/kv/statemachine"
)

// Defaults for [Options].
const (
	DefaultReadyMaxLag    = 100
	defaultHealthInterval = 500 * time.Millisecond
)

// Options configures the services.
type Options struct {
	// ReadyMaxLag is how far the applied index may trail the commit index
	// for the node to report itself healthy.
	ReadyMaxLag uint64
	// Zone is the node's placement label, reported by Status.
	Zone string
}

// Register adds the KV, cluster and health services of n to s. The health
// status follows the node until ctx is done.
func Register(ctx context.Context, s *grpc.Server, n *node.Node, opts Options) {
	if opts.ReadyMaxLag == 0 {
		opts.ReadyMaxLag = DefaultReadyMaxLag
	}

	kvpb.RegisterKVServiceServer(s, &kvService{n: n})
	kvpb.RegisterClusterServiceServer(s, &clusterService{n: n, zone: opts.Zone})
	kvpb.RegisterMaintenanceServiceServer(s, &maintenanceService{n: n})

	hs := health.NewServer()
	healthpb.RegisterHealthServer(s, hs)

	go watchHealth(ctx, n, hs, opts.ReadyMaxLag)
}

// watchHealth reports SERVING while the node knows a leader and has applied
// nearly everything that is committed.
func watchHealth(ctx context.Context, n *node.Node, hs *health.Server, maxLag uint64) {
	t := time.NewTicker(defaultHealthInterval)
	defer t.Stop()

	for {
		status := healthpb.HealthCheckResponse_NOT_SERVING
		if Ready(n, maxLag) {
			status = healthpb.HealthCheckResponse_SERVING
		}

		hs.SetServingStatus("", status)

		select {
		case <-ctx.Done():
			hs.Shutdown()

			return
		case <-n.Done():
			hs.Shutdown()

			return
		case <-t.C:
		}
	}
}

type kvService struct {
	kvpb.UnimplementedKVServiceServer

	n *node.Node
}

func (s *kvService) header(h *kvpb.ResponseHeader) *kvpb.ResponseHeader {
	if h == nil {
		h = &kvpb.ResponseHeader{}
	}

	h.MemberId = s.n.ID()
	h.RaftTerm = s.n.Term()

	return h
}

// Range implements [kvpb.KVServiceServer].
func (s *kvService) Range(ctx context.Context, req *kvpb.RangeRequest) (*kvpb.RangeResponse, error) {
	var resp *kvpb.RangeResponse

	err := s.n.Read(ctx, req.GetSerializable(), func(v *statemachine.View) error {
		var err error

		resp, err = v.Range(req)

		return err
	})
	if err != nil {
		return nil, toStatus(err)
	}

	resp.Header = s.header(resp.GetHeader())

	return resp, nil
}

// Put implements [kvpb.KVServiceServer].
func (s *kvService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	res, err := s.n.Propose(ctx, &kvpb.Proposal{
		RequestId: req.GetRequestId(),
		Operation: &kvpb.Proposal_Put{Put: req},
	})
	if err != nil {
		return nil, toStatus(err)
	}

	resp := res.GetPut()
	resp.Header = s.header(resp.GetHeader())

	return resp, nil
}

// DeleteRange implements [kvpb.KVServiceServer].
func (s *kvService) DeleteRange(ctx context.Context, req *kvpb.DeleteRangeRequest) (*kvpb.DeleteRangeResponse, error) {
	res, err := s.n.Propose(ctx, &kvpb.Proposal{
		RequestId: req.GetRequestId(),
		Operation: &kvpb.Proposal_DeleteRange{DeleteRange: req},
	})
	if err != nil {
		return nil, toStatus(err)
	}

	resp := res.GetDeleteRange()
	resp.Header = s.header(resp.GetHeader())

	return resp, nil
}

// Txn implements [kvpb.KVServiceServer]. A transaction that only reads is
// served like a read; any other goes through raft.
func (s *kvService) Txn(ctx context.Context, req *kvpb.TxnRequest) (*kvpb.TxnResponse, error) {
	if statemachine.IsReadOnly(req) {
		var resp *kvpb.TxnResponse

		err := s.n.Read(ctx, req.GetSerializable(), func(v *statemachine.View) error {
			var err error

			resp, err = v.Txn(req)

			return err
		})
		if err != nil {
			return nil, toStatus(err)
		}

		resp.Header = s.header(resp.GetHeader())

		return resp, nil
	}

	res, err := s.n.Propose(ctx, &kvpb.Proposal{
		RequestId: req.GetRequestId(),
		Operation: &kvpb.Proposal_Txn{Txn: req},
	})
	if err != nil {
		return nil, toStatus(err)
	}

	resp := res.GetTxn()
	resp.Header = s.header(resp.GetHeader())

	return resp, nil
}
