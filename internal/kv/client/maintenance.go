package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	"echpochmak/internal/kv/kvpb"
)

// ExportSnapshot streams a consistent snapshot from the node at endpoint,
// one of the configured endpoints, in the snapshot file format, and returns
// its header. The snapshot holds every write acknowledged before the call.
//
// open is called with the header before any byte of the file, and returns
// where the file goes; an error from open ends the stream and is returned,
// which lets a caller skip a snapshot it already has. A failed stream is not
// retried: the destination may hold part of a file, which the caller
// discards.
func (c *Client) ExportSnapshot(
	ctx context.Context,
	endpoint string,
	open func(*kvpb.SnapshotHeader) (io.Writer, error),
) (*kvpb.SnapshotHeader, error) {
	i := slices.Index(c.cfg.Endpoints, endpoint)
	if i < 0 {
		return nil, fmt.Errorf("%q is not one of the client's endpoints", endpoint)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := kvpb.NewMaintenanceServiceClient(c.conns[i]).ExportSnapshot(ctx, &kvpb.ExportSnapshotRequest{})
	if err != nil {
		return nil, fmt.Errorf("snapshot from %s: %w", endpoint, err)
	}

	first, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("snapshot from %s: %w", endpoint, err)
	}

	header := first.GetHeader()
	if header == nil {
		return nil, fmt.Errorf("snapshot from %s: the stream does not start with a header", endpoint)
	}

	w, err := open(header)
	if err != nil {
		return header, err
	}

	for resp := first; ; {
		if _, err := w.Write(resp.GetChunk()); err != nil {
			return nil, fmt.Errorf("writing the snapshot: %w", err)
		}

		resp, err = stream.Recv()
		if errors.Is(err, io.EOF) {
			return header, nil
		}

		if err != nil {
			return nil, fmt.Errorf("snapshot from %s: %w", endpoint, err)
		}
	}
}

// To returns an open function for [Client.ExportSnapshot] that writes to w.
func To(w io.Writer) func(*kvpb.SnapshotHeader) (io.Writer, error) {
	return func(*kvpb.SnapshotHeader) (io.Writer, error) { return w, nil }
}
