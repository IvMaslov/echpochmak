// Package client is the Go client of the KV cluster.
//
// It connects to every configured node and sends each call to one of them,
// moving to the next node when a call fails because that node cannot serve.
// Writes carry a request ID that stays the same across retries, so the
// cluster applies a retried write at most once; that makes it safe to retry
// a write whose outcome was unknown, and usually turns the unknown outcome
// into a known one.
package client

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	mrand "math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"echpochmak/internal/kv/kvpb"
)

// Errors returned by the client, matched with [errors.Is].
var (
	// ErrUnavailable means no node could serve the request before the
	// deadline, and the request was certainly not applied.
	ErrUnavailable = errors.New("kv cluster unavailable")
	// ErrOutcomeUnknown means a write may or may not have been applied, and
	// retries could not find out before the deadline.
	ErrOutcomeUnknown = errors.New("kv write outcome unknown")
	// ErrInvalidArgument means the cluster rejected the request as invalid.
	ErrInvalidArgument = errors.New("invalid kv request")
	// ErrRejected means the cluster refused a membership change that does not
	// fit the current members.
	ErrRejected = errors.New("kv request rejected")
)

// Defaults for [Config].
const (
	DefaultAttemptTimeout = 5 * time.Second
	DefaultBackoff        = 50 * time.Millisecond
	DefaultMaxBackoff     = time.Second
	requestIDBytes        = 16
)

// Config configures a Client.
type Config struct {
	// Endpoints are the client addresses of the nodes, as host:port or
	// http://host:port.
	Endpoints []string
	// AttemptTimeout bounds each attempt on one node; the caller's context
	// bounds the call as a whole.
	AttemptTimeout time.Duration
	Backoff        time.Duration
	MaxBackoff     time.Duration
	Logger         *slog.Logger
}

// Client talks to a KV cluster. It is safe for concurrent use.
type Client struct {
	cfg     Config
	conns   []*grpc.ClientConn
	kv      []kvpb.KVServiceClient
	cluster []kvpb.ClusterServiceClient
	current atomic.Int64
}

// New connects to the cluster. Connections are established lazily.
func New(cfg Config) (*Client, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("no endpoints")
	}

	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = DefaultAttemptTimeout
	}

	if cfg.Backoff <= 0 {
		cfg.Backoff = DefaultBackoff
	}

	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = DefaultMaxBackoff
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	c := &Client{cfg: cfg}

	for _, ep := range cfg.Endpoints {
		conn, err := grpc.NewClient(target(ep), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			_ = c.Close()

			return nil, fmt.Errorf("connecting to %s: %w", ep, err)
		}

		c.conns = append(c.conns, conn)
		c.kv = append(c.kv, kvpb.NewKVServiceClient(conn))
		c.cluster = append(c.cluster, kvpb.NewClusterServiceClient(conn))
	}

	return c, nil
}

// Close closes every connection.
func (c *Client) Close() error {
	errs := make([]error, 0, len(c.conns))

	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}

	return errors.Join(errs...)
}

// Endpoints returns the configured endpoints.
func (c *Client) Endpoints() []string {
	return c.cfg.Endpoints
}

func target(endpoint string) string {
	for _, scheme := range []string{"http://", "https://"} {
		endpoint = strings.TrimPrefix(endpoint, scheme)
	}

	return strings.TrimSuffix(endpoint, "/")
}

// NewRequestID returns a random request ID for a write.
func NewRequestID() []byte {
	id := make([]byte, requestIDBytes)
	// crypto/rand.Read never fails; it crashes the program if the system
	// random source does.
	_, _ = rand.Read(id)

	return id
}

// outcome classifies a failed attempt.
type outcome int

const (
	// final: return the error to the caller.
	final outcome = iota
	// retryElsewhere: the request was not applied; try another node.
	retryElsewhere
	// unknown: a write may have been applied; retry with the same ID.
	unknown
)

func classify(err error) outcome {
	st, ok := status.FromError(err)
	if !ok {
		return retryElsewhere
	}

	switch st.Code() {
	case codes.Unavailable:
		for _, d := range st.Details() {
			if detail, ok := d.(*kvpb.ErrorDetail); ok && detail.GetOutcomeUnknown() {
				return unknown
			}
		}

		return retryElsewhere
	case codes.DeadlineExceeded, codes.Canceled:
		// The attempt timed out, possibly after the node proposed the write.
		return unknown
	case codes.FailedPrecondition:
		if leaderHint(st) {
			return retryElsewhere
		}

		return final
	case codes.Aborted:
		return retryElsewhere
	default:
		return final
	}
}

// leaderHint reports whether a FAILED_PRECONDITION means "ask the leader".
func leaderHint(st *status.Status) bool {
	for _, d := range st.Details() {
		if _, ok := d.(*kvpb.ErrorDetail); ok {
			return true
		}
	}

	return false
}

func finalError(err error) error {
	st, _ := status.FromError(err)

	switch st.Code() {
	case codes.InvalidArgument:
		return fmt.Errorf("%w: %s", ErrInvalidArgument, st.Message())
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", ErrRejected, st.Message())
	default:
		return fmt.Errorf("kv request failed: %w", err)
	}
}

// do runs call on one node after another until it succeeds, fails for good,
// or ctx ends. retryUnknown says whether an attempt with an unknown outcome
// may be retried, which needs deduplication.
func (c *Client) do(ctx context.Context, retryUnknown bool, call func(ctx context.Context, i int) error) error {
	n := len(c.conns)
	start := int(c.current.Load()) % n
	backoff := c.cfg.Backoff
	sawUnknown := false

	for attempt := 0; ; attempt++ {
		i := (start + attempt) % n

		actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
		err := call(actx, i)

		cancel()

		if err == nil {
			c.current.Store(int64(i))

			return nil
		}

		switch classify(err) {
		case final:
			return finalError(err)
		case unknown:
			sawUnknown = true

			if !retryUnknown {
				return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
			}
		case retryElsewhere:
		}

		c.cfg.Logger.DebugContext(ctx, "kv attempt failed",
			slog.String("endpoint", c.cfg.Endpoints[i]),
			slog.Any("error", err),
		)

		// Pause after each full round over the nodes.
		if (attempt+1)%n == 0 {
			if err := sleep(ctx, jitter(backoff)); err != nil {
				return c.gaveUp(sawUnknown, err)
			}

			backoff = min(2*backoff, c.cfg.MaxBackoff)
		}

		if ctx.Err() != nil {
			return c.gaveUp(sawUnknown, ctx.Err())
		}
	}
}

func (c *Client) gaveUp(sawUnknown bool, err error) error {
	if sawUnknown {
		return fmt.Errorf("%w: %w", ErrOutcomeUnknown, err)
	}

	return fmt.Errorf("%w: %w", ErrUnavailable, err)
}

func jitter(d time.Duration) time.Duration {
	//nolint:gosec // G404: jitter needs no cryptographic randomness.
	return d/2 + time.Duration(mrand.Int64N(int64(d)/2+1))
}

func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err() //nolint:wrapcheck // callers wrap it with the right sentinel.
	case <-t.C:
		return nil
	}
}
