// Command catalog-server serves the Iceberg REST catalog API.
//
// It is stateless: all catalog state lives in the store, so any number of
// replicas can run behind a load balancer. Run with -help to list the flags;
// each flag can also be set through an ECHPOCHMAK_* environment variable.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	// Registers the s3:// scheme with iceberg-go's file IO.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/config"
	"echpochmak/internal/fileio"
	"echpochmak/internal/idempotency"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
	"echpochmak/internal/store/raftstore"
	"echpochmak/internal/version"
)

const serviceName = "catalog-server"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Args[1:], os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(os.Stdout, "Usage of %s:\n%s", serviceName, config.Usage())

		return nil
	}

	if err != nil {
		return err
	}

	log := logger.New(os.Stdout, cfg.Log, serviceName)
	slog.SetDefault(log)

	log.InfoContext(context.Background(), "starting",
		slog.Any(logger.KeyBuild, version.Get()),
		slog.Any(logger.KeyConfig, cfg),
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	b, err := newBackend(cfg.Store, log)
	if err != nil {
		return err
	}
	defer b.close()

	// Storage clients must keep working while in-flight requests drain after
	// a shutdown signal cancels ctx.
	storageCtx := context.WithoutCancel(ctx)
	files := fileio.NewCached(fileio.NewIceberg(storageCtx, cfg.S3.IOProps()), cfg.Catalog.MetadataCacheSize)
	svc := catalog.New(catalog.Options{
		Store:         b.store,
		Files:         files,
		Warehouse:     cfg.S3.Warehouse,
		CommitRetries: cfg.Catalog.CommitRetries,
		Logger:        log,
	})

	guard := newGuard(ctx, cfg, b.records, log)

	// The purger reads each file once; it must not evict table metadata from
	// the cache.
	purger := startPurger(ctx, cfg.Catalog, b.store, fileio.NewIceberg(storageCtx, cfg.S3.IOProps()), log)
	defer purger.wait()

	serverOpts := []api.ServerOption{}
	if guard != nil {
		serverOpts = append(serverOpts, api.WithIdempotencyLifetime(guard.Lifetime()))
	}

	var draining atomic.Bool

	srv := &http.Server{
		Addr: cfg.HTTP.Addr,
		Handler: api.NewHandler(api.Options{
			Logger:       log,
			Ready:        svc,
			Draining:     draining.Load,
			Server:       api.NewServer(svc, cfg.Catalog.Name, serverOpts...),
			MaxBodyBytes: cfg.HTTP.MaxBodyBytes,
			Idempotency:  guard,
			Docs:         cfg.HTTP.Docs,
		}),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		// Requests must not be cancelled by the shutdown signal: they are
		// still served while draining, and Shutdown ends them.
		BaseContext: func(net.Listener) context.Context { return storageCtx },
	}

	return serve(ctx, srv, cfg.HTTP, &draining, log)
}

// serve runs srv until ctx is cancelled, then shuts it down gracefully: it
// first reports itself unready for the drain delay, so a load balancer moves
// new requests to other replicas, then finishes the requests in flight.
func serve(ctx context.Context, srv *http.Server, cfg config.HTTP, draining *atomic.Bool, log *slog.Logger) error {
	errCh := make(chan error, 1)

	go func() {
		log.InfoContext(ctx, "http server listening", slog.String(logger.KeyAddr, srv.Addr))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	log.InfoContext(ctx, "shutting down",
		slog.Duration("drain_delay", cfg.DrainDelay),
		slog.Duration("timeout", cfg.ShutdownTimeout),
	)

	draining.Store(true)
	time.Sleep(cfg.DrainDelay)

	// The parent context is already cancelled; shutdown needs its own deadline.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http server shutdown: %w", err)
	}

	if err := <-errCh; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}

	log.InfoContext(ctx, "stopped")

	return nil
}

// backend is the configured storage: the catalog store, the idempotency
// records next to it, and what releases them.
type backend struct {
	store   store.Store
	records idempotency.Store
	close   func()
}

func newBackend(cfg config.Store, log *slog.Logger) (backend, error) {
	switch cfg.Backend {
	case config.StoreMemory:
		return backend{store: memstore.New(), records: idempotency.NewMemoryStore(), close: func() {}}, nil
	case config.StoreRaft:
		c, err := client.New(client.Config{
			Endpoints:      cfg.KVEndpoints,
			AttemptTimeout: cfg.KVAttemptTimeout,
			Logger:         log,
		})
		if err != nil {
			return backend{}, fmt.Errorf("connecting to the KV cluster: %w", err)
		}

		closeClient := func() {
			if err := c.Close(); err != nil {
				log.Warn("closing the KV client failed", logger.Err(err))
			}
		}

		return backend{
			store:   raftstore.New(c, raftstore.Options{CallTimeout: cfg.KVRequestTimeout}),
			records: idempotency.NewKVStore(c, raftstore.DefaultPrefix),
			close:   closeClient,
		}, nil
	default:
		return backend{}, fmt.Errorf("unsupported store backend %q", cfg.Backend)
	}
}

// runningPurger is the purge worker of this replica, if it runs one.
type runningPurger struct {
	done chan struct{}
}

// wait blocks until the purger has stopped, after its context is done.
func (p runningPurger) wait() {
	if p.done != nil {
		<-p.done
	}
}

// startPurger runs the purge worker until ctx is done, unless purge-workers
// is zero.
func startPurger(
	ctx context.Context,
	cfg config.Catalog,
	st store.Store,
	files fileio.FileIO,
	log *slog.Logger,
) runningPurger {
	if cfg.PurgeWorkers == 0 {
		log.InfoContext(ctx, "purge worker disabled; other replicas purge dropped tables")

		return runningPurger{}
	}

	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}

	p := catalog.NewPurger(catalog.PurgerOptions{
		Store:    st,
		Files:    files,
		Owner:    fmt.Sprintf("%s/%d", host, os.Getpid()),
		Workers:  cfg.PurgeWorkers,
		Lease:    cfg.PurgeLease,
		Interval: cfg.PurgeInterval,
		Logger:   log,
	})

	done := make(chan struct{})

	go func() {
		defer close(done)
		p.Run(ctx)
	}()

	return runningPurger{done: done}
}

// janitorInterval is how often expired idempotency records are deleted.
const janitorInterval = time.Minute

// newGuard returns the idempotency guard, or nil when idempotency is
// disabled. Its janitor runs until ctx is done. A claim's lease is twice the
// HTTP write timeout: no request outlives it, so a live claim is never taken
// over.
func newGuard(ctx context.Context, cfg config.Config, records idempotency.Store, log *slog.Logger) *idempotency.Guard {
	if cfg.Catalog.IdempotencyLifetime <= 0 {
		return nil
	}

	g := idempotency.NewGuard(records, idempotency.Options{
		Lifetime: cfg.Catalog.IdempotencyLifetime,
		Lease:    2 * cfg.HTTP.WriteTimeout,
		Logger:   log,
	})

	go g.RunJanitor(ctx, janitorInterval)

	return g
}
