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
	"syscall"

	"echpochmak/internal/api"
	"echpochmak/internal/config"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
	"echpochmak/internal/store/memstore"
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

	st, err := newStore(cfg.Store)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr: cfg.HTTP.Addr,
		Handler: api.NewHandler(api.Options{
			Logger: log,
			Store:  st,
			Server: api.NewServer(),
		}),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		ReadTimeout:       cfg.HTTP.ReadTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	return serve(ctx, srv, cfg.HTTP, log)
}

// serve runs srv until ctx is cancelled, then shuts it down gracefully.
func serve(ctx context.Context, srv *http.Server, cfg config.HTTP, log *slog.Logger) error {
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

	log.InfoContext(ctx, "shutting down", slog.Duration("timeout", cfg.ShutdownTimeout))

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

func newStore(cfg config.Store) (store.Store, error) {
	switch cfg.Backend {
	case config.StoreMemory:
		return memstore.New(), nil
	default:
		return nil, fmt.Errorf("unsupported store backend %q", cfg.Backend)
	}
}
