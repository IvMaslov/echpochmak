// Command kv-backup backs up the KV cluster to object storage.
//
// Usage:
//
//	kv-backup [flags] COMMAND [ARGS]
//
// Commands:
//
//	run                     back up every -interval, apply retention, test-restore
//	                        the newest backup every -verify-interval; serves
//	                        /metrics and /healthz on -listen-metrics
//	now                     take one backup and apply retention
//	list                    list complete backups, newest first
//	fetch [-at TIME] FILE   download the newest backup, or the newest at or
//	                        before TIME (RFC 3339), to FILE and check it
//	verify [SNAPSHOT]       test-restore a backup: the newest, or the one at
//	                        the location SNAPSHOT
//	probe                   check the /healthz of a running "run" on
//	                        -listen-metrics; for container health checks
//
// Run with -help to list the flags; each flag can also be set through an
// ECHPOCHMAK_BACKUP_* environment variable. See arch/phase5/plan.md (D2).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	// Registers the s3:// scheme with iceberg-go's IO layer.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"

	"echpochmak/internal/backup"
	"echpochmak/internal/config"
	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/logger"
	"echpochmak/internal/version"
)

const serviceName = "kv-backup"

var errUsage = errors.New("usage")

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if !errors.Is(err, errUsage) {
			fmt.Fprintf(os.Stderr, "%s: %v\n", serviceName, err)
		}

		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	cfg, rest, err := config.LoadBackup(args, os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(out, "Usage: %s [flags] COMMAND [ARGS]\n%s", serviceName, config.BackupUsage())

		return nil
	}

	if err != nil {
		return err
	}

	if len(rest) == 0 {
		return errors.New("a command is required: run, now, list, fetch or verify")
	}

	log := logger.New(os.Stderr, cfg.Log, serviceName)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	kv, err := client.New(client.Config{Endpoints: cfg.Endpoints, Logger: log})
	if err != nil {
		return err
	}
	defer kv.Close()

	m := backup.New(backup.Options{
		Source:    kv,
		Objects:   fileio.NewIceberg(context.WithoutCancel(ctx), cfg.S3.IOProps()),
		Location:  cfg.Location,
		KeepLast:  cfg.KeepLast,
		KeepDaily: cfg.KeepDaily,
		Logger:    log,
	})

	cmd := &command{cfg: cfg, m: m, log: log, out: out}

	return cmd.dispatch(ctx, rest)
}

type command struct {
	cfg config.Backup
	m   *backup.Manager
	log *slog.Logger
	out io.Writer
}

func (c *command) dispatch(ctx context.Context, args []string) error {
	name, rest := args[0], args[1:]

	switch name {
	case "run":
		c.log.InfoContext(ctx, "starting",
			slog.Any(logger.KeyBuild, version.Get()),
			slog.Any(logger.KeyConfig, c.cfg),
		)

		return c.runLoop(ctx)
	case "now":
		return c.now(ctx)
	case "list":
		return c.list(ctx)
	case "fetch":
		return c.fetch(ctx, rest)
	case "verify":
		return c.verify(ctx, rest)
	case "probe":
		return c.probe(ctx)
	default:
		return fmt.Errorf("unknown command %q", name)
	}
}
