// Command catalogctl inspects the catalog's state for operators.
//
// Usage:
//
//	catalogctl [catalog-server flags] restore-report [flags]
//
// It takes the catalog-server flags and ECHPOCHMAK_* environment variables,
// so it runs as is inside a catalog container: it reads the store and the
// object storage that catalog-server would use.
//
// restore-report compares the catalog's pointers with the metadata files in
// object storage after the KV store was restored from a backup. It only
// reads; with -emit-commands it prints the register calls that roll tables
// forward. See arch/phase5/plan.md (D7) and docs/operations.md.
//
//	-tables db.t,db.u   only these tables; default: every table
//	-deep               check the current snapshot's manifest list and manifests
//	-scan-warehouse     find metadata directories no pointer leads to
//	-emit-commands      print a curl command per finding that can be applied
//	-catalog-url URL    base URL for the commands; default http://localhost:8181
//	-json               print one JSON object per finding
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	// Registers the s3:// scheme with iceberg-go's file IO.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"

	"echpochmak/internal/catalog"
	"echpochmak/internal/config"
	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/store"
	"echpochmak/internal/store/raftstore"
)

const toolName = "catalogctl"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdout)

	stop()

	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", toolName, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, out io.Writer) error {
	cfg, rest, err := config.LoadWithArgs(args, getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintf(out, "Usage: %s [catalog-server flags] restore-report [flags]\n%s", toolName, config.Usage())

		return nil
	}

	if err != nil {
		return err
	}

	if len(rest) == 0 || rest[0] != "restore-report" {
		return errors.New("the command is restore-report")
	}

	st, closeStore, err := openStore(cfg.Store)
	if err != nil {
		return err
	}
	defer closeStore()

	files := fileio.NewIceberg(context.WithoutCancel(ctx), cfg.S3.IOProps())

	svc := catalog.New(catalog.Options{
		Store:     st,
		Files:     files,
		Warehouse: cfg.S3.Warehouse,
		Logger:    slog.New(slog.DiscardHandler),
	})

	return restoreReport(ctx, svc, files, cfg.Catalog.Name, rest[1:], out)
}

// openStore connects to the catalog's store. Only the raft backend holds
// state that outlives a process, so it is the only one.
func openStore(cfg config.Store) (store.Store, func(), error) {
	if cfg.Backend != config.StoreRaft {
		return nil, nil, fmt.Errorf("catalogctl needs the raft backend, not %q", cfg.Backend)
	}

	c, err := client.New(client.Config{
		Endpoints:      cfg.KVEndpoints,
		AttemptTimeout: cfg.KVAttemptTimeout,
		Logger:         slog.New(slog.DiscardHandler),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to the KV cluster: %w", err)
	}

	st := raftstore.New(c, raftstore.Options{CallTimeout: cfg.KVRequestTimeout})

	return st, func() { _ = c.Close() }, nil
}

func restoreReport(
	ctx context.Context,
	svc *catalog.Service,
	lister catalog.Lister,
	prefix string,
	args []string,
	out io.Writer,
) error {
	fs := flag.NewFlagSet("restore-report", flag.ContinueOnError)
	tables := fs.String("tables", "", "only these tables, comma-separated, such as db.t")
	deep := fs.Bool("deep", false, "check the current snapshot's manifest list and manifests")
	scan := fs.Bool("scan-warehouse", false, "find metadata directories that no pointer leads to")
	emit := fs.Bool("emit-commands", false, "print a curl command per finding that can be applied")
	catalogURL := fs.String("catalog-url", "http://localhost:8181", "base URL of the catalog for the commands")
	asJSON := fs.Bool("json", false, "print one JSON object per finding")

	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("restore-report: %w", err)
	}

	ids, err := parseTables(*tables)
	if err != nil {
		return err
	}

	findings, err := svc.RestoreReport(ctx, catalog.ReportOptions{
		Lister:        lister,
		Tables:        ids,
		Deep:          *deep,
		ScanWarehouse: *scan,
	})
	if err != nil {
		return err
	}

	switch {
	case *emit:
		return printCommands(out, findings, *catalogURL, prefix)
	case *asJSON:
		return printJSON(out, findings)
	default:
		return printTable(out, findings)
	}
}

// parseTables parses "db.t,a.b.c" into identifiers: the part after the last
// dot is the name.
func parseTables(s string) ([]store.Ident, error) {
	ids := []store.Ident{}

	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		levels := strings.Split(part, ".")
		if len(levels) < 2 {
			return nil, fmt.Errorf("table %q needs a namespace: ns.name", part)
		}

		ids = append(ids, store.Ident{Namespace: levels[:len(levels)-1], Name: levels[len(levels)-1]})
	}

	return ids, nil
}

// printCommands prints a register command for every finding that a register
// can repair: ahead tables, and unregistered directories.
func printCommands(out io.Writer, findings []catalog.Finding, catalogURL, prefix string) error {
	for i := range findings {
		f := &findings[i]

		var line string

		switch f.Kind {
		case catalog.FindingAhead:
			line = fmt.Sprintf("# %s: %d commits after the backup\n%s",
				f.Table, f.Commits, catalog.RegisterCommand(*f, catalogURL, prefix))
		case catalog.FindingUnregistered:
			line = fmt.Sprintf("# %s: %s; fill in the namespace and name\n%s",
				f.Newest, f.Detail, catalog.RegisterCommand(*f, catalogURL, prefix))
		default:
			line = fmt.Sprintf("# %s: %s needs a person: %s", f.Table, f.Kind, describe(f))
		}

		if _, err := fmt.Fprintln(out, line); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}
	}

	return nil
}

func printJSON(out io.Writer, findings []catalog.Finding) error {
	enc := json.NewEncoder(out)

	for i := range findings {
		if err := enc.Encode(&findings[i]); err != nil {
			return fmt.Errorf("writing output: %w", err)
		}
	}

	return nil
}

func printTable(out io.Writer, findings []catalog.Finding) error {
	w := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "FINDING\tTABLE\tDETAIL")

	for i := range findings {
		f := &findings[i]
		fmt.Fprintf(w, "%s\t%s\t%s\n", f.Kind, f.Table, describe(f))
	}

	if len(findings) == 0 {
		fmt.Fprintln(w, "none\t\tevery pointer is current and its files exist")
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	return nil
}

func describe(f *catalog.Finding) string {
	switch f.Kind {
	case catalog.FindingAhead:
		return fmt.Sprintf("%d commits ahead; newest %s", f.Commits, f.Newest)
	case catalog.FindingAmbiguous:
		return fmt.Sprintf("after %s, candidates %s", f.Current, strings.Join(f.Candidates, ", "))
	case catalog.FindingMissingMetadata:
		return "missing " + f.Current
	case catalog.FindingMissingFiles:
		return "missing " + f.Missing
	default:
		return f.Newest + ": " + f.Detail
	}
}
