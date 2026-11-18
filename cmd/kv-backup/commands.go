package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"echpochmak/internal/backup"
	"echpochmak/internal/kv/snapshot"
)

// keySnapshot names a snapshot's location in command output.
const keySnapshot = "snapshot"

// now takes one backup and applies retention.
func (c *command) now(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	res, err := c.m.Backup(ctx)
	if err != nil {
		return err
	}

	if !res.Skipped {
		if _, err := c.m.Retain(ctx, res.Manifest.ClusterID); err != nil {
			return err
		}
	}

	return c.printJSON(map[string]any{
		keySnapshot: res.Manifest.Snapshot,
		"revision":  res.Manifest.Revision,
		"skipped":   res.Skipped,
		"endpoint":  res.Endpoint,
	})
}

func (c *command) list(ctx context.Context) error {
	manifests, err := c.m.List(ctx)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(c.out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CREATED\tCLUSTER\tREVISION\tINDEX\tSIZE\tSNAPSHOT")

	for i := range manifests {
		m := &manifests[i]
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%d\t%s\n",
			m.Created.Format(time.RFC3339), m.ClusterID, m.Revision, m.Index, m.Size, m.Snapshot)
	}

	if err := w.Flush(); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	return nil
}

// fetch downloads a backup to a file, which appears only once it is complete
// and checked.
func (c *command) fetch(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ContinueOnError)
	at := fs.String("at", "", "the newest backup created at or before this time, RFC 3339; default: the newest")

	if err := fs.Parse(args); err != nil || fs.NArg() != 1 {
		return errors.New("usage: fetch [-at TIME] FILE")
	}

	var when time.Time

	if *at != "" {
		var err error

		when, err = time.Parse(time.RFC3339, *at)
		if err != nil {
			return fmt.Errorf("-at: %w", err)
		}
	}

	manifest, err := c.m.Find(ctx, when)
	if err != nil {
		return err
	}

	path := fs.Arg(0)
	tmp := path + ".partial"

	if err := c.download(ctx, manifest, tmp); err != nil {
		_ = os.Remove(tmp)

		return err
	}

	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("renaming to %s: %w", path, err)
	}

	return c.printJSON(map[string]any{
		"file":       path,
		keySnapshot:  manifest.Snapshot,
		"created":    manifest.Created.Format(time.RFC3339),
		"cluster_id": manifest.ClusterID,
		"revision":   manifest.Revision,
		"index":      manifest.Index,
	})
}

func (c *command) download(ctx context.Context, manifest backup.Manifest, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating %s: %w", path, err)
	}
	defer f.Close()

	if err := c.m.Fetch(ctx, manifest, f); err != nil {
		return err
	}

	if _, err := f.Seek(0, 0); err != nil {
		return fmt.Errorf("rewinding %s: %w", path, err)
	}

	if _, err := snapshot.Verify(f); err != nil {
		return fmt.Errorf("%s: %w", manifest.Snapshot, err)
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing %s: %w", path, err)
	}

	return nil
}

func (c *command) verify(ctx context.Context, args []string) error {
	if len(args) > 1 {
		return errors.New("usage: verify [SNAPSHOT]")
	}

	manifest, err := c.pick(ctx, args)
	if err != nil {
		return err
	}

	if err := c.m.Verify(ctx, manifest); err != nil {
		return err
	}

	return c.printJSON(map[string]any{keySnapshot: manifest.Snapshot, "verified": true})
}

// pick returns the backup whose snapshot is at args[0], or the newest.
func (c *command) pick(ctx context.Context, args []string) (backup.Manifest, error) {
	if len(args) == 0 {
		return c.m.Find(ctx, time.Time{})
	}

	manifests, err := c.m.List(ctx)
	if err != nil {
		return backup.Manifest{}, err
	}

	for i := range manifests {
		if manifests[i].Snapshot == args[0] {
			return manifests[i], nil
		}
	}

	return backup.Manifest{}, fmt.Errorf("%w at %s", backup.ErrNoBackup, args[0])
}

func (c *command) printJSON(v any) error {
	enc := json.NewEncoder(c.out)
	enc.SetIndent("", "  ")

	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	return nil
}
