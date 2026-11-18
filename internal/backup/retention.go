package backup

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"echpochmak/internal/logger"
)

// RetentionResult reports what retention deleted.
type RetentionResult struct {
	// Deleted is the number of complete backups deleted.
	Deleted int
	// Incomplete is the number of snapshots without a manifest deleted.
	Incomplete int
	// Retained is the number of complete backups of the cluster kept.
	Retained int
}

// Retain applies the retention policy to the backups of one cluster: it keeps
// the newest KeepLast backups and the newest backup of each of the last
// KeepDaily days, always keeps the newest backup, and deletes the rest. It
// also deletes snapshots that never got a manifest, once they are older than
// IncompleteAge. Backups of other clusters are left alone.
//
// A backup is deleted manifest first, so an interruption leaves an
// incomplete snapshot, never a manifest without its snapshot.
func (m *Manager) Retain(ctx context.Context, clusterID string) (RetentionResult, error) {
	var res RetentionResult

	all, err := m.List(ctx)
	if err != nil {
		return res, err
	}

	manifests := make([]Manifest, 0, len(all))
	for i := range all {
		if all[i].ClusterID == clusterID {
			manifests = append(manifests, all[i])
		}
	}

	keep := m.toKeep(manifests)

	for i := range manifests {
		if keep[manifests[i].Snapshot] {
			res.Retained++

			continue
		}

		if err := m.deleteBackup(ctx, &manifests[i]); err != nil {
			return res, err
		}

		res.Deleted++
	}

	incomplete, err := m.deleteIncomplete(ctx, clusterID, manifests)
	res.Incomplete = incomplete

	return res, err
}

// toKeep returns the snapshots that the policy keeps, from manifests sorted
// newest first.
func (m *Manager) toKeep(manifests []Manifest) map[string]bool {
	keep := map[string]bool{}

	for i := range min(m.opts.KeepLast, len(manifests)) {
		keep[manifests[i].Snapshot] = true
	}

	today := m.opts.Now().UTC().Truncate(24 * time.Hour)
	oldest := today.AddDate(0, 0, -m.opts.KeepDaily+1)
	seenDay := map[time.Time]bool{}

	for i := range manifests {
		day := manifests[i].Created.UTC().Truncate(24 * time.Hour)
		if m.opts.KeepDaily == 0 || day.Before(oldest) || seenDay[day] {
			continue
		}

		seenDay[day] = true
		keep[manifests[i].Snapshot] = true
	}

	return keep
}

func (m *Manager) deleteBackup(ctx context.Context, manifest *Manifest) error {
	for _, location := range []string{manifest.Snapshot + ".json", manifest.Snapshot} {
		if err := m.opts.Objects.Delete(ctx, location); err != nil {
			return fmt.Errorf("deleting %s: %w", location, err)
		}
	}

	m.opts.Logger.InfoContext(ctx, "backup: expired",
		slog.String(logger.KeyLocation, manifest.Snapshot),
		slog.Time("created", manifest.Created),
	)

	return nil
}

// deleteIncomplete deletes the snapshots of a cluster that have no manifest
// and are older than IncompleteAge, judged by the time in their name.
func (m *Manager) deleteIncomplete(ctx context.Context, clusterID string, manifests []Manifest) (int, error) {
	objects, err := m.opts.Objects.List(ctx, m.clusterPrefix(clusterID))
	if err != nil {
		return 0, fmt.Errorf("listing backups: %w", err)
	}

	complete := map[string]bool{}
	for i := range manifests {
		complete[manifests[i].Snapshot] = true
	}

	deleted := 0
	cutoff := m.opts.Now().Add(-m.opts.IncompleteAge)

	for _, o := range objects {
		if !strings.HasSuffix(o.Location, snapshotSuffix) || complete[o.Location] {
			continue
		}

		created, ok := createdFromName(o.Location)
		if !ok || created.After(cutoff) {
			continue
		}

		if err := m.opts.Objects.Delete(ctx, o.Location); err != nil {
			return deleted, fmt.Errorf("deleting %s: %w", o.Location, err)
		}

		deleted++
	}

	return deleted, nil
}

// createdFromName parses the creation time from a snapshot's name.
func createdFromName(location string) (time.Time, bool) {
	name := location[strings.LastIndex(location, "/")+1:]

	stamp, _, ok := strings.Cut(name, "-r")
	if !ok {
		return time.Time{}, false
	}

	t, err := time.Parse(timeLayout, stamp)

	return t, err == nil
}
