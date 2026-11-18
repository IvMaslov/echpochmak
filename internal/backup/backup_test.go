package backup_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"echpochmak/internal/backup"
	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/client"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/kv/kvtest"
)

// location returns a fresh in-memory backup location; mem:// buckets are
// shared by the whole process.
func location(t *testing.T) string {
	t.Helper()

	return "mem://" + strings.ToLower(strings.NewReplacer("/", "-", "_", "-").Replace(t.Name())) + "/kv"
}

func newManager(t *testing.T, src backup.Source, opts backup.Options) (*backup.Manager, *fileio.Iceberg) {
	t.Helper()

	objects := fileio.NewIceberg(context.WithoutCancel(t.Context()), nil)
	opts.Source = src
	opts.Objects = objects
	opts.Location = location(t)
	opts.Logger = slog.New(slog.DiscardHandler)

	return backup.New(opts), objects
}

func put(t *testing.T, cl *client.Client, key string) {
	t.Helper()

	if _, err := cl.Put(t.Context(), []byte(key), []byte("v")); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

func TestBackupSkipsAnUnchangedCluster(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()
	put(t, cl, "a")

	m, objects := newManager(t, cl, backup.Options{})

	first, err := m.Backup(t.Context())
	if err != nil || first.Skipped {
		t.Fatalf("first backup = %+v, %v", first, err)
	}

	if c.Endpoint(c.Leader()) == first.Endpoint {
		t.Errorf("the backup was taken from the leader %s, want a follower", first.Endpoint)
	}

	again, err := m.Backup(t.Context())
	if err != nil || !again.Skipped || again.Manifest.Snapshot != first.Manifest.Snapshot {
		t.Fatalf("backup of an unchanged cluster = %+v, %v; want skipped", again, err)
	}

	put(t, cl, "b")

	third, err := m.Backup(t.Context())
	if err != nil || third.Skipped || third.Manifest.Revision <= first.Manifest.Revision {
		t.Fatalf("backup after a write = %+v, %v", third, err)
	}

	listed, err := m.List(t.Context())
	if err != nil || len(listed) != 2 || listed[0].Snapshot != third.Manifest.Snapshot {
		t.Fatalf("List = %+v, %v; want the two backups, newest first", listed, err)
	}

	for _, manifest := range listed {
		if err := m.Verify(t.Context(), manifest); err != nil {
			t.Fatalf("Verify(%s): %v", manifest.Snapshot, err)
		}
	}

	// A corrupted object fails verification.
	if err := objects.Write(t.Context(), first.Manifest.Snapshot, []byte("garbage")); err != nil {
		t.Fatal(err)
	}

	if err := m.Verify(t.Context(), first.Manifest); err == nil {
		t.Fatal("a corrupted backup passed verification")
	}
}

// failingSource cuts the snapshot stream after the header.
type failingSource struct {
	*client.Client
}

func (f failingSource) ExportSnapshot(
	ctx context.Context,
	endpoint string,
	open func(*kvpb.SnapshotHeader) (io.Writer, error),
) (*kvpb.SnapshotHeader, error) {
	return f.Client.ExportSnapshot(ctx, endpoint, func(h *kvpb.SnapshotHeader) (io.Writer, error) {
		w, err := open(h)
		if err != nil {
			return nil, err
		}

		return &cutWriter{w: w, left: 20}, nil
	})
}

type cutWriter struct {
	w    io.Writer
	left int
}

func (c *cutWriter) Write(p []byte) (int, error) {
	if len(p) > c.left {
		n, _ := c.w.Write(p[:c.left])
		c.left = 0

		return n, errors.New("connection reset")
	}

	c.left -= len(p)

	return c.w.Write(p)
}

func TestInterruptedUploadIsIgnored(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 1)
	cl := c.Client()
	put(t, cl, "a")

	m, objects := newManager(t, failingSource{cl}, backup.Options{})

	if _, err := m.Backup(t.Context()); err == nil {
		t.Fatal("a cut stream reported success")
	}

	listed, err := m.List(t.Context())
	if err != nil || len(listed) != 0 {
		t.Fatalf("List = %+v, %v; want no complete backup", listed, err)
	}

	files, err := objects.List(t.Context(), location(t)+"/")
	if err != nil || len(files) != 1 || !strings.HasSuffix(files[0].Location, ".snap") {
		t.Fatalf("objects = %+v, %v; want one partial snapshot", files, err)
	}
}

// history writes complete backups of cluster c, created at the given times,
// and returns their snapshot locations.
func history(t *testing.T, objects *fileio.Iceberg, base, cluster string, times []time.Time) []string {
	t.Helper()

	locations := make([]string, 0, len(times))

	for i, created := range times {
		snap := fmt.Sprintf("%s/%s/%s-r%d.snap", base, cluster, created.UTC().Format("20060102T150405.000000000Z"), i+1)
		raw, err := json.Marshal(backup.Manifest{
			Format:    1,
			ClusterID: cluster,
			Revision:  int64(i + 1),
			Created:   created,
			Snapshot:  snap,
		})
		if err != nil {
			t.Fatal(err)
		}

		if err := objects.Write(t.Context(), snap, []byte("x")); err != nil {
			t.Fatal(err)
		}

		if err := objects.Write(t.Context(), snap+".json", raw); err != nil {
			t.Fatal(err)
		}

		locations = append(locations, snap)
	}

	return locations
}

func TestRetention(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	m, objects := newManager(t, nil, backup.Options{
		KeepLast:  3,
		KeepDaily: 2,
		Now:       func() time.Time { return now },
	})
	base := location(t)

	// Hourly backups over three days, oldest first.
	times := []time.Time{}
	for h := 60; h >= 0; h-- {
		times = append(times, now.Add(-time.Duration(h)*time.Hour))
	}

	snaps := history(t, objects, base, "c1", times)
	other := history(t, objects, base, "c2", times[:2])

	// A partial upload from long ago, and one still running.
	old := base + "/c1/" + now.Add(-3*time.Hour).Format("20060102T150405.000000000Z") + "-r999.snap"
	young := base + "/c1/" + now.Add(-time.Minute).Format("20060102T150405.000000000Z") + "-r1000.snap"

	for _, loc := range []string{old, young} {
		if err := objects.Write(t.Context(), loc, []byte("partial")); err != nil {
			t.Fatal(err)
		}
	}

	res, err := m.Retain(t.Context(), "c1")
	if err != nil {
		t.Fatalf("Retain: %v", err)
	}

	// Kept: the 3 newest (today 12:00, 11:00, 10:00) and the newest of
	// yesterday (23:00); today's newest is among the 3.
	want := []string{snaps[60], snaps[59], snaps[58], snaps[47]}

	listed, err := m.List(t.Context())
	if err != nil {
		t.Fatal(err)
	}

	got := []string{}

	for _, manifest := range listed {
		if manifest.ClusterID == "c1" {
			got = append(got, manifest.Snapshot)
		}
	}

	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("kept %v,\nwant %v", got, want)
	}

	if res.Retained != 4 || res.Deleted != 57 || res.Incomplete != 1 {
		t.Fatalf("result = %+v", res)
	}

	files, _ := objects.List(t.Context(), base+"/c1/")
	if !containsLocation(files, young) || containsLocation(files, old) {
		t.Fatal("retention must delete the old partial upload and keep the running one")
	}

	if files, _ := objects.List(t.Context(), base+"/c2/"); len(files) != 2*len(other) {
		t.Fatalf("another cluster's backups were touched: %v", files)
	}
}

func TestRetentionKeepsTheNewest(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	m, objects := newManager(t, nil, backup.Options{KeepLast: 1, Now: func() time.Time { return now }})

	// The only backup is a month old: outside every daily window.
	history(t, objects, location(t), "c1", []time.Time{now.AddDate(0, -1, 0)})

	if res, err := m.Retain(t.Context(), "c1"); err != nil || res.Retained != 1 || res.Deleted != 0 {
		t.Fatalf("Retain = %+v, %v; want the only backup kept", res, err)
	}
}

func TestFind(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)
	m, objects := newManager(t, nil, backup.Options{})
	snaps := history(t, objects, location(t), "c1", []time.Time{now.Add(-2 * time.Hour), now.Add(-time.Hour), now})

	tests := []struct {
		at   time.Time
		want string
	}{
		{at: time.Time{}, want: snaps[2]},
		{at: now.Add(-90 * time.Minute), want: snaps[0]},
		{at: now.Add(-time.Hour), want: snaps[1]},
	}

	for _, tt := range tests {
		got, err := m.Find(t.Context(), tt.at)
		if err != nil || got.Snapshot != tt.want {
			t.Errorf("Find(%v) = %s, %v; want %s", tt.at, got.Snapshot, err, tt.want)
		}
	}

	if _, err := m.Find(t.Context(), now.Add(-3*time.Hour)); !errors.Is(err, backup.ErrNoBackup) {
		t.Fatalf("Find before every backup = %v, want ErrNoBackup", err)
	}
}

func containsLocation(objects []fileio.ObjectInfo, location string) bool {
	for _, o := range objects {
		if o.Location == location {
			return true
		}
	}

	return false
}
