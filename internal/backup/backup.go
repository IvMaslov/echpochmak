// Package backup stores snapshots of the KV cluster in object storage and
// manages them: it takes a consistent snapshot from a node, preferably a
// follower, streams it to an object, and writes a manifest next to it once
// the upload is complete. A snapshot without a manifest is incomplete and is
// ignored. Retention keeps the newest backups and one per day, and
// verification restores a backup into memory to prove it can be restored.
//
// Layout under the location, for example s3://backups/kv:
//
//	<cluster-id>/<UTC time>-r<revision>.snap        the snapshot file
//	<cluster-id>/<UTC time>-r<revision>.snap.json   its manifest
//
// See arch/phase5/plan.md (D2).
package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"log/slog"
	"slices"
	"strings"
	"time"

	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/kvpb"
	"echpochmak/internal/logger"
)

// File name suffixes.
const (
	snapshotSuffix = ".snap"
	manifestSuffix = ".snap.json"
	timeLayout     = "20060102T150405.000000000Z"
	manifestFormat = 1
)

// ErrNoBackup means no complete backup matches.
var ErrNoBackup = errors.New("no complete backup")

// errUnchanged stops an export whose revision is already backed up.
var errUnchanged = errors.New("revision already backed up")

// Objects is the object storage that holds backups; [fileio.Iceberg]
// implements it.
type Objects interface {
	Create(ctx context.Context, location string) (io.WriteCloser, error)
	Open(ctx context.Context, location string) (io.ReadCloser, error)
	Write(ctx context.Context, location string, data []byte) error
	Read(ctx context.Context, location string) ([]byte, error)
	List(ctx context.Context, prefix string) ([]fileio.ObjectInfo, error)
	Delete(ctx context.Context, location string) error
}

// Source is the KV cluster backups are taken from; [client.Client]
// implements it.
type Source interface {
	Endpoints() []string
	Status(ctx context.Context) (map[string]*kvpb.StatusResponse, map[string]error)
	ExportSnapshot(
		ctx context.Context,
		endpoint string,
		open func(*kvpb.SnapshotHeader) (io.Writer, error),
	) (*kvpb.SnapshotHeader, error)
}

// Manifest describes a complete backup. It is stored as JSON next to the
// snapshot, after the snapshot is complete.
type Manifest struct {
	Format    int       `json:"format"`
	ClusterID string    `json:"cluster_id"`
	NodeID    uint64    `json:"node_id"`
	Index     uint64    `json:"index"`
	Term      uint64    `json:"term"`
	Revision  int64     `json:"revision"`
	Members   int       `json:"members"`
	Created   time.Time `json:"created"`
	Version   string    `json:"version,omitempty"`
	// Snapshot is the location of the snapshot file.
	Snapshot string `json:"snapshot"`
	// Size and SHA256 describe the whole snapshot file as stored.
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Options configures a [Manager].
type Options struct {
	Source  Source
	Objects Objects
	// Location is the base location of the backups, such as s3://b/kv.
	Location string
	// KeepLast is how many of the newest backups retention keeps.
	KeepLast int
	// KeepDaily is for how many days retention keeps the newest backup of
	// each day.
	KeepDaily int
	// IncompleteAge is how old a snapshot without a manifest must be before
	// retention deletes it; an upload may still be running before that.
	IncompleteAge time.Duration
	Logger        *slog.Logger
	// Now returns the current time; tests replace it.
	Now func() time.Time
}

// Defaults for [Options].
const (
	DefaultKeepLast      = 48
	DefaultKeepDaily     = 14
	DefaultIncompleteAge = time.Hour
)

// Manager takes, lists, fetches, verifies and expires backups.
type Manager struct {
	opts Options
}

// New returns a Manager. Zero options take their defaults.
func New(opts Options) *Manager {
	opts.Location = strings.TrimSuffix(opts.Location, "/")

	if opts.KeepLast <= 0 {
		opts.KeepLast = DefaultKeepLast
	}

	if opts.KeepDaily < 0 {
		opts.KeepDaily = 0
	}

	if opts.IncompleteAge <= 0 {
		opts.IncompleteAge = DefaultIncompleteAge
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Manager{opts: opts}
}

// Result is the outcome of one backup.
type Result struct {
	// Manifest describes the new backup, or the latest one if Skipped.
	Manifest Manifest
	// Skipped is set when the cluster had not changed since the latest
	// backup, which is still current; nothing was uploaded.
	Skipped bool
	// Endpoint is the node the snapshot came from.
	Endpoint string
}

// Backup takes a snapshot, preferably from a follower, and stores it, unless
// the cluster's revision has not moved since the latest backup.
func (m *Manager) Backup(ctx context.Context) (Result, error) {
	endpoint := m.pickEndpoint(ctx)

	latest, err := m.latest(ctx)
	if err != nil && !errors.Is(err, ErrNoBackup) {
		return Result{}, err
	}

	up := &upload{m: m, ctx: ctx, latest: latest}

	header, err := m.opts.Source.ExportSnapshot(ctx, endpoint, up.open)

	switch {
	case errors.Is(err, errUnchanged):
		return Result{Manifest: latest, Skipped: true, Endpoint: endpoint}, nil
	case err != nil:
		up.abort()

		return Result{}, fmt.Errorf("taking the snapshot from %s: %w", endpoint, err)
	}

	manifest, err := up.finish(header)
	if err != nil {
		return Result{}, err
	}

	return Result{Manifest: manifest, Endpoint: endpoint}, nil
}

// pickEndpoint returns a follower's endpoint, to keep the export off the
// leader, else the first endpoint that answers, else the first endpoint.
func (m *Manager) pickEndpoint(ctx context.Context) string {
	statuses, _ := m.opts.Source.Status(ctx)
	endpoints := m.opts.Source.Endpoints()
	fallback := ""

	for _, ep := range endpoints {
		st, ok := statuses[ep]
		if !ok {
			continue
		}

		if st.GetLeaderId() != 0 && st.GetLeaderId() != st.GetHeader().GetMemberId() {
			return ep
		}

		if fallback == "" {
			fallback = ep
		}
	}

	if fallback == "" {
		return endpoints[0]
	}

	return fallback
}

// upload streams one snapshot to its object and records what it wrote.
type upload struct {
	m      *Manager
	ctx    context.Context //nolint:containedctx // one upload, one context.
	latest Manifest

	location string
	w        io.WriteCloser
	hash     hash.Hash
	size     int64
}

// open names the object from the header and opens it, unless the header's
// revision is already backed up.
func (u *upload) open(h *kvpb.SnapshotHeader) (io.Writer, error) {
	if u.latest.ClusterID == h.GetClusterId() && u.latest.Revision == h.GetRevision() {
		return nil, errUnchanged
	}

	created := time.Unix(0, h.GetCreatedUnixNano()).UTC()
	name := fmt.Sprintf("%s-r%d%s", created.Format(timeLayout), h.GetRevision(), snapshotSuffix)
	u.location = u.m.clusterPrefix(h.GetClusterId()) + name

	w, err := u.m.opts.Objects.Create(u.ctx, u.location)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", u.location, err)
	}

	u.w = w
	u.hash = sha256.New()

	return io.MultiWriter(w, u.hash, countWriter{n: &u.size}), nil
}

// abort closes a failed upload. The partial object has no manifest, so it
// is ignored, and retention deletes it later.
func (u *upload) abort() {
	if u.w != nil {
		_ = u.w.Close()
	}
}

// finish completes the object, then writes the manifest that makes the backup
// complete.
func (u *upload) finish(h *kvpb.SnapshotHeader) (Manifest, error) {
	if err := u.w.Close(); err != nil {
		return Manifest{}, fmt.Errorf("completing %s: %w", u.location, err)
	}

	manifest := Manifest{
		Format:    manifestFormat,
		ClusterID: h.GetClusterId(),
		NodeID:    h.GetNodeId(),
		Index:     h.GetIndex(),
		Term:      h.GetTerm(),
		Revision:  h.GetRevision(),
		Members:   len(h.GetMembers()),
		Created:   time.Unix(0, h.GetCreatedUnixNano()).UTC(),
		Version:   h.GetVersion(),
		Snapshot:  u.location,
		Size:      u.size,
		SHA256:    hex.EncodeToString(u.hash.Sum(nil)),
	}

	raw, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return Manifest{}, fmt.Errorf("encoding the manifest: %w", err)
	}

	if err := u.m.opts.Objects.Write(u.ctx, u.location+".json", raw); err != nil {
		return Manifest{}, fmt.Errorf("writing the manifest of %s: %w", u.location, err)
	}

	return manifest, nil
}

// List returns every complete backup, of every cluster, newest first.
func (m *Manager) List(ctx context.Context) ([]Manifest, error) {
	objects, err := m.opts.Objects.List(ctx, m.opts.Location+"/")
	if err != nil {
		return nil, fmt.Errorf("listing backups: %w", err)
	}

	manifests := []Manifest{}

	for _, o := range objects {
		if !strings.HasSuffix(o.Location, manifestSuffix) {
			continue
		}

		raw, err := m.opts.Objects.Read(ctx, o.Location)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", o.Location, err)
		}

		var manifest Manifest
		if err := json.Unmarshal(raw, &manifest); err != nil || manifest.Format != manifestFormat {
			m.opts.Logger.WarnContext(ctx, "backup: ignoring an unreadable manifest",
				slog.String(logger.KeyLocation, o.Location))

			continue
		}

		manifests = append(manifests, manifest)
	}

	slices.SortFunc(manifests, func(a, b Manifest) int { return b.Created.Compare(a.Created) })

	return manifests, nil
}

// latest returns the newest complete backup of the cluster the source
// belongs to. Without a status from the cluster, it is the newest of all.
func (m *Manager) latest(ctx context.Context) (Manifest, error) {
	manifests, err := m.List(ctx)
	if err != nil {
		return Manifest{}, err
	}

	if len(manifests) == 0 {
		return Manifest{}, ErrNoBackup
	}

	return manifests[0], nil
}

// Find returns the newest complete backup created at or before at; a zero at
// means the newest of all.
func (m *Manager) Find(ctx context.Context, at time.Time) (Manifest, error) {
	manifests, err := m.List(ctx)
	if err != nil {
		return Manifest{}, err
	}

	for i := range manifests {
		if at.IsZero() || !manifests[i].Created.After(at) {
			return manifests[i], nil
		}
	}

	return Manifest{}, ErrNoBackup
}

// Fetch copies the snapshot of a backup to w and checks it against its
// manifest.
func (m *Manager) Fetch(ctx context.Context, manifest Manifest, w io.Writer) error {
	r, err := m.opts.Objects.Open(ctx, manifest.Snapshot)
	if err != nil {
		return fmt.Errorf("opening %s: %w", manifest.Snapshot, err)
	}
	defer r.Close()

	h := sha256.New()

	n, err := io.Copy(io.MultiWriter(w, h), r)
	if err != nil {
		return fmt.Errorf("downloading %s: %w", manifest.Snapshot, err)
	}

	if n != manifest.Size || hex.EncodeToString(h.Sum(nil)) != manifest.SHA256 {
		return fmt.Errorf("%s does not match its manifest: %d bytes, want %d", manifest.Snapshot, n, manifest.Size)
	}

	return nil
}

func (m *Manager) clusterPrefix(clusterID string) string {
	return m.opts.Location + "/" + clusterID + "/"
}

type countWriter struct {
	n *int64
}

func (c countWriter) Write(p []byte) (int, error) {
	*c.n += int64(len(p))

	return len(p), nil
}
