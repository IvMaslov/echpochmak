package catalog

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/fileio"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

// Defaults for [PurgerOptions].
const (
	DefaultPurgeWorkers  = 2
	DefaultPurgeLease    = 2 * time.Minute
	DefaultPurgeInterval = 10 * time.Second

	// purgeBatch is how many files are deleted between lease renewals.
	purgeBatch = 100
	// purgeListLimit is how many queued tasks one scan looks at.
	purgeListLimit = 32
)

// PurgerOptions configures a [Purger].
type PurgerOptions struct {
	Store store.Store
	// Files reads metadata and manifests and deletes files. Pass an uncached
	// FileIO: a purge reads many files once each.
	Files fileio.FileIO
	// Owner names this worker in claims, for logs.
	Owner string
	// Workers is how many tasks run at once.
	Workers int
	// Lease is how long a claim lasts. It is renewed between delete batches,
	// so it only needs to outlast one batch.
	Lease time.Duration
	// Interval is the pause between scans of the queue, jittered.
	Interval time.Duration
	Logger   *slog.Logger
	// Now returns the current time; tests replace it.
	Now func() time.Time
}

// Purger deletes the files of tables dropped with purge. Every replica runs
// one: workers claim queued tasks with a lease, so a task whose worker died
// is taken over once its lease expires. Deleting a file that is already gone
// succeeds, so running a task twice is harmless.
type Purger struct {
	opts PurgerOptions
	sem  chan struct{}
	wg   sync.WaitGroup

	mu      sync.Mutex
	running map[string]bool
}

// NewPurger returns a Purger. Zero options take their defaults.
func NewPurger(opts PurgerOptions) *Purger {
	if opts.Workers <= 0 {
		opts.Workers = DefaultPurgeWorkers
	}

	if opts.Lease <= 0 {
		opts.Lease = DefaultPurgeLease
	}

	if opts.Interval <= 0 {
		opts.Interval = DefaultPurgeInterval
	}

	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.Now == nil {
		opts.Now = time.Now
	}

	return &Purger{
		opts:    opts,
		sem:     make(chan struct{}, opts.Workers),
		running: map[string]bool{},
	}
}

// Run scans the queue until ctx is done, then waits for running tasks to
// stop. A task stopped by ctx keeps its claim, which expires.
func (p *Purger) Run(ctx context.Context) {
	defer p.wg.Wait()

	for {
		p.Scan(ctx)

		t := time.NewTimer(jitter(p.opts.Interval))

		select {
		case <-ctx.Done():
			t.Stop()

			return
		case <-t.C:
		}
	}
}

// Scan starts every claimable task it can, up to the worker limit, and
// returns without waiting for them. Use Wait to wait.
func (p *Purger) Scan(ctx context.Context) {
	tasks, err := p.opts.Store.ListPurgeTasks(ctx, purgeListLimit)
	if err != nil {
		if ctx.Err() == nil {
			p.opts.Logger.WarnContext(ctx, "listing purge tasks failed", logger.Err(err))
		}

		return
	}

	for i := range tasks {
		task := tasks[i]
		if !task.Claimable(p.opts.Now()) || p.isRunning(task.ID) {
			continue
		}

		select {
		case p.sem <- struct{}{}:
		default:
			return // every worker is busy
		}

		claimed, err := p.claim(ctx, task)
		if err != nil {
			<-p.sem

			if !errors.Is(err, store.ErrPurgeTaskChanged) {
				p.opts.Logger.WarnContext(ctx, "claiming a purge task failed", logger.Err(err))
			}

			continue
		}

		p.setRunning(claimed.ID, true)
		p.wg.Go(func() {
			defer func() {
				p.setRunning(claimed.ID, false)
				<-p.sem
			}()

			p.purge(ctx, claimed)
		})
	}
}

// Wait waits for the tasks started by Scan.
func (p *Purger) Wait() {
	p.wg.Wait()
}

func (p *Purger) claim(ctx context.Context, task store.PurgeTask) (store.PurgeTask, error) {
	return p.opts.Store.ClaimPurgeTask(ctx, task, store.PurgeClaim{
		Owner: p.opts.Owner,
		Until: p.opts.Now().Add(p.opts.Lease),
	})
}

func (p *Purger) isRunning(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.running[id]
}

func (p *Purger) setRunning(id string, running bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if running {
		p.running[id] = true
	} else {
		delete(p.running, id)
	}
}

// purge runs one claimed task. On failure the task keeps its claim, which
// expires, and a later scan retries it.
func (p *Purger) purge(ctx context.Context, task store.PurgeTask) {
	log := p.opts.Logger.With(
		slog.String(logger.KeyTable, task.Ident.String()),
		slog.String("task", task.ID),
	)
	start := time.Now()

	files, kept, err := p.collect(ctx, task.MetadataLocation)
	if err != nil {
		log.WarnContext(ctx, "purge: collecting files failed; retrying later", logger.Err(err))

		return
	}

	for _, location := range kept {
		log.WarnContext(ctx, "purge: keeping a file outside the table location",
			slog.String(logger.KeyLocation, location))
	}

	task, err = p.deleteAll(ctx, task, files)
	if err != nil {
		log.WarnContext(ctx, "purge: deleting files failed; retrying later", logger.Err(err))

		return
	}

	if err := p.opts.Store.CompletePurgeTask(ctx, task); err != nil {
		log.WarnContext(ctx, "purge: completing the task failed", logger.Err(err))

		return
	}

	log.InfoContext(ctx, "table purged",
		slog.Int("files_deleted", len(files)),
		slog.Int("files_kept", len(kept)),
		slog.Duration(logger.KeyDuration, time.Since(start)),
	)
}

// deleteAll deletes files in order, in batches, renewing the claim before each
// batch. It returns the task at its latest version.
func (p *Purger) deleteAll(ctx context.Context, task store.PurgeTask, files []string) (store.PurgeTask, error) {
	for batch := range slices.Chunk(files, purgeBatch) {
		renewed, err := p.claim(ctx, task)
		if err != nil {
			return store.PurgeTask{}, fmt.Errorf("renewing the claim: %w", err)
		}

		task = renewed

		for _, location := range batch {
			if err := p.opts.Files.Delete(ctx, location); err != nil {
				return store.PurgeTask{}, fmt.Errorf("deleting %q: %w", location, err)
			}
		}
	}

	return task, nil
}

// collect returns the files to delete for the table whose last metadata file
// is at metadataLocation, in deletion order, and the referenced files that are
// kept because they lie outside the table location.
//
// The order lets a worker that takes over a half-done task collect the files
// again: data and delete files go first, then manifests and manifest lists,
// statistics, old metadata files and, last, the metadata file the task names.
func (p *Purger) collect(ctx context.Context, metadataLocation string) (files, kept []string, err error) {
	raw, err := p.opts.Files.Read(ctx, metadataLocation)
	if errors.Is(err, fileio.ErrNotFound) {
		// A previous run deleted everything, the metadata file last.
		return []string{}, []string{}, nil
	}

	if err != nil {
		return nil, nil, fmt.Errorf("reading %q: %w", metadataLocation, err)
	}

	raw, err = decompress(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("decompressing %q: %w", metadataLocation, err)
	}

	md, err := table.ParseMetadataBytes(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing %q: %w", metadataLocation, err)
	}

	referenced, err := referencedFiles(ctx, p.opts.Files, md)
	if err != nil {
		return nil, nil, err
	}

	referenced = append(referenced, metadataLocation)

	root := strings.TrimSuffix(md.Location(), "/") + "/"
	seen := map[string]bool{}
	files = make([]string, 0, len(referenced))
	kept = []string{}

	for _, location := range referenced {
		switch {
		case seen[location]:
		case strings.HasPrefix(location, root):
			files = append(files, location)
		default:
			kept = append(kept, location)
		}

		seen[location] = true
	}

	return files, kept, nil
}

// referencedFiles lists every file md references, in deletion order, except
// its own metadata file.
func referencedFiles(ctx context.Context, files fileio.FileIO, md table.Metadata) ([]string, error) {
	fsys := fileio.AsIcebergIO(ctx, files)

	var data, manifests, lists []string

	seenManifests := map[string]bool{}

	for _, snap := range md.Snapshots() {
		if snap.ManifestList == "" {
			continue
		}

		lists = append(lists, snap.ManifestList)

		mfs, err := snap.Manifests(fsys)
		if err != nil {
			return nil, fmt.Errorf("reading manifest list %q: %w", snap.ManifestList, err)
		}

		for _, m := range mfs {
			if seenManifests[m.FilePath()] {
				continue
			}

			seenManifests[m.FilePath()] = true
			manifests = append(manifests, m.FilePath())

			for entry, err := range m.Entries(fsys, false) {
				if err != nil {
					return nil, fmt.Errorf("reading manifest %q: %w", m.FilePath(), err)
				}

				data = append(data, entry.DataFile().FilePath())
			}
		}
	}

	var stats, metadata []string

	for sf := range md.Statistics() {
		stats = append(stats, sf.StatisticsPath)
	}

	for sf := range md.PartitionStatistics() {
		stats = append(stats, sf.StatisticsPath)
	}

	for entry := range md.PreviousFiles() {
		metadata = append(metadata, entry.MetadataFile)
	}

	return slices.Concat(data, manifests, lists, stats, metadata), nil
}

// jitter returns d scaled by a random factor in [0.5, 1.5).
func jitter(d time.Duration) time.Duration {
	return d/2 + time.Duration(rand.Int64N(int64(d))) //nolint:gosec // G404: jitter needs no cryptographic randomness.
}
