// Package memstore implements [store.Store] in memory. It is meant for tests
// and local development: it favours being obviously correct over being fast,
// and its data is lost when the process exits.
package memstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"echpochmak/internal/store"
)

// Store is an in-memory [store.Store]. A single lock guards all data, so every
// operation is trivially linearizable. The zero value is not usable; use [New].
type Store struct {
	mu sync.RWMutex
	// rev is the last version issued. Every write bumps it, so versions are
	// unique across all entries, like etcd revisions.
	rev        store.Version
	namespaces map[string]store.NamespaceRecord
	tables     map[string]store.Pointer
	views      map[string]store.Pointer
	purges     map[string]store.PurgeTask
	intents    map[string]store.Intent
	// now returns the time recorded as a purge task's or an intent's
	// creation time.
	now func() time.Time
}

var _ store.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{
		namespaces: map[string]store.NamespaceRecord{},
		tables:     map[string]store.Pointer{},
		views:      map[string]store.Pointer{},
		purges:     map[string]store.PurgeTask{},
		intents:    map[string]store.Intent{},
		now:        time.Now,
	}
}

// Ping always succeeds unless ctx is done.
func (s *Store) Ping(ctx context.Context) error {
	return ctxErr(ctx)
}

// CreateNamespace implements [store.Store].
func (s *Store) CreateNamespace(ctx context.Context, ns store.Namespace, props map[string]string) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}

	if err := ns.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.namespaces[ns.Encode()]; ok {
		return fmt.Errorf("%w: %s", store.ErrNamespaceAlreadyExists, ns.String())
	}

	if err := s.requireNamespace(ns.Parent()); err != nil {
		return err
	}

	s.namespaces[ns.Encode()] = store.NamespaceRecord{
		Namespace:  slices.Clone(ns),
		Properties: cloneProps(props),
		Version:    s.nextVersion(),
	}

	return nil
}

// GetNamespace implements [store.Store].
func (s *Store) GetNamespace(ctx context.Context, ns store.Namespace) (store.NamespaceRecord, error) {
	if err := ctxErr(ctx); err != nil {
		return store.NamespaceRecord{}, err
	}

	if err := ns.Validate(); err != nil {
		return store.NamespaceRecord{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	rec, ok := s.namespaces[ns.Encode()]
	if !ok {
		return store.NamespaceRecord{}, fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, ns.String())
	}

	return cloneNamespaceRecord(rec), nil
}

// ListNamespaces implements [store.Store].
func (s *Store) ListNamespaces(
	ctx context.Context,
	parent store.Namespace,
	req store.PageRequest,
) (store.Page[store.Namespace], error) {
	if err := ctxErr(ctx); err != nil {
		return store.Page[store.Namespace]{}, err
	}

	if !parent.IsRoot() {
		if err := parent.Validate(); err != nil {
			return store.Page[store.Namespace]{}, err
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.requireNamespace(parent); err != nil {
		return store.Page[store.Namespace]{}, err
	}

	children := []store.Namespace{}
	for _, rec := range s.namespaces {
		if rec.Namespace.Parent().Equal(parent) {
			children = append(children, slices.Clone(rec.Namespace))
		}
	}

	slices.SortFunc(children, func(a, b store.Namespace) int {
		return strings.Compare(a.Name(), b.Name())
	})

	return store.Paginate(children, store.Namespace.Name, req)
}

// UpdateNamespaceProperties implements [store.Store].
func (s *Store) UpdateNamespaceProperties(
	ctx context.Context,
	ns store.Namespace,
	upd store.PropertiesUpdate,
) (store.PropertiesUpdateResult, error) {
	if err := ctxErr(ctx); err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	if err := ns.Validate(); err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	if err := upd.Validate(); err != nil {
		return store.PropertiesUpdateResult{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.namespaces[ns.Encode()]
	if !ok {
		return store.PropertiesUpdateResult{}, fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, ns.String())
	}

	props := cloneProps(rec.Properties)
	result := store.PropertiesUpdateResult{
		Updated: slices.AppendSeq(make([]string, 0, len(upd.Updates)), maps.Keys(upd.Updates)),
		Removed: []string{},
		Missing: []string{},
	}

	maps.Copy(props, upd.Updates)

	for _, key := range upd.Removals {
		if _, exists := props[key]; !exists {
			result.Missing = append(result.Missing, key)

			continue
		}

		delete(props, key)
		result.Removed = append(result.Removed, key)
	}

	slices.Sort(result.Updated)
	slices.Sort(result.Removed)
	slices.Sort(result.Missing)

	rec.Properties = props
	rec.Version = s.nextVersion()
	s.namespaces[ns.Encode()] = rec

	return result, nil
}

// DropNamespace implements [store.Store].
func (s *Store) DropNamespace(ctx context.Context, ns store.Namespace) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}

	if err := ns.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.namespaces[ns.Encode()]; !ok {
		return fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, ns.String())
	}

	if s.hasChildren(ns) {
		return fmt.Errorf("%w: %s", store.ErrNamespaceNotEmpty, ns.String())
	}

	delete(s.namespaces, ns.Encode())
	s.nextVersion()

	return nil
}

// GetTable implements [store.Store].
func (s *Store) GetTable(ctx context.Context, id store.Ident) (store.Pointer, error) {
	return s.get(ctx, s.tables, id, store.ErrTableNotFound)
}

// GetTables implements [store.Store].
func (s *Store) GetTables(ctx context.Context, ids []store.Ident) ([]store.Pointer, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}

	if err := validateIdents(ids); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	ptrs := make([]store.Pointer, 0, len(ids))
	for _, id := range ids {
		p, ok := s.tables[identKey(id)]
		if !ok {
			p = store.Pointer{Ident: cloneIdent(id)}
		}

		ptrs = append(ptrs, clonePointer(p))
	}

	return ptrs, nil
}

// ListTables implements [store.Store].
func (s *Store) ListTables(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return s.list(ctx, s.tables, ns, req)
}

// CommitTables implements [store.Store].
func (s *Store) CommitTables(ctx context.Context, commits []store.Commit) ([]store.Pointer, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}

	if err := validateCommits(commits); err != nil {
		return nil, err
	}

	intent, err := store.CommitsIntent(commits)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check every commit before applying any, so the batch is all or nothing.
	for _, c := range commits {
		if err := s.checkCommit(c, s.tables, store.ErrTableNotFound); err != nil {
			return nil, err
		}
	}

	if err := s.takeIntent(intent); err != nil {
		return nil, err
	}

	version := s.nextVersion()
	result := make([]store.Pointer, 0, len(commits))

	for _, c := range commits {
		p := store.Pointer{
			Ident:            cloneIdent(c.Ident),
			MetadataLocation: c.MetadataLocation,
			Version:          version,
		}
		s.tables[identKey(c.Ident)] = p
		result = append(result, clonePointer(p))
	}

	return result, nil
}

// RenameTable implements [store.Store].
func (s *Store) RenameTable(ctx context.Context, from, to store.Ident) error {
	return s.rename(ctx, s.tables, from, to, store.ErrTableNotFound)
}

// DropTable implements [store.Store].
func (s *Store) DropTable(ctx context.Context, id store.Ident, opts store.DropOptions) (store.Pointer, error) {
	var task store.PurgeTask

	if opts.Purge {
		taskID, err := store.NewPurgeTaskID()
		if err != nil {
			return store.Pointer{}, err
		}

		task.ID = taskID
	}

	return s.drop(ctx, s.tables, id, store.ErrTableNotFound, func(p store.Pointer) {
		if !opts.Purge {
			return
		}

		task.Ident = cloneIdent(p.Ident)
		task.MetadataLocation = p.MetadataLocation
		task.Created = s.now()
		task.Version = s.nextVersion()
		s.purges[task.ID] = task
	})
}

// GetView implements [store.Store].
func (s *Store) GetView(ctx context.Context, id store.Ident) (store.Pointer, error) {
	return s.get(ctx, s.views, id, store.ErrViewNotFound)
}

// ListViews implements [store.Store].
func (s *Store) ListViews(
	ctx context.Context,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	return s.list(ctx, s.views, ns, req)
}

// CommitView implements [store.Store].
func (s *Store) CommitView(ctx context.Context, commit store.Commit) (store.Pointer, error) {
	if err := ctxErr(ctx); err != nil {
		return store.Pointer{}, err
	}

	if err := commit.Validate(); err != nil {
		return store.Pointer{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.checkCommit(commit, s.views, store.ErrViewNotFound); err != nil {
		return store.Pointer{}, err
	}

	if err := s.takeIntent(commit.Intent); err != nil {
		return store.Pointer{}, err
	}

	p := store.Pointer{
		Ident:            cloneIdent(commit.Ident),
		MetadataLocation: commit.MetadataLocation,
		Version:          s.nextVersion(),
	}
	s.views[identKey(commit.Ident)] = p

	return clonePointer(p), nil
}

// RenameView implements [store.Store].
func (s *Store) RenameView(ctx context.Context, from, to store.Ident) error {
	return s.rename(ctx, s.views, from, to, store.ErrViewNotFound)
}

// DropView implements [store.Store].
func (s *Store) DropView(ctx context.Context, id store.Ident) error {
	_, err := s.drop(ctx, s.views, id, store.ErrViewNotFound, nil)

	return err
}

// ListPurgeTasks implements [store.Store].
func (s *Store) ListPurgeTasks(ctx context.Context, limit int) ([]store.PurgeTask, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}

	if limit <= 0 {
		return nil, fmt.Errorf("%w: non-positive limit %d", store.ErrInvalidArgument, limit)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := slices.Sorted(maps.Keys(s.purges))
	ids = ids[:min(limit, len(ids))]
	tasks := make([]store.PurgeTask, 0, len(ids))

	for _, id := range ids {
		tasks = append(tasks, clonePurgeTask(s.purges[id]))
	}

	return tasks, nil
}

// ClaimPurgeTask implements [store.Store].
func (s *Store) ClaimPurgeTask(
	ctx context.Context,
	task store.PurgeTask,
	claim store.PurgeClaim,
) (store.PurgeTask, error) {
	if err := ctxErr(ctx); err != nil {
		return store.PurgeTask{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := s.purgeTaskAt(task)
	if err != nil {
		return store.PurgeTask{}, err
	}

	current.Claim = claim
	current.Version = s.nextVersion()
	s.purges[current.ID] = current

	return clonePurgeTask(current), nil
}

// CompletePurgeTask implements [store.Store].
func (s *Store) CompletePurgeTask(ctx context.Context, task store.PurgeTask) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.purgeTaskAt(task); err != nil {
		return err
	}

	delete(s.purges, task.ID)
	s.nextVersion()

	return nil
}

// CreateIntent implements [store.Store].
func (s *Store) CreateIntent(ctx context.Context, intent store.Intent) (store.Intent, error) {
	if err := ctxErr(ctx); err != nil {
		return store.Intent{}, err
	}

	if err := intent.Validate(); err != nil {
		return store.Intent{}, err
	}

	id, err := store.NewIntentID()
	if err != nil {
		return store.Intent{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	stored := cloneIntent(intent)
	stored.ID = id
	stored.Created = s.now()
	stored.Condemned = false
	stored.Version = s.nextVersion()
	s.intents[id] = stored

	return cloneIntent(stored), nil
}

// ListIntents implements [store.Store].
func (s *Store) ListIntents(ctx context.Context, limit int) ([]store.Intent, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}

	if limit <= 0 {
		return nil, fmt.Errorf("%w: non-positive limit %d", store.ErrInvalidArgument, limit)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	ids := slices.Sorted(maps.Keys(s.intents))
	ids = ids[:min(limit, len(ids))]
	intents := make([]store.Intent, 0, len(ids))

	for _, id := range ids {
		intents = append(intents, cloneIntent(s.intents[id]))
	}

	return intents, nil
}

// CondemnIntent implements [store.Store].
func (s *Store) CondemnIntent(ctx context.Context, ref store.IntentRef) (store.Intent, error) {
	if err := ctxErr(ctx); err != nil {
		return store.Intent{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := s.intentAt(ref)
	if err != nil {
		return store.Intent{}, err
	}

	current.Condemned = true
	current.Version = s.nextVersion()
	s.intents[current.ID] = current

	return cloneIntent(current), nil
}

// ReleaseIntent implements [store.Store].
func (s *Store) ReleaseIntent(ctx context.Context, ref store.IntentRef) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.intentAt(ref); err != nil {
		return err
	}

	delete(s.intents, ref.ID)
	s.nextVersion()

	return nil
}

// takeIntent removes the intent a commit names, after checking it is still
// at its version; a zero ref is no intent. It must be called with s.mu held,
// after every other check of the commit passed.
func (s *Store) takeIntent(ref store.IntentRef) error {
	if ref.IsZero() {
		return nil
	}

	if _, err := s.intentAt(ref); err != nil {
		return err
	}

	delete(s.intents, ref.ID)

	return nil
}

// intentAt returns the stored intent, if it is still at ref's version. It
// must be called with s.mu held.
func (s *Store) intentAt(ref store.IntentRef) (store.Intent, error) {
	current, ok := s.intents[ref.ID]
	if !ok || current.Version != ref.Version {
		return store.Intent{}, fmt.Errorf("%w: %q", store.ErrIntentChanged, ref.ID)
	}

	return current, nil
}

// purgeTaskAt returns the stored task, if it is still at task's version. It
// must be called with s.mu held.
func (s *Store) purgeTaskAt(task store.PurgeTask) (store.PurgeTask, error) {
	current, ok := s.purges[task.ID]
	if !ok || current.Version != task.Version {
		return store.PurgeTask{}, fmt.Errorf("%w: %q", store.ErrPurgeTaskChanged, task.ID)
	}

	return current, nil
}

func (s *Store) get(
	ctx context.Context,
	entries map[string]store.Pointer,
	id store.Ident,
	errNotFound error,
) (store.Pointer, error) {
	if err := ctxErr(ctx); err != nil {
		return store.Pointer{}, err
	}

	if err := id.Validate(); err != nil {
		return store.Pointer{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	p, ok := entries[identKey(id)]
	if !ok {
		return store.Pointer{}, fmt.Errorf("%w: %s", errNotFound, id.String())
	}

	return clonePointer(p), nil
}

func (s *Store) list(
	ctx context.Context,
	entries map[string]store.Pointer,
	ns store.Namespace,
	req store.PageRequest,
) (store.Page[store.Ident], error) {
	if err := ctxErr(ctx); err != nil {
		return store.Page[store.Ident]{}, err
	}

	if err := ns.Validate(); err != nil {
		return store.Page[store.Ident]{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.requireNamespace(ns); err != nil {
		return store.Page[store.Ident]{}, err
	}

	ids := []store.Ident{}
	for _, p := range entries {
		if p.Ident.Namespace.Equal(ns) {
			ids = append(ids, cloneIdent(p.Ident))
		}
	}

	slices.SortFunc(ids, func(a, b store.Ident) int {
		return strings.Compare(a.Name, b.Name)
	})

	return store.Paginate(ids, func(id store.Ident) string { return id.Name }, req)
}

func (s *Store) rename(
	ctx context.Context,
	entries map[string]store.Pointer,
	from, to store.Ident,
	errNotFound error,
) error {
	if err := ctxErr(ctx); err != nil {
		return err
	}

	if err := from.Validate(); err != nil {
		return err
	}

	if err := to.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := entries[identKey(from)]
	if !ok {
		return fmt.Errorf("%w: %s", errNotFound, from.String())
	}

	if err := s.requireNamespace(to.Namespace); err != nil {
		return err
	}

	if err := s.requireNameFree(to); err != nil {
		return err
	}

	delete(entries, identKey(from))

	p.Ident = cloneIdent(to)
	p.Version = s.nextVersion()
	entries[identKey(to)] = p

	return nil
}

// drop removes a pointer and returns it. onDrop, if set, runs under the lock
// after the removal, so its writes are atomic with it.
func (s *Store) drop(
	ctx context.Context,
	entries map[string]store.Pointer,
	id store.Ident,
	errNotFound error,
	onDrop func(store.Pointer),
) (store.Pointer, error) {
	if err := ctxErr(ctx); err != nil {
		return store.Pointer{}, err
	}

	if err := id.Validate(); err != nil {
		return store.Pointer{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := entries[identKey(id)]
	if !ok {
		return store.Pointer{}, fmt.Errorf("%w: %s", errNotFound, id.String())
	}

	delete(entries, identKey(id))
	s.nextVersion()

	if onDrop != nil {
		onDrop(p)
	}

	return clonePointer(p), nil
}

// checkCommit reports whether c can be applied to entries. It must be called
// with s.mu held.
func (s *Store) checkCommit(c store.Commit, entries map[string]store.Pointer, errNotFound error) error {
	current, exists := entries[identKey(c.Ident)]

	switch {
	case c.IsCreate():
		if err := s.requireNamespace(c.Ident.Namespace); err != nil {
			return err
		}

		return s.requireNameFree(c.Ident)
	case !exists:
		return fmt.Errorf("%w: %s", errNotFound, c.Ident.String())
	case current.Version != c.ExpectedVersion:
		return &store.CommitConflictError{
			Ident:    cloneIdent(c.Ident),
			Expected: c.ExpectedVersion,
			Actual:   current.Version,
		}
	default:
		return nil
	}
}

// requireNamespace fails unless ns exists. The root always exists. It must be
// called with s.mu held.
func (s *Store) requireNamespace(ns store.Namespace) error {
	if ns.IsRoot() {
		return nil
	}

	if _, ok := s.namespaces[ns.Encode()]; !ok {
		return fmt.Errorf("%w: %s", store.ErrNamespaceNotFound, ns.String())
	}

	return nil
}

// requireNameFree fails if id is taken by a table or a view, which share one
// name space. It must be called with s.mu held.
func (s *Store) requireNameFree(id store.Ident) error {
	key := identKey(id)

	if _, ok := s.tables[key]; ok {
		return fmt.Errorf("%w: %s", store.ErrTableAlreadyExists, id.String())
	}

	if _, ok := s.views[key]; ok {
		return fmt.Errorf("%w: %s", store.ErrViewAlreadyExists, id.String())
	}

	return nil
}

// hasChildren reports whether ns contains namespaces, tables or views. It must
// be called with s.mu held.
func (s *Store) hasChildren(ns store.Namespace) bool {
	for _, rec := range s.namespaces {
		if rec.Namespace.Parent().Equal(ns) {
			return true
		}
	}

	for _, entries := range []map[string]store.Pointer{s.tables, s.views} {
		for _, p := range entries {
			if p.Ident.Namespace.Equal(ns) {
				return true
			}
		}
	}

	return false
}

// nextVersion issues a new version. It must be called with s.mu held.
func (s *Store) nextVersion() store.Version {
	s.rev++

	return s.rev
}

func validateCommits(commits []store.Commit) error {
	if len(commits) == 0 {
		return fmt.Errorf("%w: no commits", store.ErrInvalidArgument)
	}

	seen := make(map[string]struct{}, len(commits))

	for _, c := range commits {
		if err := c.Validate(); err != nil {
			return err
		}

		key := identKey(c.Ident)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("%w: duplicate commit for %q", store.ErrInvalidArgument, c.Ident.String())
		}

		seen[key] = struct{}{}
	}

	return nil
}

// validateIdents checks the identifiers of a batch read: valid, and none
// twice.
func validateIdents(ids []store.Ident) error {
	seen := make(map[string]struct{}, len(ids))

	for _, id := range ids {
		if err := id.Validate(); err != nil {
			return err
		}

		key := identKey(id)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("%w: %q requested twice", store.ErrInvalidArgument, id.String())
		}

		seen[key] = struct{}{}
	}

	return nil
}

// identKey is the map key of an identifier. Validated names never contain the
// separator, so keys are unambiguous.
func identKey(id store.Ident) string {
	return id.Namespace.Child(id.Name).Encode()
}

func ctxErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("memstore: %w", err)
	}

	return nil
}

func cloneProps(props map[string]string) map[string]string {
	if props == nil {
		return map[string]string{}
	}

	return maps.Clone(props)
}

func cloneIdent(id store.Ident) store.Ident {
	return store.Ident{
		Namespace: slices.Clone(id.Namespace),
		Name:      id.Name,
	}
}

func clonePointer(p store.Pointer) store.Pointer {
	p.Ident = cloneIdent(p.Ident)

	return p
}

func clonePurgeTask(t store.PurgeTask) store.PurgeTask {
	t.Ident = cloneIdent(t.Ident)

	return t
}

func cloneIntent(i store.Intent) store.Intent {
	i.Locations = slices.Clone(i.Locations)

	idents := make([]store.Ident, 0, len(i.Idents))
	for _, id := range i.Idents {
		idents = append(idents, cloneIdent(id))
	}

	i.Idents = idents

	return i
}

func cloneNamespaceRecord(rec store.NamespaceRecord) store.NamespaceRecord {
	return store.NamespaceRecord{
		Namespace:  slices.Clone(rec.Namespace),
		Properties: cloneProps(rec.Properties),
		Version:    rec.Version,
	}
}
