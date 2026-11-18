package catalog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/fileio"
	"echpochmak/internal/store"
)

// Kinds of [Finding].
const (
	// FindingAhead: newer metadata files continue the current one in a single
	// chain; the table can be rolled forward to the newest.
	FindingAhead = "ahead"
	// FindingAmbiguous: two or more newer files continue the same parent;
	// one was a commit that never landed, and a person must choose.
	FindingAmbiguous = "ambiguous"
	// FindingMissingMetadata: the pointer's metadata file is gone.
	FindingMissingMetadata = "missing-metadata"
	// FindingMissingFiles: a manifest list or manifest the current snapshot
	// needs is gone.
	FindingMissingFiles = "missing-files"
	// FindingUnregistered: a metadata directory in the warehouse that no
	// pointer leads to, such as a table created after the backup.
	FindingUnregistered = "unregistered"
)

// Finding is one thing a restore report found about a table.
type Finding struct {
	Kind string `json:"finding"`
	// Ident is the table; zero for an unregistered directory.
	Ident store.Ident `json:"-"`
	Table string      `json:"table,omitempty"`
	// Current is the metadata file the pointer names, or, for an ambiguous
	// table, the newest file that every candidate continues.
	Current string `json:"current,omitempty"`
	// Newest is the file to roll forward to, or to register.
	Newest string `json:"newest,omitempty"`
	// Commits is the number of commits a roll-forward recovers.
	Commits int `json:"commits,omitempty"`
	// Candidates are the competing files of an ambiguous table.
	Candidates []string `json:"candidates,omitempty"`
	// Missing is the first missing file found.
	Missing string `json:"missing,omitempty"`
	Detail  string `json:"detail,omitempty"`
}

// Lister lists the files under a prefix; [fileio.Iceberg] implements it.
type Lister interface {
	List(ctx context.Context, prefix string) ([]fileio.ObjectInfo, error)
}

// ReportOptions configures [Service.RestoreReport].
type ReportOptions struct {
	Lister Lister
	// Tables limits the report to these tables; empty means every table.
	Tables []store.Ident
	// Deep also checks that the current snapshot's manifest list and
	// manifests exist.
	Deep bool
	// ScanWarehouse also looks for metadata directories under the warehouse
	// that no pointer leads to.
	ScanWarehouse bool
}

// RestoreReport compares the catalog's pointers with the metadata files in
// object storage, after the KV store was restored from a backup: it finds
// tables whose metadata moved past the restored pointer, and pointers whose
// files are gone. It only reads. See arch/phase5/plan.md (D7).
func (s *Service) RestoreReport(ctx context.Context, opts ReportOptions) ([]Finding, error) {
	ids := opts.Tables
	if len(ids) == 0 {
		var err error

		ids, err = s.allTables(ctx)
		if err != nil {
			return nil, err
		}
	}

	findings := []Finding{}
	knownDirs := map[string]bool{}

	for _, id := range ids {
		ptr, err := s.store.GetTable(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("reading the pointer of %s: %w", id.String(), err)
		}

		knownDirs[dirOf(ptr.MetadataLocation)] = true

		found, err := s.reportTable(ctx, opts, ptr)
		if err != nil {
			return nil, fmt.Errorf("checking %s: %w", id.String(), err)
		}

		findings = append(findings, found...)
	}

	if !opts.ScanWarehouse {
		return findings, nil
	}

	if err := s.addViewDirs(ctx, knownDirs); err != nil {
		return nil, err
	}

	unregistered, err := s.unregisteredDirs(ctx, opts.Lister, knownDirs)
	if err != nil {
		return nil, err
	}

	return append(findings, unregistered...), nil
}

// reportTable checks one table.
func (s *Service) reportTable(ctx context.Context, opts ReportOptions, ptr store.Pointer) ([]Finding, error) {
	base := Finding{Ident: ptr.Ident, Table: ptr.Ident.String(), Current: ptr.MetadataLocation}

	current, _, err := s.readMetadata(ctx, ptr.MetadataLocation)
	if errors.Is(err, ErrMetadataMissing) {
		base.Kind = FindingMissingMetadata

		return []Finding{base}, nil
	}

	if err != nil {
		return nil, err
	}

	findings := []Finding{}

	if chain, err := s.newerMetadata(ctx, opts.Lister, ptr.MetadataLocation, current); err != nil {
		return nil, err
	} else if chain.Kind != "" {
		chain.Ident, chain.Table = base.Ident, base.Table
		findings = append(findings, chain)
	}

	if opts.Deep {
		missing, err := s.missingSnapshotFile(ctx, current)
		if err != nil {
			return nil, err
		}

		if missing != "" {
			f := base
			f.Kind, f.Missing = FindingMissingFiles, missing
			findings = append(findings, f)
		}
	}

	return findings, nil
}

// newerMetadata follows the metadata files in the current file's directory
// that continue it, each naming the previous one last in its metadata log.
// It returns an "ahead" or "ambiguous" finding, or a zero one.
func (s *Service) newerMetadata(
	ctx context.Context,
	lister Lister,
	location string,
	current table.Metadata,
) (Finding, error) {
	dir := dirOf(location) + "/"

	objects, err := lister.List(ctx, dir)
	if err != nil {
		return Finding{}, fmt.Errorf("listing %s: %w", dir, err)
	}

	known := map[string]bool{location: true}
	for entry := range current.PreviousFiles() {
		known[entry.MetadataFile] = true
	}

	children := map[string][]string{}

	for _, o := range objects {
		if known[o.Location] || !isMetadataFile(o.Location) {
			continue
		}

		md, _, err := s.readMetadata(ctx, o.Location)
		if err != nil || md.TableUUID() != current.TableUUID() {
			continue // not a readable file of this table
		}

		var parent string
		for entry := range md.PreviousFiles() {
			parent = entry.MetadataFile
		}

		children[parent] = append(children[parent], o.Location)
	}

	at, commits := location, 0

	for {
		next := children[at]

		switch len(next) {
		case 0:
			if commits == 0 {
				return Finding{}, nil
			}

			return Finding{Kind: FindingAhead, Current: location, Newest: at, Commits: commits}, nil
		case 1:
			at = next[0]
			commits++
		default:
			slices.Sort(next)

			return Finding{
				Kind:       FindingAmbiguous,
				Current:    at,
				Newest:     at,
				Commits:    commits,
				Candidates: next,
				Detail:     "several files continue the same metadata file; choose one by its snapshot summary",
			}, nil
		}
	}
}

// missingSnapshotFile returns the first missing manifest list or manifest of
// md's current snapshot, or "" if none is missing.
func (s *Service) missingSnapshotFile(ctx context.Context, md table.Metadata) (string, error) {
	snap := md.CurrentSnapshot()
	if snap == nil || snap.ManifestList == "" {
		return "", nil
	}

	if _, err := s.files.Read(ctx, snap.ManifestList); err != nil {
		if errors.Is(err, fileio.ErrNotFound) {
			return snap.ManifestList, nil
		}

		return "", fmt.Errorf("reading %s: %w", snap.ManifestList, err)
	}

	manifests, err := snap.Manifests(fileio.AsIcebergIO(ctx, s.files))
	if err != nil {
		return "", fmt.Errorf("reading the manifest list %s: %w", snap.ManifestList, err)
	}

	for _, m := range manifests {
		if _, err := s.files.Read(ctx, m.FilePath()); err != nil {
			if errors.Is(err, fileio.ErrNotFound) {
				return m.FilePath(), nil
			}

			return "", fmt.Errorf("reading %s: %w", m.FilePath(), err)
		}
	}

	return "", nil
}

// unregisteredDirs finds metadata directories under the warehouse that no
// known pointer leads to, and names the newest file of each.
func (s *Service) unregisteredDirs(ctx context.Context, lister Lister, knownDirs map[string]bool) ([]Finding, error) {
	objects, err := lister.List(ctx, s.warehouse+"/")
	if err != nil {
		return nil, fmt.Errorf("listing the warehouse %s: %w", s.warehouse, err)
	}

	newest := map[string]string{}
	version := map[string]int{}

	for _, o := range objects {
		dir := dirOf(o.Location)
		if knownDirs[dir] || path.Base(dir) != "metadata" || !isMetadataFile(o.Location) {
			continue
		}

		if v := fileVersion(o.Location); newest[dir] == "" || v > version[dir] {
			newest[dir], version[dir] = o.Location, v
		}
	}

	findings := []Finding{}

	for _, dir := range slices.Sorted(maps.Keys(newest)) {
		raw, err := s.readMetadataJSON(ctx, newest[dir])
		if err != nil {
			return nil, err
		}

		detail := "a table no pointer leads to; register it"
		if isViewMetadata(raw) {
			detail = "a view no pointer leads to; register it with register-view"
		}

		findings = append(findings, Finding{Kind: FindingUnregistered, Newest: newest[dir], Detail: detail})
	}

	return findings, nil
}

// addViewDirs adds the metadata directories of every view to dirs, so the
// warehouse scan does not report them.
func (s *Service) addViewDirs(ctx context.Context, dirs map[string]bool) error {
	namespaces, err := s.allNamespaces(ctx)
	if err != nil {
		return err
	}

	for _, ns := range namespaces {
		views, err := collectPages(func(req store.PageRequest) (store.Page[store.Ident], error) {
			return s.store.ListViews(ctx, ns, req)
		})
		if err != nil {
			return fmt.Errorf("listing the views of %s: %w", ns.String(), err)
		}

		for _, id := range views {
			ptr, err := s.store.GetView(ctx, id)
			if err != nil {
				return fmt.Errorf("reading the pointer of view %s: %w", id.String(), err)
			}

			dirs[dirOf(ptr.MetadataLocation)] = true
		}
	}

	return nil
}

// allTables returns every table of the catalog.
func (s *Service) allTables(ctx context.Context) ([]store.Ident, error) {
	namespaces, err := s.allNamespaces(ctx)
	if err != nil {
		return nil, err
	}

	ids := []store.Ident{}

	for _, ns := range namespaces {
		tables, err := collectPages(func(req store.PageRequest) (store.Page[store.Ident], error) {
			return s.store.ListTables(ctx, ns, req)
		})
		if err != nil {
			return nil, fmt.Errorf("listing the tables of %s: %w", ns.String(), err)
		}

		ids = append(ids, tables...)
	}

	return ids, nil
}

// allNamespaces returns every namespace, depth first.
func (s *Service) allNamespaces(ctx context.Context) ([]store.Namespace, error) {
	out := []store.Namespace{}
	queue := []store.Namespace{{}}

	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]

		children, err := collectPages(func(req store.PageRequest) (store.Page[store.Namespace], error) {
			return s.store.ListNamespaces(ctx, parent, req)
		})
		if err != nil {
			return nil, fmt.Errorf("listing namespaces: %w", err)
		}

		out = append(out, children...)
		queue = append(queue, children...)
	}

	return out, nil
}

// collectPages returns every item of a paginated listing.
func collectPages[T any](list func(store.PageRequest) (store.Page[T], error)) ([]T, error) {
	items := []T{}
	req := store.PageRequest{Size: store.MaxPageSize}

	for {
		page, err := list(req)
		if err != nil {
			return nil, err
		}

		items = append(items, page.Items...)

		if page.NextToken == "" {
			return items, nil
		}

		req.Token = page.NextToken
	}
}

// dirOf returns the directory of a location URI. path.Dir would collapse
// the "//" after the scheme.
func dirOf(location string) string {
	return location[:strings.LastIndex(location, "/")]
}

func isMetadataFile(location string) bool {
	name := path.Base(location)

	return metadataFileName.MatchString(name) || legacyMetadataFileName.MatchString(name)
}

// fileVersion returns the version number in a metadata file's name, or -1.
func fileVersion(location string) int {
	name := path.Base(location)

	for _, re := range []*regexp.Regexp{metadataFileName, legacyMetadataFileName} {
		if m := re.FindStringSubmatch(name); m != nil {
			if v, err := strconv.Atoi(m[1]); err == nil {
				return v
			}
		}
	}

	return -1
}

func isViewMetadata(raw []byte) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}

	_, ok := probe["view-uuid"]

	return ok
}

// RegisterCommand returns a curl command that rolls f's table forward with
// register and overwrite, through the catalog at catalogURL with the given
// prefix. For an unregistered directory the namespace and name are
// placeholders.
func RegisterCommand(f Finding, catalogURL, prefix string) string {
	ns, name := "<namespace>", "<name>"
	overwrite := false

	if f.Ident.Name != "" {
		ns = strings.Join(f.Ident.Namespace, "%1F")
		name = f.Ident.Name
		overwrite = true
	}

	body, _ := json.Marshal(map[string]any{ //nolint:errchkjson // strings and a bool always encode.
		"name":              name,
		"metadata-location": f.Newest,
		"overwrite":         overwrite,
	})

	return fmt.Sprintf("curl -fsS -X POST %s/v1/%s/namespaces/%s/register -H 'Content-Type: application/json' -d '%s'",
		strings.TrimSuffix(catalogURL, "/"), prefix, ns, body)
}
