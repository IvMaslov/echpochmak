package catalog_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// rewind moves a table's pointer back to location, as restoring an older
// backup of the KV store does.
func rewind(t *testing.T, f *fixture, name, location string) {
	t.Helper()

	ptr, err := f.store.GetTable(t.Context(), ident(name))
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.store.Store.CommitTables(t.Context(), []store.Commit{
		{Ident: ident(name), ExpectedVersion: ptr.Version, MetadataLocation: location},
	})
	if err != nil {
		t.Fatalf("rewinding %s: %v", name, err)
	}
}

func setProp(t *testing.T, f *fixture, name, value string) catalog.Table {
	t.Helper()

	tbl, err := f.commitJSON(t, name, `[]`, `[{"action":"set-properties","updates":{"k":"`+value+`"}}]`)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	return tbl
}

func report(t *testing.T, f *fixture, opts catalog.ReportOptions) []catalog.Finding {
	t.Helper()

	opts.Lister = f.files

	findings, err := f.svc.RestoreReport(t.Context(), opts)
	if err != nil {
		t.Fatalf("RestoreReport: %v", err)
	}

	return findings
}

func TestReportFindsCommitsAfterTheBackup(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	restored := f.createTable(t, "t", nil)
	f.createTable(t, "clean", nil)

	for _, v := range []string{"1", "2", "3"} {
		setProp(t, f, "t", v)
	}

	newest, err := f.svc.LoadTable(t.Context(), ident("t"))
	if err != nil {
		t.Fatal(err)
	}

	rewind(t, f, "t", restored.MetadataLocation)

	findings := report(t, f, catalog.ReportOptions{})
	if len(findings) != 1 {
		t.Fatalf("findings = %+v, want one for t", findings)
	}

	got := findings[0]
	if got.Kind != catalog.FindingAhead || got.Newest != newest.MetadataLocation || got.Commits != 3 {
		t.Fatalf("finding = %+v, want ahead by 3 commits to %s", got, newest.MetadataLocation)
	}

	cmd := catalog.RegisterCommand(got, "http://catalog:8181", "main")
	if !strings.Contains(cmd, "/v1/main/namespaces/db/register") || !strings.Contains(cmd, `"overwrite":true`) {
		t.Fatalf("command %q", cmd)
	}

	// Roll forward as the command would, then keep committing.
	_, err = f.svc.RegisterTable(t.Context(), catalog.RegisterTableInput{
		Ident:            ident("t"),
		MetadataLocation: got.Newest,
		Overwrite:        true,
	})
	if err != nil {
		t.Fatalf("rolling forward: %v", err)
	}

	after := setProp(t, f, "t", "4")
	if after.MetadataLocation == newest.MetadataLocation {
		t.Fatal("the commit after the roll-forward wrote nothing new")
	}

	if findings := report(t, f, catalog.ReportOptions{}); len(findings) != 0 {
		t.Fatalf("findings after the roll-forward = %+v, want none", findings)
	}
}

// A commit that never landed left a file that continues the same parent as
// a commit that did: the report cannot choose.
func TestReportFlagsAmbiguousChains(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	restored := f.createTable(t, "t", nil)

	f.store.setCommit(func(context.Context, []store.Commit) ([]store.Pointer, error) {
		return nil, store.OutcomeUnknown(errors.New("lost"))
	})

	if _, err := f.commitJSON(t, "t", `[]`, `[{"action":"set-properties","updates":{"lost":"x"}}]`); err == nil {
		t.Fatal("the failing commit succeeded")
	}

	f.store.setCommit(nil)
	setProp(t, f, "t", "landed")
	rewind(t, f, "t", restored.MetadataLocation)

	findings := report(t, f, catalog.ReportOptions{Tables: []store.Ident{ident("t")}})
	if len(findings) != 1 || findings[0].Kind != catalog.FindingAmbiguous || len(findings[0].Candidates) != 2 {
		t.Fatalf("findings = %+v, want one ambiguous with two candidates", findings)
	}
}

func TestReportFindsMissingFiles(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	gone := f.createTable(t, "gone", nil)

	if err := f.files.Delete(t.Context(), gone.MetadataLocation); err != nil {
		t.Fatal(err)
	}

	f.createTable(t, "snap", nil)

	missingList := warehouse + "/db/snap/metadata/snap-1-missing.avro"
	_, err := f.commitJSON(t, "snap", `[]`, `[
		{"action":"add-snapshot","snapshot":{"snapshot-id":1,
		 "timestamp-ms":`+strconv.FormatInt(time.Now().UnixMilli(), 10)+`,
		 "manifest-list":"`+missingList+`","summary":{"operation":"append"},"schema-id":0}},
		{"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":1}
	]`)
	if err != nil {
		t.Fatalf("adding a snapshot: %v", err)
	}

	findings := report(t, f, catalog.ReportOptions{Deep: true})

	kinds := map[string]catalog.Finding{}
	for _, finding := range findings {
		kinds[finding.Kind] = finding
	}

	if kinds[catalog.FindingMissingMetadata].Table != "db.gone" {
		t.Errorf("findings %+v lack missing-metadata for db.gone", findings)
	}

	if kinds[catalog.FindingMissingFiles].Missing != missingList {
		t.Errorf("findings %+v lack missing-files for %s", findings, missingList)
	}
}

func TestReportFindsUnregisteredTables(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	tbl := f.createTable(t, "t", nil)

	// A table created after the backup: its files exist, its pointer does
	// not.
	data, err := f.files.Read(t.Context(), tbl.MetadataLocation)
	if err != nil {
		t.Fatal(err)
	}

	orphan := warehouse + "/db/newer/metadata/00000-00000000-0000-0000-0000-000000000099.metadata.json"
	if err := f.files.Write(t.Context(), orphan, data); err != nil {
		t.Fatal(err)
	}

	findings := report(t, f, catalog.ReportOptions{ScanWarehouse: true})
	if len(findings) != 1 || findings[0].Kind != catalog.FindingUnregistered || findings[0].Newest != orphan {
		t.Fatalf("findings = %+v, want one unregistered at %s", findings, orphan)
	}

	if cmd := catalog.RegisterCommand(findings[0], "http://c", "main"); !strings.Contains(cmd, `"overwrite":false`) {
		t.Fatalf("command %q must not overwrite", cmd)
	}
}
