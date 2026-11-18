package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/node"
)

func TestPlacementWarning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		zones []string
		want  string
	}{
		{zones: []string{"a", "b", "c"}, want: ""},
		{zones: []string{"a", "a", "b", "b", "c"}, want: ""},
		{zones: []string{"a", "a", "b"}, want: `zone "a" holds 2 of 3 nodes`},
		{zones: []string{"a", "a", "a", "b", "c"}, want: `zone "a" holds 3 of 5 nodes`},
		{zones: []string{"", "", ""}, want: ""},
	}

	for _, tt := range tests {
		rows := make([]statusRow, 0, len(tt.zones))
		for _, z := range tt.zones {
			rows = append(rows, statusRow{Zone: z})
		}

		got := placementWarning(rows)
		if (tt.want == "") != (got == "") || !strings.Contains(got, tt.want) {
			t.Errorf("zones %v: warning %q, want %q", tt.zones, got, tt.want)
		}
	}
}

func kvctl(t *testing.T, endpoints string, args ...string) map[string]any {
	t.Helper()

	var out bytes.Buffer

	full := append([]string{"-endpoints", endpoints, "-json"}, args...)
	if err := run(full, &out); err != nil {
		t.Fatalf("kvctl %v: %v", args, err)
	}

	result := map[string]any{}
	if err := json.Unmarshal(out.Bytes(), &result); err != nil {
		t.Fatalf("kvctl %v printed %q: %v", args, out.String(), err)
	}

	return result
}

func TestSnapshotSaveStatusRestore(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	cl := c.Client()

	for _, k := range []string{"a", "b", "c"} {
		if _, err := cl.Put(t.Context(), []byte(k), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}

	endpoints := strings.Join(c.Endpoints(), ",")
	dir := t.TempDir()
	file := filepath.Join(dir, "backup.snap")

	saved := kvctl(t, endpoints, "snapshot", "save", file)
	status := kvctl(t, endpoints, "snapshot", "status", file)

	if saved["sha256"] == "" || saved["sha256"] != status["sha256"] || status["members"] != float64(3) {
		t.Fatalf("save = %v, status = %v", saved, status)
	}

	dataDir := filepath.Join(dir, "node1")
	restored := kvctl(t, endpoints, "snapshot", "restore", file,
		"-id", "1", "-data-dir", dataDir, "-initial-cluster", "1=http://127.0.0.1:1", "-revision-bump", "100")

	statusRevision, _ := status["revision"].(float64)
	statusIndex, _ := status["index"].(float64)

	if restored["revision"] != statusRevision+100 || restored["members"] != float64(1) {
		t.Fatalf("restore = %v, want the revision bumped by 100 and one member", restored)
	}

	var body bytes.Buffer

	exported, err := node.ExportDataDir(nil, dataDir, &body)
	if err != nil {
		t.Fatalf("reading the restored node: %v", err)
	}

	if exported.State.GetIndex() != uint64(statusIndex) || exported.ClusterID == status["cluster_id"] {
		t.Fatalf("restored node at %d in cluster %s; want index %v in a new cluster",
			exported.State.GetIndex(), exported.ClusterID, status["index"])
	}

	// A second restore into the same directory is refused.
	again := []string{
		"snapshot", "restore", file, "-id", "1", "-data-dir", dataDir, "-initial-cluster", "1=http://127.0.0.1:1",
	}

	err = run(again, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "already holds a database") {
		t.Fatalf("second restore = %v, want a refusal", err)
	}
}
