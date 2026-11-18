package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"echpochmak/internal/kv/kvtest"
	"echpochmak/internal/kv/snapshot"
)

func TestCommands(t *testing.T) {
	t.Parallel()

	c := kvtest.NewGRPCCluster(t, 3)
	if _, err := c.Client().Put(t.Context(), []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}

	flags := []string{"-endpoints", strings.Join(c.Endpoints(), ","), "-location", "mem://kv-backup-cli/kv"}
	kvBackup := func(args ...string) string {
		t.Helper()

		var out bytes.Buffer
		if err := run(append(append([]string{}, flags...), args...), &out); err != nil {
			t.Fatalf("kv-backup %v: %v", args, err)
		}

		return out.String()
	}

	var now struct {
		Snapshot string `json:"snapshot"`
		Skipped  bool   `json:"skipped"`
	}

	if err := json.Unmarshal([]byte(kvBackup("now")), &now); err != nil || now.Skipped || now.Snapshot == "" {
		t.Fatalf("now = %+v, %v", now, err)
	}

	if out := kvBackup("now"); !strings.Contains(out, `"skipped": true`) {
		t.Fatalf("second now = %s, want skipped", out)
	}

	if out := kvBackup("list"); !strings.Contains(out, now.Snapshot) {
		t.Fatalf("list = %s, want %s", out, now.Snapshot)
	}

	file := filepath.Join(t.TempDir(), "latest.snap")
	kvBackup("fetch", file)

	f, err := os.Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	if _, err := snapshot.Verify(f); err != nil {
		t.Fatalf("the fetched file is invalid: %v", err)
	}

	if out := kvBackup("verify"); !strings.Contains(out, `"verified": true`) {
		t.Fatalf("verify = %s", out)
	}
}
