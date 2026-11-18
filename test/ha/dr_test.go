//go:build dr

// The disaster drills of Phase 5 (arch/phase5/plan.md, Step 7), on the stack
// of make dr-e2e: 5 kv-nodes in 3 zones, two catalogs behind HAProxy,
// kv-backup, Prometheus, Grafana and Jaeger. Run it with make dr-e2e, which
// starts the stack and removes it afterwards.
//
// The drills run in order and change the cluster: the later ones restore it
// from backups. Each one logs what it measured: rows lost, time without
// writes.
package ha_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/fileio"
)

const (
	fiveNodes = "1=http://kv1:2380,2=http://kv2:2380,3=http://kv3:2380,4=http://kv4:2380,5=http://kv5:2380"
	// orphanWait covers the zone stack's orphan grace (20 s) and two janitor
	// intervals (5 s each).
	orphanWait = 35 * time.Second
	promURL    = "http://localhost:9090"
	jaegerURL  = "http://localhost:16686"
	network    = "echpochmak_default"
)

func kvServices() []string { return []string{"kv1", "kv2", "kv3", "kv4", "kv5"} }

func containers(services ...string) []string {
	out := make([]string, 0, len(services))
	for _, s := range services {
		out = append(out, container(s))
	}

	return out
}

// compose runs docker compose on the drill stack, with extra environment
// variables, and returns its output; it fails the test on an error.
func compose(t *testing.T, envs []string, args ...string) string {
	t.Helper()

	base := strings.Fields(env("DR_COMPOSE", "docker compose"))
	cmd := exec.CommandContext(t.Context(), base[0], append(base[1:], args...)...)
	cmd.Env = append(os.Environ(), envs...)

	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, out)
	}

	return string(out)
}

// mustDocker runs docker and fails the test on an error.
func mustDocker(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}

	return string(out)
}

// shared returns a directory that the containers' non-root user can write,
// for snapshot files.
func shared(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	// The containers run as another user.
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}

	return dir
}

func TestDR(t *testing.T) {
	t.Setenv("HA_KV_ENDPOINTS", "localhost:23791,localhost:23792,localhost:23793,localhost:23794,localhost:23795")

	s := newStack(t)

	t.Run("1 zone loss", func(t *testing.T) { testZoneLoss(t, s.in(t)) })
	t.Run("2 orphans under faults", func(t *testing.T) { testOrphansUnderFaults(t, s.in(t)) })
	t.Run("3 observability", func(t *testing.T) { testObservability(t, s.in(t)) })
	t.Run("4 backups", func(t *testing.T) { testBackups(t, s.in(t)) })
	t.Run("5 total loss, restore and roll-forward", func(t *testing.T) { testTotalLoss(t, s.in(t)) })
	t.Run("6 unsafe recovery from one node", func(t *testing.T) { testUnsafeRecovery(t, s.in(t)) })
}

// testZoneLoss kills zone a (2 of 5 nodes) under load: no errors beyond
// retries, no row lost. Then one more node: the quorum is lost and both
// catalogs report unready, until the nodes come back.
func testZoneLoss(t *testing.T, s *stack) {
	t.Helper()

	s.underLoad("zone_a", func() {
		t.Log("killing zone a: kv1, kv2")
		docker(t, append([]string{"kill"}, containers("kv1", "kv2")...)...)
		time.Sleep(10 * time.Second)
		docker(t, append([]string{"start"}, containers("kv1", "kv2")...)...)
	})

	t.Log("killing zone a and kv3: 2 of 5 nodes left")
	docker(t, append([]string{"kill"}, containers("kv1", "kv2", "kv3")...)...)

	for _, port := range []string{"18181", "18182"} {
		s.waitStatus(port, http.StatusServiceUnavailable)
	}

	start := time.Now()

	docker(t, append([]string{"start"}, containers("kv1", "kv2", "kv3")...)...)
	s.waitReady("18181")
	s.waitReady("18182")
	t.Logf("quorum back: catalogs ready %s after the nodes were started", time.Since(start).Round(time.Second))

	s.createTable("zone_after")
	s.exec("INSERT INTO %s.zone_after VALUES ('trino', 1)", s.schema)
	s.check("zone_after", map[string]*writes{"trino": {acked: []int64{1}}})
}

// waitStatus waits until a catalog replica's /readyz answers status.
func (s *stack) waitStatus(port string, status int) {
	s.t.Helper()

	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		code, err := tryRequest(s.t.Context(), http.MethodGet, port, "/readyz", "", "")
		if err == nil && code == status {
			return
		}

		time.Sleep(250 * time.Millisecond)
	}

	s.t.Fatalf("catalog on port %s never answered %d on /readyz", port, status)
}

// testOrphansUnderFaults kills a catalog replica and the raft leader during
// commits. After the janitor's grace period, every metadata file of each
// table is its current file or in its metadata log.
func testOrphansUnderFaults(t *testing.T, s *stack) {
	t.Helper()

	s.underLoad("orphans", func() {
		leader := s.raftLeader()
		t.Logf("killing catalog-a and %s", leader)
		docker(t, "kill", container("catalog-a"), container(leader))
		time.Sleep(5 * time.Second)
		docker(t, "start", container("catalog-a"), container(leader))
	})

	s.waitReady("18181")
	time.Sleep(orphanWait)

	for _, name := range []string{"orphans_trino", "orphans_py"} {
		stray, total := s.unreferencedMetadata(name)
		if len(stray) > 0 {
			t.Errorf("%s: %d of %d metadata files are referenced by no commit: %v", name, len(stray), total, stray)
		}

		t.Logf("%s: %d metadata files, all referenced", name, total)
	}
}

func (s *stack) objects() *fileio.Iceberg {
	return fileio.NewIceberg(s.t.Context(), map[string]string{
		"s3.endpoint":                 "http://localhost:9000",
		"s3.region":                   "us-east-1",
		"s3.access-key-id":            env("HA_S3_ACCESS_KEY", "minioadmin"),
		"s3.secret-access-key":        env("HA_S3_SECRET_KEY", "minioadmin"),
		"s3.force-virtual-addressing": "false",
	})
}

// pointer returns the metadata location of a table, from the KV store.
func (s *stack) pointer(name string) string {
	s.t.Helper()

	kv, err := s.kv.Get(s.t.Context(), []byte("cat/v1/tbl/"+s.schema+"\x00"+name))
	if err != nil || kv == nil {
		s.t.Fatalf("pointer of %s: %v, %v", name, kv, err)
	}

	var ptr struct {
		MetadataLocation string `json:"metadata_location"`
	}

	if err := json.Unmarshal(kv.GetValue(), &ptr); err != nil {
		s.t.Fatal(err)
	}

	return ptr.MetadataLocation
}

// readMetadata reads and parses a table metadata file, compressed or not.
func (s *stack) readMetadata(location string) table.Metadata {
	s.t.Helper()

	data, err := s.objects().Read(s.t.Context(), location)
	if err != nil {
		s.t.Fatalf("reading %s: %v", location, err)
	}

	if bytes.HasPrefix(data, []byte{0x1f, 0x8b}) {
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			s.t.Fatal(err)
		}

		if data, err = io.ReadAll(zr); err != nil {
			s.t.Fatal(err)
		}
	}

	md, err := table.ParseMetadataBytes(data)
	if err != nil {
		s.t.Fatalf("parsing %s: %v", location, err)
	}

	return md
}

// unreferencedMetadata returns the metadata files of a table that are
// neither its current file nor in its metadata log, and how many metadata
// files it has. Files older than the oldest log entry are skipped: they are
// committed files that aged out of the log (write.metadata.previous-versions-max),
// not orphans.
func (s *stack) unreferencedMetadata(name string) (stray []string, total int) {
	s.t.Helper()

	current := s.pointer(name)
	md := s.readMetadata(current)

	referenced := map[string]bool{current: true}
	oldest := fileVersion(current)

	for entry := range md.PreviousFiles() {
		referenced[entry.MetadataFile] = true
		oldest = min(oldest, fileVersion(entry.MetadataFile))
	}

	dir := current[:strings.LastIndex(current, "/")+1]

	objects, err := s.objects().List(s.t.Context(), dir)
	if err != nil {
		s.t.Fatalf("listing %s: %v", dir, err)
	}

	for _, o := range objects {
		if !strings.HasSuffix(o.Location, ".metadata.json") {
			continue
		}

		total++

		if !referenced[o.Location] && fileVersion(o.Location) >= oldest {
			stray = append(stray, o.Location)
		}
	}

	return stray, total
}

// fileVersion returns the version number at the start of a metadata file's
// name, "00012-<uuid>.metadata.json".
func fileVersion(location string) int {
	name := location[strings.LastIndex(location, "/")+1:]
	prefix, _, _ := strings.Cut(name, "-")

	v, err := strconv.Atoi(prefix)
	if err != nil {
		return -1
	}

	return v
}

// cleanupDocker runs docker when a test ends, after its context is done.
func cleanupDocker(t *testing.T, args ...string) {
	t.Helper()

	t.Cleanup(func() {
		_ = exec.CommandContext(context.Background(), "docker", args...).Run()
	})
}

// testObservability checks the metrics in Prometheus and a trace in Jaeger,
// then stops a majority of the KV nodes and watches the metrics say so.
func testObservability(t *testing.T, s *stack) {
	t.Helper()

	for _, query := range []string{
		`sum(echpochmak_catalog_commits_total{outcome="committed"})`,
		`sum(echpochmak_http_requests_total)`,
		`max(echpochmak_kv_raft_leader_changes_total)`,
		`max(echpochmak_backup_last_success_timestamp_seconds)`,
		`count(echpochmak_kv_node_info)`,
	} {
		if v := s.promValue(query); v <= 0 {
			t.Errorf("%s = %v, want positive", query, v)
		}
	}

	if zones := s.promValue(`count(count by (zone) (echpochmak_kv_node_info))`); zones != 3 {
		t.Errorf("%v zones in the metrics, want 3", zones)
	}

	s.createTable("traced")
	s.exec("INSERT INTO %s.traced VALUES ('trino', 1)", s.schema)
	s.waitForTrace()

	t.Log("stopping kv1, kv2, kv3: no quorum")
	docker(t, append([]string{"stop", "--time", "1"}, containers("kv1", "kv2", "kv3")...)...)

	s.eventually("the remaining nodes report no leader, the catalogs unready", func() bool {
		noLeader := s.promValue(`max(echpochmak_kv_raft_leader_id{instance=~"kv4:2381|kv5:2381"})`) == 0
		unready := s.promValue(`sum(echpochmak_catalog_ready)`) == 0

		return noLeader && unready
	})

	docker(t, append([]string{"start"}, containers("kv1", "kv2", "kv3")...)...)
	s.waitReady("18181")
	s.waitReady("18182")
}

// promValue returns the first value of an instant query, or -1.
func (s *stack) promValue(query string) float64 {
	s.t.Helper()

	var resp struct {
		Data struct {
			Result []struct {
				Value [2]any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}

	s.getJSON(promURL+"/api/v1/query?query="+url.QueryEscape(query), &resp)

	if len(resp.Data.Result) == 0 {
		return -1
	}

	raw, _ := resp.Data.Result[0].Value[1].(string)

	var v float64
	if _, err := fmt.Sscan(raw, &v); err != nil {
		return -1
	}

	return v
}

// waitForTrace waits for a trace in Jaeger that holds spans of both the
// catalog and a kv-node: a commit traced down to the raft proposal.
func (s *stack) waitForTrace() {
	s.t.Helper()

	s.eventually("a trace from the catalog to the KV cluster", func() bool {
		start := url.QueryEscape(time.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339))
		end := url.QueryEscape(time.Now().UTC().Format(time.RFC3339))

		var resp struct {
			Result struct {
				ResourceSpans []struct {
					Resource struct {
						Attributes []struct {
							Key   string `json:"key"`
							Value struct {
								StringValue string `json:"stringValue"`
							} `json:"value"`
						} `json:"attributes"`
					} `json:"resource"`
					ScopeSpans []struct {
						Spans []struct {
							TraceID string `json:"traceId"`
							Name    string `json:"name"`
						} `json:"spans"`
					} `json:"scopeSpans"`
				} `json:"resourceSpans"`
			} `json:"result"`
		}

		s.getJSON(jaegerURL+"/api/v3/traces?query.service_name=catalog-server&query.search_depth=50"+
			"&query.start_time_min="+start+"&query.start_time_max="+end, &resp)

		services := map[string]map[string]bool{}

		for _, rs := range resp.Result.ResourceSpans {
			service := ""

			for _, a := range rs.Resource.Attributes {
				if a.Key == "service.name" {
					service = a.Value.StringValue
				}
			}

			for _, ss := range rs.ScopeSpans {
				for _, span := range ss.Spans {
					if services[span.TraceID] == nil {
						services[span.TraceID] = map[string]bool{}
					}

					services[span.TraceID][service] = true
				}
			}
		}

		for _, seen := range services {
			if seen["catalog-server"] && seen["kv-node"] {
				return true
			}
		}

		return false
	})
}

func (s *stack) getJSON(u string, v any) {
	s.t.Helper()

	req, err := http.NewRequestWithContext(s.t.Context(), http.MethodGet, u, http.NoBody)
	if err != nil {
		s.t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return // the caller retries
	}
	defer resp.Body.Close()

	_ = json.NewDecoder(resp.Body).Decode(v)
}

// eventually waits up to a minute for cond.
func (s *stack) eventually(what string, cond func() bool) {
	s.t.Helper()

	deadline := time.Now().Add(time.Minute)

	for time.Now().Before(deadline) {
		if cond() {
			return
		}

		time.Sleep(time.Second)
	}

	s.t.Fatalf("timed out waiting for %s", what)
}

// backupManifest is the part of a backup manifest the drills read.
type backupManifest struct {
	Snapshot  string    `json:"snapshot"`
	ClusterID string    `json:"cluster_id"`
	Revision  int64     `json:"revision"`
	Created   time.Time `json:"created"`
}

// kvBackup runs a kv-backup command in a one-off container of the
// kv-backup service, with dir mounted at /restore.
func kvBackup(t *testing.T, dir string, args ...string) string {
	t.Helper()

	run := []string{"run", "--rm", "--no-deps", "--entrypoint", "/kv-backup"}
	if dir != "" {
		run = append(run, "-v", dir+":/restore")
	}

	return compose(t, nil, append(append(run, "kv-backup"), args...)...)
}

// lastJSON decodes the last JSON object in out, after any log lines.
func lastJSON(t *testing.T, out string, v any) {
	t.Helper()

	i := strings.LastIndex(out, "\n{")
	if i < 0 {
		i = strings.Index(out, "{")
	} else {
		i++
	}

	if i < 0 || json.Unmarshal([]byte(out[i:]), v) != nil {
		t.Fatalf("no JSON in %q", out)
	}
}

// testBackups checks the service's backups, retention with a small policy,
// and the test restore.
func testBackups(t *testing.T, s *stack) {
	t.Helper()

	s.eventually("backups in the bucket", func() bool {
		return strings.Count(kvBackup(t, "", "list"), "s3://") > 0
	})

	s.exec("INSERT INTO %s.zone_after VALUES ('trino', 2)", s.schema)

	var now struct {
		Skipped bool `json:"skipped"`
	}

	lastJSON(t, kvBackup(t, "", "-keep-last", "2", "-keep-daily", "0", "now"), &now)

	if now.Skipped {
		t.Fatal("a backup after a write was skipped")
	}

	if n := strings.Count(kvBackup(t, "", "list"), "s3://"); n > 3 {
		// 2 kept, plus at most one the service took meanwhile.
		t.Errorf("%d backups after retention with keep-last 2", n)
	}

	if out := kvBackup(t, "", "verify"); !strings.Contains(out, `"verified": true`) {
		t.Fatalf("verify: %s", out)
	}
}

// testTotalLoss destroys every kv-node and its disk, restores the cluster
// from the newest backup as the runbook says, measures the time without
// writes, then finds and rolls forward the commits made after the backup.
func testTotalLoss(t *testing.T, s *stack) {
	t.Helper()

	dir := shared(t)

	// Pause the backup service: the drill decides which backup is newest.
	docker(t, "stop", container("kv-backup"))
	cleanupDocker(t, "start", container("kv-backup"))

	s.createTable("restored")

	for seq := range 10 {
		s.exec("INSERT INTO %s.restored VALUES ('trino', %d)", s.schema, seq)
	}

	var backup backupManifest

	lastJSON(t, kvBackup(t, "", "now"), &backup)
	t.Logf("backup at revision %d: %s", backup.Revision, backup.Snapshot)

	// After the backup: ten more commits to the table, a new table and a
	// namespace property. The first two are recovered by roll-forward; the
	// property is the documented loss.
	for seq := 10; seq < 20; seq++ {
		s.exec("INSERT INTO %s.restored VALUES ('trino', %d)", s.schema, seq)
	}

	s.createTable("created_after")
	s.exec("INSERT INTO %s.created_after VALUES ('trino', 1)", s.schema)
	request(t, http.MethodPost, lbPort, "/v1/main/namespaces/"+s.schema+"/properties",
		`{"updates":{"lost":"yes"}}`, "")

	// Keep the old disk of kv1, to check later that it cannot rejoin.
	docker(t, append([]string{"stop", "--time", "1"}, containers(kvServices()...)...)...)
	copyVolume(t, "echpochmak_kv1-data", "echpochmak_kv1-old")

	start := time.Now()

	t.Log("destroying every kv-node and its disk")
	compose(t, nil, append([]string{"rm", "-fsv"}, kvServices()...)...)

	for _, svc := range kvServices() {
		mustDocker(t, "volume", "rm", "echpochmak_"+svc+"-data")
	}

	restoreCluster(t, dir, "latest.snap", func() {
		kvBackup(t, dir, "fetch", "/restore/latest.snap")
	})

	compose(t, []string{"ORPHAN_CLEANUP=false"}, "up", "-d", "--no-deps", "--force-recreate", "catalog-a", "catalog-b")
	s.waitReady("18181")
	s.waitReady("18182")
	s.exec("INSERT INTO %s.zone_after VALUES ('trino', 3)", s.schema)
	t.Logf("RTO: first commit %s after the cluster was destroyed", time.Since(start).Round(time.Second))

	if n := s.count("restored"); n != 10 {
		t.Errorf("restored table has %d rows before the roll-forward, want the 10 of the backup", n)
	}

	s.rollForward()

	if n := s.count("restored"); n != 20 {
		t.Errorf("restored table has %d rows after the roll-forward, want 20", n)
	}

	if n := s.count("created_after"); n != 1 {
		t.Errorf("the table created after the backup has %d rows, want 1", n)
	}

	var ns struct {
		Properties map[string]string `json:"properties"`
	}

	request(t, http.MethodGet, lbPort, "/v1/main/namespaces/"+s.schema, "", "").decode(t, &ns)

	if _, ok := ns.Properties["lost"]; ok {
		t.Error("a namespace property set after the backup survived the restore")
	}

	// The janitor back on: it must not delete any file a pointer needs.
	compose(t, nil, "up", "-d", "--no-deps", "--force-recreate", "catalog-a", "catalog-b")
	s.waitReady("18181")
	s.waitReady("18182")
	time.Sleep(orphanWait)

	for _, name := range []string{"restored", "created_after", "zone_after"} {
		s.checkPointer(name)
	}

	if n := s.count("restored"); n != 20 {
		t.Errorf("restored table has %d rows after the janitor ran, want 20", n)
	}

	testOldNodeStaysOut(t, s)
}

// restoreCluster restores all five nodes from dir/file, which fetch writes,
// and starts them.
func restoreCluster(t *testing.T, dir, file string, fetch func()) {
	t.Helper()

	fetch()

	for i, svc := range kvServices() {
		compose(t, nil, "run", "--rm", "--no-deps", "-v", dir+":/restore:ro", "--entrypoint", "/kvctl", svc,
			"snapshot", "restore", "/restore/"+file,
			"-id", strconv.Itoa(i+1), "-data-dir", "/data", "-initial-cluster", fiveNodes)
	}

	compose(t, nil, append([]string{"up", "-d", "--no-deps"}, kvServices()...)...)
}

// copyVolume copies a docker volume, with the shell of the MinIO image: the
// node images have none.
func copyVolume(t *testing.T, from, to string) {
	t.Helper()

	// A volume left by an earlier run would merge into the copy.
	docker(t, "volume", "rm", "-f", to)

	mustDocker(t, "run", "--rm", "-v", from+":/from:ro", "-v", to+":/to", "--entrypoint", "sh",
		"pgsty/minio:RELEASE.2026-08-04T00-00-00Z", "-c", "cp -a /from/. /to/")
}

// count returns the number of rows of a table.
func (s *stack) count(name string) int {
	s.t.Helper()

	var n int

	err := s.timed(func(ctx context.Context) error {
		return s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s", s.schema, name)).Scan(&n)
	})
	if err != nil {
		s.t.Fatalf("counting %s: %v", name, err)
	}

	return n
}

// reportFinding is the part of a restore report finding the drill reads.
type reportFinding struct {
	Kind    string `json:"finding"`
	Table   string `json:"table"`
	Newest  string `json:"newest"`
	Commits int    `json:"commits"`
}

// rollForward runs the restore report in a catalog container and applies
// what it finds for the drill's schema: ahead tables are registered over
// their pointer, the unregistered table is registered under its name.
func (s *stack) rollForward() {
	s.t.Helper()

	out := mustDocker(s.t, "exec", container("catalog-a"), "/catalogctl", "restore-report", "-json", "-scan-warehouse")

	for line := range strings.Lines(out) {
		var f reportFinding
		if json.Unmarshal([]byte(line), &f) != nil {
			continue
		}

		s.t.Logf("restore report: %+v", f)

		switch {
		case f.Kind == "ahead" && f.Table == s.schema+".restored":
			if f.Commits != 10 {
				s.t.Errorf("report: %d commits after the backup, want 10", f.Commits)
			}

			s.register("restored", f.Newest, true)
		case f.Kind == "unregistered" && strings.Contains(f.Newest, "/created_after"):
			s.register("created_after", f.Newest, false)
		}
	}
}

func (s *stack) register(name, location string, overwrite bool) {
	s.t.Helper()

	body := fmt.Sprintf(`{"name":%q,"metadata-location":%q,"overwrite":%v}`, name, location, overwrite)

	resp := request(s.t, http.MethodPost, lbPort, "/v1/main/namespaces/"+s.schema+"/register", body, "")
	if resp.status != http.StatusOK {
		s.t.Fatalf("registering %s at %s: %d %s", name, location, resp.status, resp.body)
	}
}

// testOldNodeStaysOut starts kv1's disk from before the restore next to the
// restored cluster. Its cluster ID is the old one, so its peers reject it:
// it never finds a leader, and the restored cluster is unaffected.
func testOldNodeStaysOut(t *testing.T, s *stack) {
	t.Helper()

	name := "echpochmak-kv-old"
	docker(t, "rm", "-f", name) // left by an earlier run
	mustDocker(t, "run", "-d", "--name", name, "--network", network,
		"-v", "echpochmak_kv1-old:/data", "-e", "ECHPOCHMAK_KV_ID=1", "-e", "ECHPOCHMAK_KV_DATA_DIR=/data",
		"-p", "127.0.0.1::2381", "echpochmak/kv-node:dev")

	cleanupDocker(t, "rm", "-f", name)

	time.Sleep(15 * time.Second)

	port := strings.TrimSpace(mustDocker(t, "port", name, "2381"))
	port = port[strings.LastIndex(port, ":")+1:]

	code, err := tryRequest(t.Context(), http.MethodGet, port, "/readyz", "", "")
	if err != nil || code == http.StatusOK {
		t.Errorf("the old node is ready (%d, %v); it must not find a leader", code, err)
	}

	if logs := mustDocker(t, "logs", name); strings.Contains(logs, "became leader") {
		t.Error("the old node led a cluster")
	}

	s.exec("INSERT INTO %s.zone_after VALUES ('trino', 4)", s.schema)
	s.checkPointer("zone_after")
}

// testUnsafeRecovery loses every node but one, for good: the survivor's
// disk is exported offline and restored as a new cluster.
func testUnsafeRecovery(t *testing.T, s *stack) {
	t.Helper()

	dir := shared(t)
	before := s.count("restored")

	docker(t, append([]string{"stop", "--time", "1"}, containers(kvServices()...)...)...)

	start := time.Now()

	compose(t, nil, "run", "--rm", "--no-deps", "-v", dir+":/restore", "--entrypoint", "/kvctl", "kv3",
		"snapshot", "save", "/restore/kv3.snap", "-data-dir", "/data")

	compose(t, nil, append([]string{"rm", "-fsv"}, kvServices()...)...)

	for _, svc := range kvServices() {
		mustDocker(t, "volume", "rm", "echpochmak_"+svc+"-data")
	}

	restoreCluster(t, dir, "kv3.snap", func() {})
	s.waitReady("18181")
	s.waitReady("18182")
	s.exec("INSERT INTO %s.zone_after VALUES ('trino', 5)", s.schema)
	t.Logf("RTO of the unsafe recovery: %s", time.Since(start).Round(time.Second))

	if n := s.count("restored"); n != before {
		t.Errorf("restored table has %d rows after the unsafe recovery, want %d", n, before)
	}
}
