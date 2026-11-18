//go:build ha || dr

// Package ha is the Phase 3 milestone, extended in Phase 4: Trino and
// PyIceberg write through two catalog replicas on the raft store while
// catalog replicas and the raft leader are killed and restarted, and
// multi-table transactions and purges survive the same faults. Run it with make ha-e2e, which
// starts the HA stack and removes it afterwards.
//
// Every scenario checks that each acknowledged row is present exactly once,
// that no row is duplicated, and that the table pointer in the KV store
// names a metadata file that exists in MinIO.
package ha_test

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	// Registers the s3:// scheme with iceberg-go's file IO.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	_ "github.com/trinodb/trino-go-client/trino"

	"echpochmak/internal/fileio"
	"echpochmak/internal/kv/client"
)

const (
	writeDuration = 20 * time.Second
	opTimeout     = 90 * time.Second
	// lbRiseTime covers HAProxy's "rise 2" at "inter 1s", with margin.
	lbRiseTime = 3 * time.Second
	// trinoPause spaces Trino's inserts, so it does not starve the
	// PyIceberg writer with commit conflicts on a shared table.
	trinoPause = 50 * time.Millisecond
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return fallback
}

func container(service string) string { return "echpochmak-" + service + "-1" }

// stack is the running HA stack.
type stack struct {
	t  *testing.T
	db *sql.DB
	kv *client.Client
	// schema is fresh for every run, so tables of earlier runs never mix in.
	schema string
}

func newStack(t *testing.T) *stack {
	t.Helper()

	db, err := sql.Open("trino", env("HA_TRINO_URL", "http://ha@localhost:8080")+"?catalog=iceberg")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = db.Close() })

	kv, err := client.New(client.Config{
		Endpoints: strings.Split(env("HA_KV_ENDPOINTS", "localhost:23791,localhost:23792,localhost:23793"), ","),
		Logger:    slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = kv.Close() })

	s := &stack{t: t, db: db, kv: kv, schema: fmt.Sprintf("ha_%d", time.Now().Unix())}
	s.exec("CREATE SCHEMA %s", s.schema)

	return s
}

func (s *stack) exec(query string, args ...any) {
	s.t.Helper()

	ctx, cancel := context.WithTimeout(s.t.Context(), opTimeout)
	defer cancel()

	if _, err := s.db.ExecContext(ctx, fmt.Sprintf(query, args...)); err != nil {
		s.t.Fatalf("%s: %v", fmt.Sprintf(query, args...), err)
	}
}

func (s *stack) createTable(name string) {
	s.t.Helper()
	s.exec("CREATE TABLE %s.%s (writer varchar, seq bigint) WITH (partitioning = ARRAY['writer'])", s.schema, name)
}

func docker(t *testing.T, args ...string) {
	t.Helper()

	if out, err := exec.CommandContext(t.Context(), "docker", args...).CombinedOutput(); err != nil {
		t.Logf("docker %v: %v: %s", args, err, out)
	}
}

// writes is what one writer did.
type writes struct {
	acked  []int64
	errors []string
}

// trinoWriter inserts (writer, seq) rows into table until stop closes. On
// an error the outcome may be unknown, so before retrying it checks whether
// the row landed: retrying blindly could insert it twice.
func (s *stack) trinoWriter(table, writer string, start int64, stop <-chan struct{}) *writes {
	w := &writes{}

	for seq := start; ; seq++ {
		select {
		case <-stop:
			return w
		default:
		}

		for {
			err := s.timed(func(ctx context.Context) error {
				query := fmt.Sprintf("INSERT INTO %s.%s VALUES ('%s', %d)", s.schema, table, writer, seq)
				_, err := s.db.ExecContext(ctx, query)

				return err
			})
			if err == nil {
				break
			}

			w.errors = append(w.errors, err.Error())

			if s.landed(table, writer, seq) {
				break
			}

			time.Sleep(200 * time.Millisecond)
		}

		w.acked = append(w.acked, seq)
		time.Sleep(trinoPause)
	}
}

func (s *stack) timed(fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(s.t.Context(), opTimeout)
	defer cancel()

	return fn(ctx)
}

// landed reports whether the row is in the table, retrying through outages.
func (s *stack) landed(table, writer string, seq int64) bool {
	for {
		var n int

		err := s.timed(func(ctx context.Context) error {
			return s.db.QueryRowContext(ctx, fmt.Sprintf(
				"SELECT count(*) FROM %s.%s WHERE writer = '%s' AND seq = %d", s.schema, table, writer, seq)).Scan(&n)
		})
		if err == nil {
			return n > 0
		}

		time.Sleep(500 * time.Millisecond)
	}
}

// pyWriter runs the PyIceberg writer in its container for writeDuration.
func (s *stack) pyWriter(table, writer string, start int64) *writes {
	w := &writes{}

	cmd := exec.CommandContext(s.t.Context(), "docker", "exec", container("pyiceberg"),
		"python", "writer.py", "--table", s.schema+"."+table, "--writer", writer,
		"--duration", fmt.Sprint(writeDuration.Seconds()), "--start", strconv.FormatInt(start, 10))

	var stderr bytes.Buffer

	cmd.Stderr = &stderr

	out, err := cmd.StdoutPipe()
	if err != nil {
		s.t.Fatal(err)
	}

	if err := cmd.Start(); err != nil {
		s.t.Fatal(err)
	}

	sc := bufio.NewScanner(out)
	for sc.Scan() {
		var ev struct {
			Event string `json:"event"`
			Seq   int64  `json:"seq"`
			Error string `json:"error"`
		}

		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			continue
		}

		switch ev.Event {
		case "acked":
			w.acked = append(w.acked, ev.Seq)
		case "unknown", "conflict", "verify_error":
			w.errors = append(w.errors, ev.Event+": "+ev.Error)
		}
	}

	if err := cmd.Wait(); err != nil {
		s.t.Errorf("PyIceberg writer failed: %v\n%s", err, stderr.String())
	}

	return w
}

// check verifies the table: every acknowledged row exactly once, no
// duplicates at all, and a pointer that names an existing metadata file.
func (s *stack) check(table string, results map[string]*writes) {
	s.t.Helper()

	counts := map[string]int{}

	err := s.timed(func(ctx context.Context) error {
		rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
			"SELECT writer, seq, count(*) FROM %s.%s GROUP BY writer, seq", s.schema, table))
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var (
				writer string
				seq    int64
				n      int
			)

			if err := rows.Scan(&writer, &seq, &n); err != nil {
				return err
			}

			counts[fmt.Sprintf("%s/%d", writer, seq)] = n
		}

		return rows.Err()
	})
	if err != nil {
		s.t.Fatalf("reading %s: %v", table, err)
	}

	for key, n := range counts {
		if n > 1 {
			s.t.Errorf("%s: row %s is present %d times", table, key, n)
		}
	}

	total := 0

	for writer, w := range results {
		for _, seq := range w.acked {
			if counts[fmt.Sprintf("%s/%d", writer, seq)] != 1 {
				s.t.Errorf("%s: acknowledged row %s/%d is missing", table, writer, seq)
			}
		}

		total += len(w.acked)
		s.t.Logf("%s: writer %s: %d rows acknowledged, %d retried errors", table, writer, len(w.acked), len(w.errors))
	}

	if total == 0 {
		s.t.Errorf("%s: no row was written", table)
	}

	s.checkPointer(table)
}

// checkPointer reads the table pointer from the KV store and checks that
// its metadata file exists in MinIO.
func (s *stack) checkPointer(table string) {
	s.t.Helper()

	kv, err := s.kv.Get(s.t.Context(), []byte("cat/v1/tbl/"+s.schema+"\x00"+table))
	if err != nil || kv == nil {
		s.t.Fatalf("pointer of %s in the KV store: %v, %v", table, kv, err)
	}

	var ptr struct {
		MetadataLocation string `json:"metadata_location"`
	}

	if err := json.Unmarshal(kv.GetValue(), &ptr); err != nil {
		s.t.Fatal(err)
	}

	files := fileio.NewIceberg(s.t.Context(), map[string]string{
		"s3.endpoint":                 "http://localhost:9000",
		"s3.region":                   "us-east-1",
		"s3.access-key-id":            env("HA_S3_ACCESS_KEY", "minioadmin"),
		"s3.secret-access-key":        env("HA_S3_SECRET_KEY", "minioadmin"),
		"s3.force-virtual-addressing": "false",
	})

	if _, err := files.Read(s.t.Context(), ptr.MetadataLocation); err != nil {
		s.t.Fatalf("metadata file %s of %s: %v", ptr.MetadataLocation, table, err)
	}
}

// raftLeader returns the service name of the kv-node that leads.
func (s *stack) raftLeader() string {
	s.t.Helper()

	for range 50 {
		statuses, _ := s.kv.Status(s.t.Context())
		for _, st := range statuses {
			if id := st.GetLeaderId(); id != 0 {
				return fmt.Sprintf("kv%d", id)
			}
		}

		time.Sleep(200 * time.Millisecond)
	}

	s.t.Fatal("no raft leader")

	return ""
}

// in returns the stack bound to a subtest, so failures are reported on the
// scenario that caused them.
func (s *stack) in(t *testing.T) *stack {
	t.Helper()

	c := *s
	c.t = t

	return &c
}

// waitReady waits until a catalog replica reports ready on its own port.
func (s *stack) waitReady(port string) {
	s.t.Helper()

	deadline := time.Now().Add(time.Minute)

	for time.Now().Before(deadline) {
		url := "http://localhost:" + port + "/readyz"
		req, _ := http.NewRequestWithContext(s.t.Context(), http.MethodGet, url, http.NoBody)
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				return
			}
		}

		time.Sleep(250 * time.Millisecond)
	}

	s.t.Fatalf("catalog on port %s did not become ready", port)
}

// underLoad runs a Trino and a PyIceberg writer while fault runs, then
// checks their tables. Each writer has its own table: two writers on one
// table race with optimistic concurrency, and a slower writer can lose every
// race for minutes. That is correct behaviour, but it would make the test
// about the race instead of the fault. Contention on one table is covered by
// the baseline, the conformance suite and the Trino suite.
func (s *stack) underLoad(table string, fault func()) {
	s.t.Helper()

	trinoTable, pyTable := table+"_trino", table+"_py"
	s.createTable(trinoTable)
	s.createTable(pyTable)

	stop := make(chan struct{})

	var (
		wg        sync.WaitGroup
		trino, py *writes
	)

	wg.Go(func() { trino = s.trinoWriter(trinoTable, "trino", 0, stop) })
	wg.Go(func() {
		py = s.pyWriter(pyTable, "py", 0)

		close(stop)
	})

	time.Sleep(3 * time.Second)
	fault()

	wg.Wait()
	s.check(trinoTable, map[string]*writes{"trino": trino})
	s.check(pyTable, map[string]*writes{"py": py})
}

func testIdempotentRetry(t *testing.T, s *stack) {
	t.Helper()

	s.createTable("idem")
	path := "/v1/main/namespaces/" + s.schema + "/tables/idem"

	load := func() (uuid string, logLen int) {
		var body struct {
			Metadata struct {
				UUID string            `json:"table-uuid"`
				Log  []json.RawMessage `json:"metadata-log"`
			} `json:"metadata"`
		}

		request(t, http.MethodGet, "18181", path, "", "").decode(t, &body)

		return body.Metadata.UUID, len(body.Metadata.Log)
	}

	uuid, before := load()
	commit := `{"requirements":[{"type":"assert-table-uuid","uuid":"` + uuid + `"}],` +
		`"updates":[{"action":"set-properties","updates":{"retried":"once"}}]}`
	key := "017f22e2-79b0-7cc3-98c4-" + fmt.Sprintf("%012x", time.Now().UnixNano()&0xffffffffffff)

	first := request(t, http.MethodPost, "18181", path, commit, key)
	second := request(t, http.MethodPost, "18182", path, commit, key)

	if first.status != http.StatusOK || second.status != http.StatusOK {
		t.Fatalf("statuses %d and %d, want 200 and 200", first.status, second.status)
	}

	if second.header.Get("Idempotent-Replay") != "true" {
		t.Fatal("the retry through catalog-b was executed, not replayed")
	}

	if _, after := load(); after != before+1 {
		t.Fatalf("metadata log grew by %d, want 1: the commit was applied more than once", after-before)
	}
}

// response is an HTTP response, read in full.
type response struct {
	status int
	header http.Header
	body   []byte
}

func (r response) decode(t *testing.T, v any) {
	t.Helper()

	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decoding response (status %d): %v", r.status, err)
	}
}

func request(t *testing.T, method, port, path, body, key string) response {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://localhost:"+port+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}

	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: reading the body: %v", method, path, err)
	}

	return response{status: resp.StatusCode, header: resp.Header, body: raw}
}
