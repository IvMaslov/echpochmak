//go:build ha

package ha_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"echpochmak/internal/fileio"
)

// lbPort is the load balancer on the host.
const lbPort = "8181"

// txDeadline bounds the retries of one transaction. It must outlast an
// idempotency lease, twice the HTTP write timeout (2 x 60 s): a transaction
// whose replica died while holding its key's claim is "in progress" (503)
// until the lease expires and another replica takes the claim over.
const txDeadline = 150 * time.Second

func (s *stack) files() fileio.FileIO {
	return fileio.NewIceberg(s.t.Context(), map[string]string{
		"s3.endpoint":                 "http://localhost:9000",
		"s3.region":                   "us-east-1",
		"s3.access-key-id":            env("HA_S3_ACCESS_KEY", "minioadmin"),
		"s3.secret-access-key":        env("HA_S3_SECRET_KEY", "minioadmin"),
		"s3.force-virtual-addressing": "false",
	})
}

// tryRequest is request without failing the test: a fault may break the
// connection, and the caller retries.
func tryRequest(ctx context.Context, method, port, path, body, key string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://localhost:"+port+path, strings.NewReader(body))
	if err != nil {
		return 0, err
	}

	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	_ = resp.Body.Close()

	return resp.StatusCode, nil
}

// transactionWriter commits two-table transactions until stop closes. Each
// transaction sets the property tx-<seq> on both tables. On any failure it
// retries with the same Idempotency-Key, so a transaction that already
// landed is replayed, not applied again. It returns the acknowledged
// sequence numbers and the failed attempts.
func (s *stack) transactionWriter(tables [2]string, stop <-chan struct{}) *writes {
	w := &writes{}
	runID := time.Now().UnixNano() & 0xffffffff

	for seq := int64(0); ; seq++ {
		select {
		case <-stop:
			return w
		default:
		}

		body := fmt.Sprintf(`{"table-changes": [
			{"identifier": {"namespace": [%[1]q], "name": %[2]q}, "requirements": [],
				"updates": [{"action": "set-properties", "updates": {"tx-%[4]d": "1"}}]},
			{"identifier": {"namespace": [%[1]q], "name": %[3]q}, "requirements": [],
				"updates": [{"action": "set-properties", "updates": {"tx-%[4]d": "1"}}]}]}`,
			s.schema, tables[0], tables[1], seq)
		key := fmt.Sprintf("017f22e2-79b0-7cc3-%04x-%08x%04x", seq&0xffff, runID, seq>>16&0xffff)

		deadline := time.Now().Add(txDeadline)
		attempts := 0

		for {
			status, err := tryRequest(s.t.Context(), http.MethodPost, lbPort, "/v1/main/transactions/commit", body, key)
			if err == nil && status == http.StatusNoContent {
				w.acked = append(w.acked, seq)

				break
			}

			attempts++
			w.errors = append(w.errors, fmt.Sprintf("tx %d: status %d, %v", seq, status, err))

			if time.Now().After(deadline) {
				s.t.Errorf("transaction %d not acknowledged within %s; last attempt: %s",
					seq, txDeadline, w.errors[len(w.errors)-1])

				return w
			}

			if attempts%25 == 0 {
				s.t.Logf("transaction %d still retrying: %s", seq, w.errors[len(w.errors)-1])
			}

			time.Sleep(200 * time.Millisecond)
		}
	}
}

// txProperties returns the tx-* properties of a table.
func (s *stack) txProperties(table string) map[string]bool {
	s.t.Helper()

	var body struct {
		Metadata struct {
			Properties map[string]string `json:"properties"`
		} `json:"metadata"`
	}

	request(s.t, http.MethodGet, lbPort, "/v1/main/namespaces/"+s.schema+"/tables/"+table, "", "").decode(s.t, &body)

	props := map[string]bool{}

	for k := range body.Metadata.Properties {
		if strings.HasPrefix(k, "tx-") {
			props[k] = true
		}
	}

	return props
}

func testTransactionsUnderFaults(t *testing.T, s *stack) {
	t.Helper()

	tables := [2]string{"tx_a", "tx_b"}
	for _, name := range tables {
		s.createTable(name)
	}

	stop := make(chan struct{})

	var (
		wg sync.WaitGroup
		w  *writes
	)

	wg.Go(func() { w = s.transactionWriter(tables, stop) })

	time.Sleep(3 * time.Second)

	leader := s.raftLeader()
	t.Logf("killing catalog-a and %s together", leader)
	docker(t, "kill", container("catalog-a"), container(leader))
	time.Sleep(5 * time.Second)
	docker(t, "start", container("catalog-a"), container(leader))
	s.waitReady("18181")

	time.Sleep(writeDuration / 2)
	close(stop)
	wg.Wait()

	a, b := s.txProperties(tables[0]), s.txProperties(tables[1])

	for key := range a {
		if !b[key] {
			t.Errorf("%s changed %s but not %s: the transaction was not atomic", key, tables[0], tables[1])
		}
	}

	for key := range b {
		if !a[key] {
			t.Errorf("%s changed %s but not %s: the transaction was not atomic", key, tables[1], tables[0])
		}
	}

	for _, seq := range w.acked {
		if key := fmt.Sprintf("tx-%d", seq); !a[key] {
			t.Errorf("acknowledged transaction %d is missing", seq)
		}
	}

	if len(a) != len(w.acked) {
		t.Errorf("%d transactions landed, %d were acknowledged: a retry was applied twice or one was lost",
			len(a), len(w.acked))
	}

	if len(w.acked) < 10 {
		t.Errorf("only %d transactions acknowledged", len(w.acked))
	}

	t.Logf("%d transactions acknowledged, %d failed attempts retried", len(w.acked), len(w.errors))

	for _, name := range tables {
		s.checkPointer(name)
	}
}

func testPurgeAcrossReplicas(t *testing.T, s *stack) {
	t.Helper()

	s.createTable("purged")
	s.exec("INSERT INTO %s.purged VALUES ('trino', 0), ('trino', 1)", s.schema)
	s.exec("INSERT INTO %s.purged VALUES ('trino', 2)", s.schema)

	files := s.purgedFiles("purged")

	// Drop through catalog-a, which dies right away: catalog-b's purge worker
	// must finish the task, after catalog-a's claim expires if it had one.
	path := "/v1/main/namespaces/" + s.schema + "/tables/purged?purgeRequested=true"
	if resp := request(t, http.MethodDelete, "18181", path, "", ""); resp.status != http.StatusNoContent {
		t.Fatalf("drop with purge = %d: %s", resp.status, resp.body)
	}

	docker(t, "kill", container("catalog-a"))

	fs := s.files()
	deadline := time.Now().Add(time.Minute)

	for {
		left := 0

		for _, location := range files {
			if _, err := fs.Read(t.Context(), location); !errors.Is(err, fileio.ErrNotFound) {
				left++
			}
		}

		if left == 0 {
			break
		}

		if time.Now().After(deadline) {
			docker(t, "start", container("catalog-a"))
			t.Fatalf("%d of %d files of the purged table are left", left, len(files))
		}

		time.Sleep(time.Second)
	}

	docker(t, "start", container("catalog-a"))
	s.waitReady("18181")
	t.Logf("catalog-b purged all %d files", len(files))
}

// purgedFiles lists the data files and the current metadata file of a table.
func (s *stack) purgedFiles(table string) []string {
	s.t.Helper()

	ctx, cancel := context.WithTimeout(s.t.Context(), opTimeout)
	defer cancel()

	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(`SELECT file_path FROM %s."%s$files"`, s.schema, table))
	if err != nil {
		s.t.Fatal(err)
	}
	defer rows.Close()

	files := []string{}

	for rows.Next() {
		var path sql.NullString
		if err := rows.Scan(&path); err != nil {
			s.t.Fatal(err)
		}

		files = append(files, path.String)
	}

	if err := rows.Err(); err != nil {
		s.t.Fatal(err)
	}

	var body struct {
		MetadataLocation string `json:"metadata-location"`
	}

	request(s.t, http.MethodGet, lbPort, "/v1/main/namespaces/"+s.schema+"/tables/"+table, "", "").decode(s.t, &body)

	if len(files) == 0 || body.MetadataLocation == "" {
		s.t.Fatalf("no files found for %s", table)
	}

	return append(files, body.MetadataLocation)
}
