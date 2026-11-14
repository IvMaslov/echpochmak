//go:build e2e

// Package e2e runs Trino against the catalog server and MinIO from the
// compose stack. Start the stack first (make compose-up), then run make e2e.
//
// Endpoints can be overridden with E2E_TRINO_URL, E2E_CATALOG_URL and
// E2E_S3_ENDPOINT; MinIO credentials default to the compose defaults.
package e2e_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	// Registers the s3:// scheme with iceberg-go's file IO.
	_ "github.com/apache/iceberg-go/io/gocloud/s3"
	_ "github.com/trinodb/trino-go-client/trino"

	"echpochmak/internal/fileio"
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}

	return fallback
}

type suite struct {
	t       *testing.T
	db      *sql.DB
	schema  string
	catalog string
}

func newSuite(t *testing.T) *suite {
	t.Helper()

	dsn := env("E2E_TRINO_URL", "http://e2e@localhost:8080") + "?catalog=iceberg"

	db, err := sql.Open("trino", dsn)
	if err != nil {
		t.Fatalf("opening Trino: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("Trino is not reachable (run make compose-up): %v", err)
	}

	return &suite{
		t:       t,
		db:      db,
		schema:  fmt.Sprintf("e2e_%d", time.Now().UnixNano()),
		catalog: env("E2E_CATALOG_URL", "http://localhost:8181"),
	}
}

// exec runs a statement and fails the test on error.
func (s *suite) exec(query string, args ...any) {
	s.t.Helper()

	if _, err := s.db.ExecContext(s.t.Context(), fmt.Sprintf(query, args...)); err != nil {
		s.t.Fatalf("%s: %v", fmt.Sprintf(query, args...), err)
	}
}

// rows runs a query and returns every row with its columns formatted as
// strings, joined by "|".
func (s *suite) rows(query string, args ...any) []string {
	s.t.Helper()

	q := fmt.Sprintf(query, args...)

	rs, err := s.db.QueryContext(s.t.Context(), q)
	if err != nil {
		s.t.Fatalf("%s: %v", q, err)
	}
	defer rs.Close()

	cols, err := rs.Columns()
	if err != nil {
		s.t.Fatal(err)
	}

	out := []string{}

	for rs.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rs.Scan(ptrs...); err != nil {
			s.t.Fatal(err)
		}

		parts := make([]string, 0, len(vals))
		for _, v := range vals {
			parts = append(parts, fmt.Sprint(v))
		}

		out = append(out, strings.Join(parts, "|"))
	}

	if err := rs.Err(); err != nil {
		s.t.Fatalf("%s: %v", q, err)
	}

	return out
}

func (s *suite) expectRows(want []string, query string, args ...any) {
	s.t.Helper()

	if got := s.rows(query, args...); !slices.Equal(got, want) {
		s.t.Fatalf("%s\n got: %q\nwant: %q", fmt.Sprintf(query, args...), got, want)
	}
}

// loadTable reads a table from the catalog API directly.
func (s *suite) loadTable(name string) (location string, metadata map[string]any) {
	s.t.Helper()

	url := fmt.Sprintf("%s/v1/main/namespaces/%s/tables/%s", s.catalog, s.schema, name)

	req, err := http.NewRequestWithContext(s.t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		s.t.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		s.t.Fatalf("GET %s = %d", url, resp.StatusCode)
	}

	var body struct {
		MetadataLocation string         `json:"metadata-location"`
		Metadata         map[string]any `json:"metadata"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		s.t.Fatal(err)
	}

	return body.MetadataLocation, body.Metadata
}

func TestTrino(t *testing.T) {
	s := newSuite(t)
	tbl := s.schema + ".t"

	t.Run("schema", func(t *testing.T) {
		s.exec("CREATE SCHEMA %s", s.schema)

		if !slices.Contains(s.rows("SHOW SCHEMAS"), s.schema) {
			t.Fatalf("schema %s not listed", s.schema)
		}
	})

	t.Run("create insert select", func(*testing.T) {
		s.exec("CREATE TABLE %s (id bigint, name varchar) WITH (partitioning = ARRAY['bucket(id, 4)'])", tbl)
		s.exec("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tbl)
		s.expectRows([]string{"1|a", "2|b", "3|c"}, "SELECT id, name FROM %s ORDER BY id", tbl)

		s.exec("INSERT INTO %s VALUES (4, 'd')", tbl)
		s.expectRows([]string{"4"}, "SELECT count(*) FROM %s", tbl)
	})

	t.Run("create table as select", func(*testing.T) {
		s.exec("CREATE TABLE %s.nation AS SELECT * FROM tpch.tiny.nation", s.schema)
		s.expectRows([]string{"25"}, "SELECT count(*) FROM %s.nation", s.schema)
	})

	t.Run("schema evolution and row-level changes", func(*testing.T) {
		s.exec("ALTER TABLE %s ADD COLUMN score double", tbl)
		s.exec("UPDATE %s SET score = 1.5 WHERE id = 2", tbl)
		s.exec("DELETE FROM %s WHERE id = 1", tbl)
		s.exec(`MERGE INTO %s t USING (VALUES (3, 'cc'), (5, 'e')) AS u(id, name) ON t.id = u.id
			WHEN MATCHED THEN UPDATE SET name = u.name
			WHEN NOT MATCHED THEN INSERT (id, name) VALUES (u.id, u.name)`, tbl)
		s.expectRows([]string{"2|b|1.5", "3|cc|<nil>", "4|d|<nil>", "5|e|<nil>"},
			"SELECT id, name, score FROM %s ORDER BY id", tbl)
	})

	t.Run("snapshots and time travel", func(t *testing.T) {
		snapshots := s.rows(`SELECT snapshot_id FROM %s."t$snapshots" ORDER BY committed_at`, s.schema)
		if len(snapshots) < 6 {
			t.Fatalf("snapshots = %v, want at least 6", snapshots)
		}

		// The second snapshot is the first INSERT.
		s.expectRows([]string{"3"}, "SELECT count(*) FROM %s FOR VERSION AS OF %s", tbl, snapshots[1])
	})

	t.Run("rename and show tables", func(*testing.T) {
		s.exec("ALTER TABLE %s.nation RENAME TO %s.nation2", s.schema, s.schema)
		s.expectRows([]string{"nation2", "t"}, "SHOW TABLES FROM %s", s.schema)
	})

	t.Run("concurrent inserts", func(t *testing.T) {
		const writers = 4

		s.exec("CREATE TABLE %s.c (w varchar, i int)", s.schema)

		var wg sync.WaitGroup

		errs := make(chan error, writers)

		for w := range writers {
			wg.Go(func() {
				_, err := s.db.ExecContext(t.Context(), fmt.Sprintf(
					"INSERT INTO %s.c SELECT 'w%d', x FROM UNNEST(sequence(1, 10)) AS u(x)", s.schema, w))
				errs <- err
			})
		}

		wg.Wait()
		close(errs)

		for err := range errs {
			if err != nil {
				t.Errorf("concurrent insert: %v", err)
			}
		}

		s.expectRows([]string{"w0|10", "w1|10", "w2|10", "w3|10"},
			"SELECT w, count(*) FROM %s.c GROUP BY w ORDER BY w", s.schema)
	})

	t.Run("catalog state", func(t *testing.T) {
		location, md := s.loadTable("t")
		if !strings.HasPrefix(location, "s3://warehouse/") {
			t.Fatalf("metadata location %q is not in the MinIO warehouse", location)
		}

		if _, err := s.files().Read(t.Context(), location); err != nil {
			t.Fatalf("current metadata file is not in MinIO: %v", err)
		}

		log, _ := md["metadata-log"].([]any)
		if len(log) < 6 {
			t.Fatalf("metadata log has %d entries, want one per commit", len(log))
		}
	})

	t.Run("views", func(t *testing.T) {
		view := s.schema + ".recent"

		s.exec("CREATE VIEW %s AS SELECT id, name FROM %s WHERE id > 2", view, tbl)
		s.expectRows([]string{"3|cc", "4|d", "5|e"}, "SELECT * FROM %s ORDER BY id", view)

		s.exec("CREATE OR REPLACE VIEW %s AS SELECT id FROM %s WHERE id > 4", view, tbl)
		s.expectRows([]string{"5"}, "SELECT * FROM %s", view)

		// The view is in the catalog, with Trino's SQL.
		status, body := s.catalogDo(http.MethodGet, "/v1/main/namespaces/"+s.schema+"/views/recent", "")
		if status != http.StatusOK || !strings.Contains(body, `"dialect":"trino"`) {
			t.Fatalf("GET view = %d: %s", status, body)
		}

		// A view cannot take a table's name.
		if _, err := s.db.ExecContext(t.Context(), fmt.Sprintf("CREATE VIEW %s AS SELECT 1 AS x", tbl)); err == nil {
			t.Fatal("created a view with the name of a table")
		}

		s.exec("ALTER VIEW %s RENAME TO %s.recent2", view, s.schema)
		s.expectRows([]string{"5"}, "SELECT * FROM %s.recent2", s.schema)
		s.exec("DROP VIEW %s.recent2", s.schema)

		status, _ = s.catalogDo(http.MethodGet, "/v1/main/namespaces/"+s.schema+"/views/recent2", "")
		if status != http.StatusNotFound {
			t.Fatalf("GET dropped view = %d, want 404", status)
		}
	})

	t.Run("multi-table transaction", func(t *testing.T) {
		body := fmt.Sprintf(`{"table-changes": [
			{"identifier": {"namespace": [%[1]q], "name": "t"}, "requirements": [],
				"updates": [{"action": "set-properties", "updates": {"tx": "1"}}]},
			{"identifier": {"namespace": [%[1]q], "name": "nation2"}, "requirements": [],
				"updates": [{"action": "set-properties", "updates": {"tx": "1"}}]}]}`, s.schema)

		status, resp := s.catalogDo(http.MethodPost, "/v1/main/transactions/commit", body)
		if status != http.StatusNoContent {
			t.Fatalf("transaction = %d: %s", status, resp)
		}

		for _, name := range []string{"t", "nation2"} {
			s.expectRows([]string{"1"}, `SELECT value FROM %s."%s$properties" WHERE key = 'tx'`, s.schema, name)
		}
	})

	t.Run("unregister and register", func(t *testing.T) {
		status, body := s.catalogDo(http.MethodPost, "/v1/main/namespaces/"+s.schema+"/tables/nation2/unregister", "")
		if status != http.StatusOK {
			t.Fatalf("unregister = %d: %s", status, body)
		}

		var unregistered struct {
			MetadataLocation string `json:"metadata-location"`
		}
		if err := json.Unmarshal([]byte(body), &unregistered); err != nil {
			t.Fatal(err)
		}

		if slices.Contains(s.rows("SHOW TABLES FROM %s", s.schema), "nation2") {
			t.Fatal("an unregistered table is still listed")
		}

		register := fmt.Sprintf(`{"name": "nation3", "metadata-location": %q}`, unregistered.MetadataLocation)
		status, body = s.catalogDo(http.MethodPost, "/v1/main/namespaces/"+s.schema+"/register", register)
		if status != http.StatusOK {
			t.Fatalf("register = %d: %s", status, body)
		}

		s.expectRows([]string{"25"}, "SELECT count(*) FROM %s.nation3", s.schema)
	})

	t.Run("drop purges files", func(*testing.T) {
		files := s.rows(`SELECT file_path FROM %s."t$files"`, s.schema)
		location, _ := s.loadTable("t")
		files = append(files, location)

		// Trino's DROP TABLE asks the catalog to purge.
		s.exec("DROP TABLE %s", tbl)

		s.waitGone(files, time.Minute)
	})

	t.Run("drop", func(t *testing.T) {
		for _, name := range []string{"nation3", "c"} {
			s.exec("DROP TABLE %s.%s", s.schema, name)
		}

		s.exec("DROP SCHEMA %s", s.schema)

		if slices.Contains(s.rows("SHOW SCHEMAS"), s.schema) {
			t.Fatalf("schema %s still listed after drop", s.schema)
		}
	})
}

// catalogDo sends a request to the catalog API and returns the status and
// the body.
func (s *suite) catalogDo(method, path, body string) (int, string) {
	s.t.Helper()

	req, err := http.NewRequestWithContext(s.t.Context(), method, s.catalog+path, strings.NewReader(body))
	if err != nil {
		s.t.Fatal(err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		s.t.Fatal(err)
	}

	return resp.StatusCode, string(data)
}

func (s *suite) files() fileio.FileIO {
	return fileio.NewIceberg(s.t.Context(), map[string]string{
		"s3.endpoint":                 env("E2E_S3_ENDPOINT", "http://localhost:9000"),
		"s3.region":                   "us-east-1",
		"s3.access-key-id":            env("E2E_S3_ACCESS_KEY", "minioadmin"),
		"s3.secret-access-key":        env("E2E_S3_SECRET_KEY", "minioadmin"),
		"s3.force-virtual-addressing": "false",
	})
}

// waitGone waits until none of the files exists in MinIO.
func (s *suite) waitGone(locations []string, timeout time.Duration) {
	s.t.Helper()

	files := s.files()
	deadline := time.Now().Add(timeout)

	for {
		left := []string{}

		for _, location := range locations {
			if _, err := files.Read(s.t.Context(), location); !errors.Is(err, fileio.ErrNotFound) {
				left = append(left, location)
			}
		}

		if len(left) == 0 {
			return
		}

		if time.Now().After(deadline) {
			s.t.Fatalf("%d of %d files still exist after %s, for example %q",
				len(left), len(locations), timeout, left[0])
		}

		time.Sleep(time.Second)
	}
}
