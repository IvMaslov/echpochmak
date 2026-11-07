package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"echpochmak/internal/api"
	"echpochmak/internal/store/memstore"
)

// client drives the handler like an Iceberg REST client would.
type client struct {
	t *testing.T
	h http.Handler
}

func newClient(t *testing.T) *client {
	t.Helper()

	h, _ := newTestHandler(t, memstore.New())

	return &client{t: t, h: h}
}

func (c *client) do(method, target, body string, headers ...string) *httptest.ResponseRecorder {
	c.t.Helper()

	req := httptest.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}

	rec := httptest.NewRecorder()
	c.h.ServeHTTP(rec, req)

	return rec
}

// expect performs a request and fails unless it returns status. For JSON
// responses it decodes the body into out, if out is not nil.
func (c *client) expect(status int, method, target, body string, out any) *httptest.ResponseRecorder {
	c.t.Helper()

	rec := c.do(method, target, body)
	if rec.Code != status {
		c.t.Fatalf("%s %s = %d, want %d; body: %s", method, target, rec.Code, status, rec.Body.String())
	}

	if out != nil {
		if err := json.Unmarshal(rec.Body.Bytes(), out); err != nil {
			c.t.Fatalf("%s %s: decoding body: %v\n%s", method, target, err, rec.Body.String())
		}
	}

	return rec
}

// expectError performs a request and checks the Iceberg error it returns.
func (c *client) expectError(status int, errType, method, target, body string) {
	c.t.Helper()

	rec := c.do(method, target, body)
	if rec.Code != status {
		c.t.Fatalf("%s %s = %d, want %d; body: %s", method, target, rec.Code, status, rec.Body.String())
	}

	if method == http.MethodHead {
		if rec.Body.Len() != 0 {
			c.t.Fatalf("HEAD %s returned a body", target)
		}

		return
	}

	if e := decodeError(c.t, rec); e.Type != errType || e.Code != status {
		c.t.Fatalf("%s %s error = %+v, want %d %s", method, target, e, status, errType)
	}
}

type tableResult struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		FormatVersion int               `json:"format-version"`
		TableUUID     string            `json:"table-uuid"`
		Location      string            `json:"location"`
		Properties    map[string]string `json:"properties"`
		Schemas       []json.RawMessage `json:"schemas"`
	} `json:"metadata"`
	Config map[string]string `json:"config"`
}

const createTableBody = `{
	"name": "t",
	"schema": {"type": "struct", "fields": [
		{"id": 1, "name": "id", "required": true, "type": "long"},
		{"id": 2, "name": "data", "required": false, "type": "string"}]},
	"partition-spec": {"fields": [{"name": "id_bucket", "transform": "bucket[8]", "source-id": 1}]},
	"write-order": {"order-id": 1, "fields": [
		{"transform": "identity", "source-id": 2, "direction": "asc", "null-order": "nulls-first"}]},
	"properties": {"owner": "test"}
}`

func TestConfig(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	var cfg struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
		Endpoints []string          `json:"endpoints"`
	}
	c.expect(http.StatusOK, http.MethodGet, "/v1/config?warehouse=anything", "", &cfg)

	if cfg.Overrides["prefix"] != "main" || cfg.Defaults == nil {
		t.Fatalf("config = %+v", cfg)
	}

	if !slices.Equal(cfg.Endpoints, api.Endpoints()) {
		t.Fatalf("endpoints = %v", cfg.Endpoints)
	}
}

// Every advertised endpoint must be implemented, or clients would call
// endpoints that answer 501.
func TestAdvertisedEndpointsAreImplemented(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	for _, ep := range api.Endpoints() {
		method, path, _ := strings.Cut(ep, " ")
		target := strings.NewReplacer("{prefix}", "main", "{namespace}", "db", "{table}", "t").Replace(path)

		if rec := c.do(method, target, "{}"); rec.Code == http.StatusNotImplemented {
			t.Errorf("advertised endpoint %q answers 501", ep)
		}
	}
}

func TestNamespaceLifecycle(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	var created struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces",
		`{"namespace":["db"],"properties":{"owner":"alice"}}`, &created)

	if !slices.Equal(created.Namespace, []string{"db"}) || created.Properties["owner"] != "alice" {
		t.Fatalf("create response = %+v", created)
	}

	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db","sub"]}`, nil)
	c.expectError(http.StatusConflict, "AlreadyExistsException", http.MethodPost, "/v1/main/namespaces",
		`{"namespace":["db"]}`)
	c.expectError(http.StatusNotFound, "NoSuchNamespaceException", http.MethodPost, "/v1/main/namespaces",
		`{"namespace":["missing","child"]}`)

	c.expect(http.StatusNoContent, http.MethodHead, "/v1/main/namespaces/db%1Fsub", "", nil)
	c.expectError(http.StatusNotFound, "", http.MethodHead, "/v1/main/namespaces/nope", "")

	var loaded struct {
		Namespace  []string          `json:"namespace"`
		Properties map[string]string `json:"properties"`
	}
	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db", "", &loaded)

	if loaded.Properties["owner"] != "alice" {
		t.Fatalf("load response = %+v", loaded)
	}

	var upd struct {
		Updated []string `json:"updated"`
		Removed []string `json:"removed"`
		Missing []string `json:"missing"`
	}
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/properties",
		`{"updates":{"a":"1"},"removals":["owner","nope"]}`, &upd)

	if !slices.Equal(upd.Updated, []string{"a"}) || !slices.Equal(upd.Removed, []string{"owner"}) ||
		!slices.Equal(upd.Missing, []string{"nope"}) {
		t.Fatalf("properties response = %+v", upd)
	}

	c.expectError(http.StatusUnprocessableEntity, "UnprocessableEntityException", http.MethodPost,
		"/v1/main/namespaces/db/properties", `{"updates":{"a":"2"},"removals":["a"]}`)

	c.expectError(http.StatusConflict, "NamespaceNotEmptyException", http.MethodDelete, "/v1/main/namespaces/db", "")
	c.expect(http.StatusNoContent, http.MethodDelete, "/v1/main/namespaces/db%1Fsub", "", nil)
	c.expect(http.StatusNoContent, http.MethodDelete, "/v1/main/namespaces/db", "", nil)
	c.expectError(http.StatusNotFound, "NoSuchNamespaceException", http.MethodGet, "/v1/main/namespaces/db", "")
}

func TestListNamespacesPagination(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	for _, name := range []string{"c", "a", "b"} {
		c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", fmt.Sprintf(`{"namespace":[%q]}`, name), nil)
	}

	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["a","x"]}`, nil)

	type listResponse struct {
		Namespaces    [][]string `json:"namespaces"`
		NextPageToken *string    `json:"next-page-token"`
	}

	// No pageToken: the client does not paginate and gets everything.
	var all listResponse
	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces?pageSize=1", "", &all)

	if len(all.Namespaces) != 3 || all.NextPageToken != nil {
		t.Fatalf("unpaginated list = %+v", all)
	}

	// Empty pageToken: pages of the requested size.
	var got []string

	token := ""

	for range 10 {
		var page listResponse
		c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces?pageSize=2&pageToken="+token, "", &page)

		for _, ns := range page.Namespaces {
			got = append(got, ns[0])
		}

		if page.NextPageToken == nil {
			break
		}

		token = *page.NextPageToken
	}

	if !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("paginated list = %v", got)
	}

	var children listResponse
	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces?parent=a", "", &children)

	if len(children.Namespaces) != 1 || !slices.Equal(children.Namespaces[0], []string{"a", "x"}) {
		t.Fatalf("children of a = %+v", children)
	}

	c.expectError(http.StatusNotFound, "NoSuchNamespaceException", http.MethodGet, "/v1/main/namespaces?parent=zzz", "")

	// Empty listings encode as [], not null.
	rec := c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces?parent=b", "", nil)
	if !strings.Contains(rec.Body.String(), `"namespaces":[]`) {
		t.Fatalf("empty list body = %s", rec.Body.String())
	}
}

func TestTableLifecycle(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	var created tableResult

	rec := c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, &created)
	etag := rec.Header().Get("ETag")

	switch {
	case created.MetadataLocation == "" || etag == "":
		t.Fatalf("create: location %q, etag %q", created.MetadataLocation, etag)
	case created.Metadata.Properties["owner"] != "test" || len(created.Metadata.Schemas) != 1:
		t.Fatalf("create metadata = %+v", created.Metadata)
	case !strings.HasPrefix(created.Metadata.Location, "mem://api-test/"):
		t.Fatalf("location = %q", created.Metadata.Location)
	case created.Config == nil:
		t.Fatal("config is missing")
	}

	c.expectError(http.StatusConflict, "AlreadyExistsException", http.MethodPost, "/v1/main/namespaces/db/tables",
		createTableBody)
	c.expectError(http.StatusNotFound, "NoSuchNamespaceException", http.MethodPost,
		"/v1/main/namespaces/nope/tables", createTableBody)

	c.expect(http.StatusNoContent, http.MethodHead, "/v1/main/namespaces/db/tables/t", "", nil)

	var loaded tableResult

	rec = c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db/tables/t", "", &loaded)
	if loaded.MetadataLocation != created.MetadataLocation || rec.Header().Get("ETag") != etag {
		t.Fatalf("load = %q / %q, want the created table", loaded.MetadataLocation, rec.Header().Get("ETag"))
	}

	cond := c.do(http.MethodGet, "/v1/main/namespaces/db/tables/t", "", "If-None-Match", etag)
	if cond.Code != http.StatusNotModified {
		t.Fatalf("conditional load = %d, want 304", cond.Code)
	}

	var ids struct {
		Identifiers []struct {
			Namespace []string `json:"namespace"`
			Name      string   `json:"name"`
		} `json:"identifiers"`
	}
	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db/tables", "", &ids)

	if len(ids.Identifiers) != 1 || ids.Identifiers[0].Name != "t" {
		t.Fatalf("list tables = %+v", ids)
	}

	commit := fmt.Sprintf(`{
		"identifier": {"namespace": ["db"], "name": "t"},
		"requirements": [{"type": "assert-table-uuid", "uuid": %q}],
		"updates": [{"action": "set-properties", "updates": {"k": "v"}}]
	}`, created.Metadata.TableUUID)

	var committed tableResult

	rec = c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables/t", commit, &committed)
	if committed.Metadata.Properties["k"] != "v" || committed.MetadataLocation == created.MetadataLocation {
		t.Fatalf("commit = %+v", committed)
	}

	if rec.Header().Get("ETag") == etag {
		t.Fatal("ETag did not change with the commit")
	}

	stale := c.do(http.MethodGet, "/v1/main/namespaces/db/tables/t", "", "If-None-Match", etag)
	if stale.Code != http.StatusOK {
		t.Fatalf("conditional load with a stale ETag = %d, want 200", stale.Code)
	}

	c.expect(http.StatusNoContent, http.MethodPost, "/v1/main/tables/rename",
		`{"source":{"namespace":["db"],"name":"t"},"destination":{"namespace":["db"],"name":"t2"}}`, nil)
	c.expect(http.StatusNoContent, http.MethodPost, "/v1/main/namespaces/db/tables/t2/metrics",
		`{"report-type":"scan-report","table-name":"db.t2"}`, nil)
	c.expect(http.StatusNoContent, http.MethodDelete, "/v1/main/namespaces/db/tables/t2?purgeRequested=true", "", nil)
	c.expectError(http.StatusNotFound, "NoSuchTableException", http.MethodGet, "/v1/main/namespaces/db/tables/t2", "")
}

func TestStagedCreateThenCommit(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	var staged tableResult

	body := strings.Replace(createTableBody, `"name": "t",`, `"name": "t", "stage-create": true,`, 1)

	rec := c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", body, &staged)
	if staged.MetadataLocation != "" || rec.Header().Get("ETag") != "" {
		t.Fatalf("staged create returned location %q", staged.MetadataLocation)
	}

	c.expectError(http.StatusNotFound, "", http.MethodHead, "/v1/main/namespaces/db/tables/t", "")

	commit := fmt.Sprintf(`{
		"requirements": [{"type": "assert-create"}],
		"updates": [
			{"action": "assign-uuid", "uuid": %q},
			{"action": "upgrade-format-version", "format-version": 2},
			{"action": "add-schema", "schema": {"type": "struct", "schema-id": 0, "fields": [
				{"id": 1, "name": "id", "required": true, "type": "long"}]}},
			{"action": "set-current-schema", "schema-id": -1},
			{"action": "add-spec", "spec": {"spec-id": 0, "fields": []}},
			{"action": "set-default-spec", "spec-id": -1},
			{"action": "add-sort-order", "sort-order": {"order-id": 0, "fields": []}},
			{"action": "set-default-sort-order", "sort-order-id": -1},
			{"action": "set-location", "location": %q},
			{"action": "set-properties", "updates": {"created-by": "staged"}}
		]
	}`, staged.Metadata.TableUUID, staged.Metadata.Location)

	var created tableResult
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables/t", commit, &created)

	sameUUID := created.Metadata.TableUUID == staged.Metadata.TableUUID
	if !sameUUID || created.Metadata.Properties["created-by"] != "staged" {
		t.Fatalf("created = %+v", created.Metadata)
	}

	c.expectError(http.StatusConflict, "CommitFailedException", http.MethodPost,
		"/v1/main/namespaces/db/tables/t", commit)
}

func TestCommitErrors(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, nil)

	tests := []struct {
		name, target, body, errType string
		status                      int
	}{
		{
			name:    "failed requirement",
			target:  "/v1/main/namespaces/db/tables/t",
			body:    `{"requirements":[{"type":"assert-current-schema-id","current-schema-id":9}],"updates":[]}`,
			status:  http.StatusConflict,
			errType: "CommitFailedException",
		},
		{
			name:    "invalid update",
			target:  "/v1/main/namespaces/db/tables/t",
			body:    `{"requirements":[],"updates":[{"action":"set-current-schema","schema-id":9}]}`,
			status:  http.StatusBadRequest,
			errType: "BadRequestException",
		},
		{
			name:    "unknown update action",
			target:  "/v1/main/namespaces/db/tables/t",
			body:    `{"requirements":[],"updates":[{"action":"teleport"}]}`,
			status:  http.StatusBadRequest,
			errType: "BadRequestException",
		},
		{
			name:    "identifier mismatch",
			target:  "/v1/main/namespaces/db/tables/t",
			body:    `{"identifier":{"namespace":["db"],"name":"other"},"requirements":[],"updates":[]}`,
			status:  http.StatusBadRequest,
			errType: "BadRequestException",
		},
		{
			name:    "missing table",
			target:  "/v1/main/namespaces/db/tables/nope",
			body:    `{"requirements":[],"updates":[{"action":"set-properties","updates":{"k":"v"}}]}`,
			status:  http.StatusNotFound,
			errType: "NoSuchTableException",
		},
		{
			name:    "wrong prefix",
			target:  "/v1/other/namespaces/db/tables/t",
			body:    `{"requirements":[],"updates":[]}`,
			status:  http.StatusNotFound,
			errType: "NotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c.expectError(tt.status, tt.errType, http.MethodPost, tt.target, tt.body)
		})
	}
}

func TestCreateTableErrors(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	for name, body := range map[string]string{
		"no schema":  `{"name":"t"}`,
		"bad schema": `{"name":"t","schema":{"type":"struct","fields":[{"id":1,"name":"x","type":"nope"}]}}`,
		"bad partitioning": `{"name":"t","schema":{"type":"struct","fields":[]},
			"partition-spec":{"fields":[{"name":"p","transform":"identity","source-id":7}]}}`,
		"empty name": `{"name":"","schema":{"type":"struct","fields":[]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c.expectError(http.StatusBadRequest, "BadRequestException", http.MethodPost,
				"/v1/main/namespaces/db/tables", body)
		})
	}
}

func TestBodyTooLarge(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	big := `{"namespace":["db"],"properties":{"x":"` + strings.Repeat("a", 2<<20) + `"}}`

	c.expectError(http.StatusRequestEntityTooLarge, "BadRequestException", http.MethodPost, "/v1/main/namespaces", big)
}

func TestRenameErrors(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables",
		strings.Replace(createTableBody, `"name": "t"`, `"name": "u"`, 1), nil)

	rename := func(from, to string) string {
		return fmt.Sprintf(
			`{"source":{"namespace":["db"],"name":%q},"destination":{"namespace":["db"],"name":%q}}`,
			from,
			to,
		)
	}

	c.expectError(http.StatusNotFound, "NoSuchTableException", http.MethodPost,
		"/v1/main/tables/rename", rename("nope", "x"))
	c.expectError(http.StatusConflict, "AlreadyExistsException", http.MethodPost,
		"/v1/main/tables/rename", rename("t", "u"))
	c.expectError(http.StatusNotFound, "NoSuchNamespaceException", http.MethodPost, "/v1/main/tables/rename",
		`{"source":{"namespace":["db"],"name":"t"},"destination":{"namespace":["nope"],"name":"t"}}`)
}
