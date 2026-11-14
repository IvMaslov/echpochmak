package api_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"

	"echpochmak/internal/api"
	"echpochmak/internal/catalog"
	"echpochmak/internal/fileio"
	"echpochmak/internal/store/memstore"
)

const createViewBody = `{
	"name": "v",
	"schema": {"type": "struct", "schema-id": 0, "fields": [
		{"id": 1, "name": "x", "required": false, "type": "long"}]},
	"view-version": {"version-id": 1, "schema-id": 0, "timestamp-ms": 1700000000000,
		"summary": {"engine-name": "test"}, "default-namespace": ["db"],
		"representations": [{"type": "sql", "sql": "SELECT 1 AS x", "dialect": "trino"}]},
	"properties": {"comment": "a view"}
}`

type viewResult struct {
	MetadataLocation string `json:"metadata-location"`
	Metadata         struct {
		ViewUUID         string            `json:"view-uuid"`
		CurrentVersionID int64             `json:"current-version-id"`
		Properties       map[string]string `json:"properties"`
		Versions         []json.RawMessage `json:"versions"`
	} `json:"metadata"`
}

func (c *client) errorMessage(status int, method, target, body string) string {
	c.t.Helper()

	rec := c.do(method, target, body)
	if rec.Code != status {
		c.t.Fatalf("%s %s = %d, want %d; body: %s", method, target, rec.Code, status, rec.Body.String())
	}

	return decodeError(c.t, rec).Message
}

func TestViewLifecycle(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	var created viewResult
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/views", createViewBody, &created)

	if created.Metadata.CurrentVersionID != 1 || created.Metadata.Properties["comment"] != "a view" {
		t.Fatalf("created view %+v", created)
	}

	var loaded viewResult
	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db/views/v", "", &loaded)
	c.expect(http.StatusNoContent, http.MethodHead, "/v1/main/namespaces/db/views/v", "", nil)

	if loaded.MetadataLocation != created.MetadataLocation {
		t.Fatalf("loaded %q, created %q", loaded.MetadataLocation, created.MetadataLocation)
	}

	var list struct {
		Identifiers []struct {
			Name string `json:"name"`
		} `json:"identifiers"`
	}
	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db/views", "", &list)

	if len(list.Identifiers) != 1 || list.Identifiers[0].Name != "v" {
		t.Fatalf("list views = %+v", list)
	}

	replace := `{"requirements": [{"type": "assert-view-uuid", "uuid": "` + created.Metadata.ViewUUID + `"}],
		"updates": [
			{"action": "add-view-version", "view-version": {"version-id": 2, "schema-id": 0,
				"timestamp-ms": 1700000001000, "summary": {}, "default-namespace": ["db"],
				"representations": [{"type": "sql", "sql": "SELECT 2 AS x", "dialect": "trino"}]}},
			{"action": "set-current-view-version", "view-version-id": -1}]}`

	var replaced viewResult
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/views/v", replace, &replaced)

	if replaced.Metadata.CurrentVersionID != 2 || len(replaced.Metadata.Versions) != 2 {
		t.Fatalf("replaced view %+v", replaced.Metadata)
	}

	c.expect(http.StatusNoContent, http.MethodPost, "/v1/main/views/rename",
		`{"source":{"namespace":["db"],"name":"v"},"destination":{"namespace":["db"],"name":"w"}}`, nil)
	c.expect(http.StatusNoContent, http.MethodDelete, "/v1/main/namespaces/db/views/w", "", nil)
	c.expectError(http.StatusNotFound, "NoSuchViewException", http.MethodGet, "/v1/main/namespaces/db/views/w", "")
	c.expectError(http.StatusNotFound, "", http.MethodHead, "/v1/main/namespaces/db/views/w", "")
}

func TestViewErrors(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/views", createViewBody, nil)

	// The messages are the Java reference implementation's.
	const views = "/v1/main/namespaces/db/views"

	asTable := strings.Replace(createViewBody, `"name": "v"`, `"name": "t"`, 1)
	if msg := c.errorMessage(http.StatusConflict, http.MethodPost, views, asTable); msg !=
		"Table with same name already exists: db.t" {
		t.Errorf("view over a table: %q", msg)
	}

	if msg := c.errorMessage(http.StatusConflict, http.MethodPost, views, createViewBody); msg !=
		"View already exists: db.v" {
		t.Errorf("view twice: %q", msg)
	}

	asView := strings.Replace(createTableBody, `"name": "t"`, `"name": "v"`, 1)
	if msg := c.errorMessage(http.StatusConflict, http.MethodPost, "/v1/main/namespaces/db/tables", asView); msg !=
		"View with same name already exists: db.v" {
		t.Errorf("table over a view: %q", msg)
	}

	stale := `{"requirements":[{"type":"assert-view-uuid","uuid":"00000000-0000-0000-0000-00000000dead"}],"updates":[]}`

	msg := c.errorMessage(http.StatusConflict, http.MethodPost, views+"/v", stale)
	if !strings.HasPrefix(msg, "Requirement failed: view UUID does not match") {
		t.Errorf("stale view uuid: %q", msg)
	}

	twoDialects := strings.Replace(createViewBody, `"representations": [`,
		`"representations": [{"type": "sql", "sql": "SELECT 2", "dialect": "trino"}, `, 1)
	twoDialects = strings.Replace(twoDialects, `"name": "v"`, `"name": "v2"`, 1)
	c.expectError(http.StatusBadRequest, "IllegalArgumentException", http.MethodPost, views, twoDialects)

	c.expectError(http.StatusNotFound, "NoSuchViewException", http.MethodPost, "/v1/main/views/rename",
		`{"source":{"namespace":["db"],"name":"nope"},"destination":{"namespace":["db"],"name":"x"}}`)

	msg = c.errorMessage(http.StatusConflict, http.MethodPost, "/v1/main/views/rename",
		`{"source":{"namespace":["db"],"name":"v"},"destination":{"namespace":["db"],"name":"t"}}`)
	if msg != "Cannot rename db.v to db.t. Table already exists" {
		t.Errorf("rename onto a table: %q", msg)
	}
}

func TestTransactionCommit(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables",
		strings.Replace(createTableBody, `"name": "t"`, `"name": "u"`, 1), nil)

	body := `{"table-changes": [
		{"identifier": {"namespace": ["db"], "name": "t"}, "requirements": [],
			"updates": [{"action": "set-properties", "updates": {"k": "t"}}]},
		{"identifier": {"namespace": ["db"], "name": "u"}, "requirements": [],
			"updates": [{"action": "set-properties", "updates": {"k": "u"}}]}]}`
	c.expect(http.StatusNoContent, http.MethodPost, "/v1/main/transactions/commit", body, nil)

	for _, name := range []string{"t", "u"} {
		var tbl tableResult
		c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db/tables/"+name, "", &tbl)

		if tbl.Metadata.Properties["k"] != name {
			t.Fatalf("table %s properties %v", name, tbl.Metadata.Properties)
		}
	}

	failing := `{"table-changes": [
		{"identifier": {"namespace": ["db"], "name": "t"}, "requirements": [],
			"updates": [{"action": "set-properties", "updates": {"k": "again"}}]},
		{"identifier": {"namespace": ["db"], "name": "u"},
			"requirements": [{"type": "assert-current-schema-id", "current-schema-id": 9}], "updates": []}]}`
	const commit = "/v1/main/transactions/commit"

	c.expectError(http.StatusConflict, "CommitFailedException", http.MethodPost, commit, failing)
	c.expectError(http.StatusBadRequest, "BadRequestException", http.MethodPost, commit,
		`{"table-changes": [{"requirements": [], "updates": []}]}`)
	c.expectError(http.StatusNotFound, "NoSuchTableException", http.MethodPost, commit,
		`{"table-changes": [{"identifier": {"namespace": ["db"], "name": "nope"}, "requirements": [], "updates": []}]}`)
}

func TestRegisterAndUnregister(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)

	var created tableResult
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables", createTableBody, &created)

	var unregistered tableResult
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables/t/unregister", "", &unregistered)

	sameFile := unregistered.MetadataLocation == created.MetadataLocation
	if !sameFile || unregistered.Metadata.TableUUID != created.Metadata.TableUUID {
		t.Fatalf("unregister returned %q, want %q", unregistered.MetadataLocation, created.MetadataLocation)
	}

	c.expectError(http.StatusNotFound, "NoSuchTableException", http.MethodGet, "/v1/main/namespaces/db/tables/t", "")

	register := `{"name": "back", "metadata-location": "` + created.MetadataLocation + `"}`

	var registered tableResult
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/register", register, &registered)

	if registered.Metadata.TableUUID != created.Metadata.TableUUID {
		t.Fatalf("registered table %q, want %q", registered.Metadata.TableUUID, created.Metadata.TableUUID)
	}

	const target = "/v1/main/namespaces/db/register"

	c.expectError(http.StatusConflict, "AlreadyExistsException", http.MethodPost, target, register)
	c.expect(http.StatusOK, http.MethodPost, target,
		`{"name": "back", "overwrite": true, "metadata-location": "`+created.MetadataLocation+`"}`, nil)
	c.expectError(http.StatusBadRequest, "IllegalArgumentException", http.MethodPost, target,
		`{"name": "x", "metadata-location": "mem://nowhere/m.json"}`)
}

func TestDropWithPurgeIsQueued(t *testing.T) {
	t.Parallel()

	c := newClient(t)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces", `{"namespace":["db"]}`, nil)
	c.expect(http.StatusOK, http.MethodPost, "/v1/main/namespaces/db/tables",
		strings.Replace(createTableBody, `"owner": "test"`, `"gc.enabled": "false"`, 1), nil)

	if msg := c.errorMessage(http.StatusBadRequest, http.MethodDelete,
		"/v1/main/namespaces/db/tables/t?purgeRequested=true", ""); !strings.Contains(msg, "gc.enabled=false") {
		t.Fatalf("purge of a table with gc disabled: %q", msg)
	}

	c.expect(http.StatusOK, http.MethodGet, "/v1/main/namespaces/db/tables/t", "", nil)
}

func TestConfigAdvertisesEveryImplementedEndpoint(t *testing.T) {
	t.Parallel()

	c := newClient(t)

	var cfg struct {
		Endpoints []string `json:"endpoints"`
	}
	c.expect(http.StatusOK, http.MethodGet, "/v1/config", "", &cfg)

	for _, want := range []string{
		"POST /v1/{prefix}/transactions/commit",
		"POST /v1/{prefix}/namespaces/{namespace}/register",
		"POST /v1/{prefix}/namespaces/{namespace}/register-view",
		"POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister",
		"GET /v1/{prefix}/namespaces/{namespace}/views/{view}",
		"POST /v1/{prefix}/views/rename",
	} {
		if !slices.Contains(cfg.Endpoints, want) {
			t.Errorf("/v1/config does not advertise %q", want)
		}
	}
}

func TestDocs(t *testing.T) {
	t.Parallel()

	log := slog.New(slog.DiscardHandler)
	svc := catalog.New(catalog.Options{
		Store:  memstore.New(),
		Files:  fileio.NewIceberg(context.Background(), nil),
		Logger: log,
	})
	c := &client{t: t, h: api.NewHandler(api.Options{
		Logger: log,
		Ready:  svc,
		Server: api.NewServer(svc, "warehouse-a"),
		Docs:   true,
	})}

	page := c.expect(http.StatusOK, http.MethodGet, "/docs/", "", nil)
	if !strings.Contains(page.Body.String(), "SwaggerUIBundle") {
		t.Fatal("/docs/ is not the Swagger UI page")
	}

	c.expect(http.StatusOK, http.MethodGet, "/docs/swagger-ui-bundle.js", "", nil)
	c.expect(http.StatusOK, http.MethodGet, "/docs/swagger-ui.css", "", nil)

	if rec := c.do(http.MethodGet, "/docs", ""); rec.Code != http.StatusMovedPermanently {
		t.Fatalf("/docs = %d, want a redirect to /docs/", rec.Code)
	}

	var spec struct {
		Security   any                       `json:"security"`
		Paths      map[string]map[string]any `json:"paths"`
		Components struct {
			SecuritySchemes any `json:"securitySchemes"`
			Parameters      struct {
				Prefix struct {
					Schema struct {
						Default string `json:"default"`
					} `json:"schema"`
				} `json:"prefix"`
			} `json:"parameters"`
		} `json:"components"`
	}
	c.expect(http.StatusOK, http.MethodGet, "/openapi.json", "", &spec)

	served := []string{}

	for path, item := range spec.Paths {
		for method := range item {
			if method != "parameters" {
				served = append(served, strings.ToUpper(method)+" "+path)
			}
		}
	}

	want := append(api.Endpoints(), "GET /v1/config")
	slices.Sort(served)
	slices.Sort(want)

	switch {
	case !slices.Equal(served, want):
		t.Errorf("served operations %q\nwant %q", served, want)
	case spec.Security != nil || spec.Components.SecuritySchemes != nil:
		t.Error("the served spec still declares authentication")
	case spec.Components.Parameters.Prefix.Schema.Default != "warehouse-a":
		t.Errorf("prefix default %q, want the catalog name", spec.Components.Parameters.Prefix.Schema.Default)
	}
}
