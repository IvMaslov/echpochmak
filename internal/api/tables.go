package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// createTableBody is the create-table request, decoded from the raw body so
// that schema, partition spec and sort order reach iceberg-go intact.
type createTableBody struct {
	Name          string            `json:"name"`
	Location      string            `json:"location"`
	Schema        json.RawMessage   `json:"schema"`
	PartitionSpec json.RawMessage   `json:"partition-spec"`
	WriteOrder    json.RawMessage   `json:"write-order"`
	StageCreate   bool              `json:"stage-create"`
	Properties    map[string]string `json:"properties"`
}

// commitTableBody is the commit request, decoded from the raw body with
// iceberg-go's requirement and update types.
type commitTableBody struct {
	Identifier   *gen.TableIdentifier `json:"identifier"`
	Requirements table.Requirements   `json:"requirements"`
	Updates      table.Updates        `json:"updates"`
}

// ListTables implements [gen.StrictServerInterface].
func (s *Server) ListTables(
	ctx context.Context,
	req gen.ListTablesRequestObject,
) (gen.ListTablesResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	list := func(p store.PageRequest) (store.Page[store.Ident], error) {
		return s.catalog.ListTables(ctx, ns, p)
	}

	page, err := paginate(req.Params.PageToken, req.Params.PageSize, list)
	if err != nil {
		return nil, err
	}

	ids := make([]gen.TableIdentifier, 0, len(page.Items))
	for _, id := range page.Items {
		ids = append(ids, gen.TableIdentifier{Namespace: gen.Namespace(id.Namespace), Name: id.Name})
	}

	return gen.ListTables200JSONResponse{ListTablesResponseJSONResponse: gen.ListTablesResponseJSONResponse{
		Identifiers:   &ids,
		NextPageToken: nextToken(page),
	}}, nil
}

// CreateTable implements [gen.StrictServerInterface].
func (s *Server) CreateTable(
	ctx context.Context,
	req gen.CreateTableRequestObject,
) (gen.CreateTableResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	var body createTableBody
	if err := decodeRawBody(ctx, &body); err != nil {
		return nil, err
	}

	in, err := body.input(ns)
	if err != nil {
		return nil, err
	}

	t, err := s.catalog.CreateTable(ctx, in)
	if err != nil {
		return nil, err
	}

	return newTableResponse(t, ""), nil
}

func (b createTableBody) input(ns store.Namespace) (catalog.CreateTableInput, error) {
	in := catalog.CreateTableInput{
		Ident:      store.Ident{Namespace: ns, Name: b.Name},
		Location:   b.Location,
		Properties: b.Properties,
		Stage:      b.StageCreate,
	}

	if isAbsent(b.Schema) {
		return in, fmt.Errorf("%w: a schema is required", catalog.ErrInvalidRequest)
	}

	in.Schema = &iceberg.Schema{}
	if err := json.Unmarshal(b.Schema, in.Schema); err != nil {
		return in, fmt.Errorf("%w: schema: %w", catalog.ErrInvalidRequest, err)
	}

	if !isAbsent(b.PartitionSpec) {
		in.PartitionSpec = &iceberg.PartitionSpec{}
		if err := json.Unmarshal(b.PartitionSpec, in.PartitionSpec); err != nil {
			return in, fmt.Errorf("%w: partition spec: %w", catalog.ErrInvalidRequest, err)
		}
	}

	if !isAbsent(b.WriteOrder) {
		in.SortOrder = &table.SortOrder{}
		if err := json.Unmarshal(b.WriteOrder, in.SortOrder); err != nil {
			return in, fmt.Errorf("%w: write order: %w", catalog.ErrInvalidRequest, err)
		}
	}

	return in, nil
}

// LoadTable implements [gen.StrictServerInterface].
func (s *Server) LoadTable(ctx context.Context, req gen.LoadTableRequestObject) (gen.LoadTableResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.Table)
	if err != nil {
		return nil, err
	}

	t, err := s.catalog.LoadTable(ctx, id)
	if err != nil {
		return nil, err
	}

	snapshots := ""
	if req.Params.Snapshots != nil {
		snapshots = string(*req.Params.Snapshots)
	}

	resp := newTableResponse(t, snapshots)

	if req.Params.IfNoneMatch != nil && etagMatches(*req.Params.IfNoneMatch, resp.etag) {
		return notModified{etag: resp.etag}, nil
	}

	return resp, nil
}

// TableExists implements [gen.StrictServerInterface].
func (s *Server) TableExists(
	ctx context.Context,
	req gen.TableExistsRequestObject,
) (gen.TableExistsResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.Table)
	if err != nil {
		return nil, err
	}

	if err := s.catalog.TableExists(ctx, id); err != nil {
		return nil, err
	}

	return gen.TableExists204Response{}, nil
}

// UpdateTable implements [gen.StrictServerInterface].
func (s *Server) UpdateTable(
	ctx context.Context,
	req gen.UpdateTableRequestObject,
) (gen.UpdateTableResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.Table)
	if err != nil {
		return nil, err
	}

	var body commitTableBody
	if err := decodeRawBody(ctx, &body); err != nil {
		return nil, err
	}

	if body.Identifier != nil {
		named := store.Ident{Namespace: store.Namespace(body.Identifier.Namespace), Name: body.Identifier.Name}
		if !named.Equal(id) {
			return nil, fmt.Errorf("%w: body identifier %q does not match the path %q",
				catalog.ErrInvalidRequest, named.String(), id.String())
		}
	}

	t, err := s.catalog.CommitTable(ctx, catalog.CommitInput{
		Ident:        id,
		Requirements: body.Requirements,
		Updates:      body.Updates,
	})
	if err != nil {
		return nil, err
	}

	return newTableResponse(t, ""), nil
}

// DropTable implements [gen.StrictServerInterface].
func (s *Server) DropTable(ctx context.Context, req gen.DropTableRequestObject) (gen.DropTableResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.Table)
	if err != nil {
		return nil, err
	}

	purge := req.Params.PurgeRequested != nil && *req.Params.PurgeRequested

	if err := s.catalog.DropTable(ctx, id, purge); err != nil {
		return nil, err
	}

	return gen.DropTable204Response{}, nil
}

// RenameTable implements [gen.StrictServerInterface].
func (s *Server) RenameTable(
	ctx context.Context,
	req gen.RenameTableRequestObject,
) (gen.RenameTableResponseObject, error) {
	if err := s.checkPrefix(req.Prefix); err != nil {
		return nil, err
	}

	if req.Body == nil {
		return nil, fmt.Errorf("%w: missing request body", catalog.ErrInvalidRequest)
	}

	from := store.Ident{Namespace: store.Namespace(req.Body.Source.Namespace), Name: req.Body.Source.Name}
	to := store.Ident{Namespace: store.Namespace(req.Body.Destination.Namespace), Name: req.Body.Destination.Name}

	if err := s.catalog.RenameTable(ctx, from, to); err != nil {
		return nil, err
	}

	return gen.RenameTable204Response{}, nil
}

// ReportMetrics implements [gen.StrictServerInterface].
func (s *Server) ReportMetrics(
	ctx context.Context,
	req gen.ReportMetricsRequestObject,
) (gen.ReportMetricsResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.Table)
	if err != nil {
		return nil, err
	}

	reportType := ""
	if req.Body != nil {
		reportType = req.Body.ReportType
	}

	if err := s.catalog.ReportMetrics(ctx, id, reportType); err != nil {
		return nil, err
	}

	return gen.ReportMetrics204Response{}, nil
}

// tableResponse is the response of create, load and commit. It writes the
// stored metadata JSON verbatim instead of re-encoding it through generated
// types, so no field can be lost.
type tableResponse struct {
	body tableResponseBody
	etag string
}

type tableResponseBody struct {
	MetadataLocation string            `json:"metadata-location,omitempty"`
	Metadata         json.RawMessage   `json:"metadata"`
	Config           map[string]string `json:"config"`
}

func newTableResponse(t catalog.Table, snapshots string) tableResponse {
	return tableResponse{
		body: tableResponseBody{
			MetadataLocation: t.MetadataLocation,
			Metadata:         t.MetadataJSON,
			Config:           map[string]string{},
		},
		etag: etag(t, snapshots),
	}
}

func (r tableResponse) write(w http.ResponseWriter) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(r.body); err != nil {
		return fmt.Errorf("encoding table response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")

	if r.etag != "" {
		w.Header().Set("ETag", r.etag)
	}

	w.WriteHeader(http.StatusOK)

	if _, err := buf.WriteTo(w); err != nil {
		return fmt.Errorf("writing table response: %w", err)
	}

	return nil
}

// VisitCreateTableResponse implements [gen.CreateTableResponseObject].
func (r tableResponse) VisitCreateTableResponse(w http.ResponseWriter) error { return r.write(w) }

// VisitLoadTableResponse implements [gen.LoadTableResponseObject].
func (r tableResponse) VisitLoadTableResponse(w http.ResponseWriter) error { return r.write(w) }

// VisitUpdateTableResponse implements [gen.UpdateTableResponseObject].
func (r tableResponse) VisitUpdateTableResponse(w http.ResponseWriter) error { return r.write(w) }

// notModified answers a conditional load whose ETag still matches.
type notModified struct {
	etag string
}

// VisitLoadTableResponse implements [gen.LoadTableResponseObject].
func (r notModified) VisitLoadTableResponse(w http.ResponseWriter) error {
	w.Header().Set("ETag", r.etag)
	w.WriteHeader(http.StatusNotModified)

	return nil
}

// etag identifies a version of a table's metadata as served. A metadata file
// never changes, so its location identifies the content; the snapshots mode
// is included because it may change the representation.
func etag(t catalog.Table, snapshots string) string {
	if t.MetadataLocation == "" {
		return ""
	}

	sum := sha256.Sum256([]byte(t.MetadataLocation + "\x00" + snapshots))

	return `"` + hex.EncodeToString(sum[:16]) + `"`
}

// etagMatches reports whether an If-None-Match header matches tag.
func etagMatches(header, tag string) bool {
	for candidate := range strings.SplitSeq(header, ",") {
		candidate = strings.TrimPrefix(strings.TrimSpace(candidate), "W/")
		if candidate == "*" || candidate == tag {
			return true
		}
	}

	return false
}

func isAbsent(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)

	return len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null"))
}
