package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/view"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// createViewBody is the create-view request, decoded from the raw body with
// iceberg-go's schema and view version types.
type createViewBody struct {
	Name        string            `json:"name"`
	Location    string            `json:"location"`
	Schema      json.RawMessage   `json:"schema"`
	ViewVersion *view.Version     `json:"view-version"`
	Properties  map[string]string `json:"properties"`
}

// replaceViewBody is the commit-view request, decoded with iceberg-go's view
// requirement and update types.
type replaceViewBody struct {
	Identifier   *gen.TableIdentifier `json:"identifier"`
	Requirements view.Requirements    `json:"requirements"`
	Updates      view.Updates         `json:"updates"`
}

// ListViews implements [gen.StrictServerInterface].
func (s *Server) ListViews(ctx context.Context, req gen.ListViewsRequestObject) (gen.ListViewsResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	list := func(p store.PageRequest) (store.Page[store.Ident], error) {
		return s.catalog.ListViews(ctx, ns, p)
	}

	page, err := paginate(req.Params.PageToken, req.Params.PageSize, list)
	if err != nil {
		return nil, err
	}

	ids := make([]gen.TableIdentifier, 0, len(page.Items))
	for _, id := range page.Items {
		ids = append(ids, gen.TableIdentifier{Namespace: gen.Namespace(id.Namespace), Name: id.Name})
	}

	return gen.ListViews200JSONResponse{ListTablesResponseJSONResponse: gen.ListTablesResponseJSONResponse{
		Identifiers:   &ids,
		NextPageToken: nextToken(page),
	}}, nil
}

// CreateView implements [gen.StrictServerInterface].
func (s *Server) CreateView(
	ctx context.Context,
	req gen.CreateViewRequestObject,
) (gen.CreateViewResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	var body createViewBody
	if err := decodeRawBody(ctx, &body); err != nil {
		return nil, err
	}

	if isAbsent(body.Schema) || body.ViewVersion == nil {
		return nil, fmt.Errorf("%w: a view needs a schema and a view-version", errBadRequest)
	}

	schema := &iceberg.Schema{}
	if err := json.Unmarshal(body.Schema, schema); err != nil {
		return nil, fmt.Errorf("%w: schema: %w", errBadRequest, err)
	}

	v, err := s.catalog.CreateView(ctx, catalog.CreateViewInput{
		Ident:      store.Ident{Namespace: ns, Name: body.Name},
		Location:   body.Location,
		Schema:     schema,
		Version:    body.ViewVersion,
		Properties: body.Properties,
	})
	if err != nil {
		return nil, err
	}

	return newViewResponse(v), nil
}

// LoadView implements [gen.StrictServerInterface].
func (s *Server) LoadView(ctx context.Context, req gen.LoadViewRequestObject) (gen.LoadViewResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.View)
	if err != nil {
		return nil, err
	}

	v, err := s.catalog.LoadView(ctx, id)
	if err != nil {
		return nil, err
	}

	return newViewResponse(v), nil
}

// ViewExists implements [gen.StrictServerInterface].
func (s *Server) ViewExists(
	ctx context.Context,
	req gen.ViewExistsRequestObject,
) (gen.ViewExistsResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.View)
	if err != nil {
		return nil, err
	}

	if err := s.catalog.ViewExists(ctx, id); err != nil {
		return nil, err
	}

	return gen.ViewExists204Response{}, nil
}

// ReplaceView implements [gen.StrictServerInterface].
func (s *Server) ReplaceView(
	ctx context.Context,
	req gen.ReplaceViewRequestObject,
) (gen.ReplaceViewResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.View)
	if err != nil {
		return nil, err
	}

	var body replaceViewBody
	if err := decodeRawBody(ctx, &body); err != nil {
		return nil, err
	}

	if err := checkBodyIdent(body.Identifier, id); err != nil {
		return nil, err
	}

	v, err := s.catalog.ReplaceView(ctx, catalog.ReplaceViewInput{
		Ident:        id,
		Requirements: body.Requirements,
		Updates:      body.Updates,
	})
	if err != nil {
		return nil, err
	}

	return newViewResponse(v), nil
}

// DropView implements [gen.StrictServerInterface].
func (s *Server) DropView(ctx context.Context, req gen.DropViewRequestObject) (gen.DropViewResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.View)
	if err != nil {
		return nil, err
	}

	if err := s.catalog.DropView(ctx, id); err != nil {
		return nil, err
	}

	return gen.DropView204Response{}, nil
}

// RenameView implements [gen.StrictServerInterface].
func (s *Server) RenameView(
	ctx context.Context,
	req gen.RenameViewRequestObject,
) (gen.RenameViewResponseObject, error) {
	if err := s.checkPrefix(req.Prefix); err != nil {
		return nil, err
	}

	if req.Body == nil {
		return nil, fmt.Errorf("%w: missing request body", errBadRequest)
	}

	from := store.Ident{Namespace: store.Namespace(req.Body.Source.Namespace), Name: req.Body.Source.Name}
	to := store.Ident{Namespace: store.Namespace(req.Body.Destination.Namespace), Name: req.Body.Destination.Name}

	if err := s.catalog.RenameView(ctx, from, to); err != nil {
		return nil, err
	}

	return gen.RenameView204Response{}, nil
}

// RegisterView implements [gen.StrictServerInterface].
func (s *Server) RegisterView(
	ctx context.Context,
	req gen.RegisterViewRequestObject,
) (gen.RegisterViewResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	if req.Body == nil {
		return nil, fmt.Errorf("%w: missing request body", errBadRequest)
	}

	v, err := s.catalog.RegisterView(ctx, store.Ident{Namespace: ns, Name: req.Body.Name}, req.Body.MetadataLocation)
	if err != nil {
		return nil, err
	}

	return newViewResponse(v), nil
}

// checkBodyIdent rejects a request body that names another entry than the
// path.
func checkBodyIdent(body *gen.TableIdentifier, path store.Ident) error {
	if body == nil {
		return nil
	}

	named := store.Ident{Namespace: store.Namespace(body.Namespace), Name: body.Name}
	if !named.Equal(path) {
		return fmt.Errorf("%w: body identifier %q does not match the path %q",
			errBadRequest, named.String(), path.String())
	}

	return nil
}

// viewResponse is the response of every view operation that returns a view.
// It writes the stored metadata JSON verbatim.
type viewResponse struct {
	body viewResponseBody
}

type viewResponseBody struct {
	MetadataLocation string            `json:"metadata-location"`
	Metadata         json.RawMessage   `json:"metadata"`
	Config           map[string]string `json:"config"`
}

func newViewResponse(v catalog.View) viewResponse {
	return viewResponse{body: viewResponseBody{
		MetadataLocation: v.MetadataLocation,
		Metadata:         v.MetadataJSON,
		Config:           map[string]string{},
	}}
}

func (r viewResponse) write(w http.ResponseWriter) error {
	return writeRawJSON(w, http.StatusOK, r.body)
}

// VisitCreateViewResponse implements [gen.CreateViewResponseObject].
func (r viewResponse) VisitCreateViewResponse(w http.ResponseWriter) error { return r.write(w) }

// VisitLoadViewResponse implements [gen.LoadViewResponseObject].
func (r viewResponse) VisitLoadViewResponse(w http.ResponseWriter) error { return r.write(w) }

// VisitReplaceViewResponse implements [gen.ReplaceViewResponseObject].
func (r viewResponse) VisitReplaceViewResponse(w http.ResponseWriter) error { return r.write(w) }

// VisitRegisterViewResponse implements [gen.RegisterViewResponseObject].
func (r viewResponse) VisitRegisterViewResponse(w http.ResponseWriter) error { return r.write(w) }

// writeRawJSON encodes body fully before writing, so an encoding error can
// still become an error response.
func writeRawJSON(w http.ResponseWriter, status int, body any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return fmt.Errorf("encoding response: %w", err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if _, err := buf.WriteTo(w); err != nil {
		return fmt.Errorf("writing response: %w", err)
	}

	return nil
}
