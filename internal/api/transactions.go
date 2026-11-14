package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/apache/iceberg-go/table"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/catalog"
	"echpochmak/internal/store"
)

// commitTransactionBody is the multi-table commit request, decoded with
// iceberg-go's requirement and update types.
type commitTransactionBody struct {
	TableChanges []transactionChange `json:"table-changes"`
}

type transactionChange struct {
	Identifier   *gen.TableIdentifier `json:"identifier"`
	Requirements table.Requirements   `json:"requirements"`
	Updates      table.Updates        `json:"updates"`
}

// CommitTransaction implements [gen.StrictServerInterface].
func (s *Server) CommitTransaction(
	ctx context.Context,
	req gen.CommitTransactionRequestObject,
) (gen.CommitTransactionResponseObject, error) {
	if err := s.checkPrefix(req.Prefix); err != nil {
		return nil, err
	}

	var body commitTransactionBody
	if err := decodeRawBody(ctx, &body); err != nil {
		return nil, err
	}

	changes := make([]catalog.CommitInput, 0, len(body.TableChanges))

	for i, c := range body.TableChanges {
		if c.Identifier == nil {
			return nil, fmt.Errorf("%w: table change %d has no identifier", errBadRequest, i)
		}

		changes = append(changes, catalog.CommitInput{
			Ident:        store.Ident{Namespace: store.Namespace(c.Identifier.Namespace), Name: c.Identifier.Name},
			Requirements: c.Requirements,
			Updates:      c.Updates,
		})
	}

	if err := s.catalog.CommitTransaction(ctx, changes); err != nil {
		return nil, err
	}

	return gen.CommitTransaction204Response{}, nil
}

// RegisterTable implements [gen.StrictServerInterface].
func (s *Server) RegisterTable(
	ctx context.Context,
	req gen.RegisterTableRequestObject,
) (gen.RegisterTableResponseObject, error) {
	ns, err := s.namespace(req.Prefix, req.NamespaceParam)
	if err != nil {
		return nil, err
	}

	if req.Body == nil {
		return nil, fmt.Errorf("%w: missing request body", errBadRequest)
	}

	t, err := s.catalog.RegisterTable(ctx, catalog.RegisterTableInput{
		Ident:            store.Ident{Namespace: ns, Name: req.Body.Name},
		MetadataLocation: req.Body.MetadataLocation,
		Overwrite:        req.Body.Overwrite != nil && *req.Body.Overwrite,
	})
	if err != nil {
		return nil, err
	}

	return newTableResponse(t, ""), nil
}

// UnregisterTable implements [gen.StrictServerInterface].
func (s *Server) UnregisterTable(
	ctx context.Context,
	req gen.UnregisterTableRequestObject,
) (gen.UnregisterTableResponseObject, error) {
	id, err := s.ident(req.Prefix, req.NamespaceParam, req.Table)
	if err != nil {
		return nil, err
	}

	t, err := s.catalog.UnregisterTable(ctx, id)
	if err != nil {
		return nil, err
	}

	return unregisterResponse{body: unregisterResponseBody{
		MetadataLocation: t.MetadataLocation,
		Metadata:         t.MetadataJSON,
	}}, nil
}

// unregisterResponse writes the table's last metadata verbatim.
type unregisterResponse struct {
	body unregisterResponseBody
}

type unregisterResponseBody struct {
	MetadataLocation string          `json:"metadata-location"`
	Metadata         json.RawMessage `json:"metadata"`
}

// VisitUnregisterTableResponse implements [gen.UnregisterTableResponseObject].
func (r unregisterResponse) VisitUnregisterTableResponse(w http.ResponseWriter) error {
	return writeRawJSON(w, http.StatusOK, r.body)
}

// VisitRegisterTableResponse implements [gen.RegisterTableResponseObject].
func (r tableResponse) VisitRegisterTableResponse(w http.ResponseWriter) error { return r.write(w) }
