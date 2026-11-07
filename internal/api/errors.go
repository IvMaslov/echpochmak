package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"echpochmak/internal/api/gen"
	"echpochmak/internal/catalog"
	"echpochmak/internal/logger"
	"echpochmak/internal/store"
)

var (
	// errNotImplemented is returned by operations that are not implemented yet.
	errNotImplemented = errors.New("operation not implemented")
	// errNotFound is returned for requests to an unknown catalog prefix.
	errNotFound = errors.New("not found")
)

// Iceberg error types, as named by the REST spec and expected by clients.
const (
	typeBadRequest          = "BadRequestException"
	typeNoSuchNamespace     = "NoSuchNamespaceException"
	typeNoSuchTable         = "NoSuchTableException"
	typeNoSuchView          = "NoSuchViewException"
	typeAlreadyExists       = "AlreadyExistsException"
	typeNamespaceNotEmpty   = "NamespaceNotEmptyException"
	typeCommitFailed        = "CommitFailedException"
	typeCommitStateUnknown  = "CommitStateUnknownException"
	typeUnprocessable       = "UnprocessableEntityException"
	typeUnsupported         = "UnsupportedOperationException"
	typeServiceUnavailable  = "ServiceUnavailableException"
	typeInternalServerError = "InternalServerError"
	typeNotFound            = "NotFoundException"
)

// writeError writes err as an Iceberg error response. Errors without a
// mapping are logged and reported as an internal error without details.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	status, errType, ok := classify(err)
	if !ok {
		logger.FromContext(r.Context()).ErrorContext(r.Context(), "request failed", logger.Err(err))
		writeErrorModel(w, r, http.StatusInternalServerError, typeInternalServerError, "internal server error")

		return
	}

	writeErrorModel(w, r, status, errType, err.Error())
}

// classify maps a known error to its HTTP status and Iceberg error type.
func classify(err error) (status int, errType string, ok bool) {
	switch {
	case errors.Is(err, errNotImplemented):
		return http.StatusNotImplemented, typeUnsupported, true
	case errors.Is(err, errNotFound):
		return http.StatusNotFound, typeNotFound, true
	case errors.Is(err, store.ErrInvalidArgument), errors.Is(err, catalog.ErrInvalidRequest):
		return http.StatusBadRequest, typeBadRequest, true
	case errors.Is(err, catalog.ErrPropertyConflict):
		return http.StatusUnprocessableEntity, typeUnprocessable, true
	case errors.Is(err, catalog.ErrRequirementFailed):
		return http.StatusConflict, typeCommitFailed, true
	case errors.Is(err, catalog.ErrCommitStateUnknown):
		return http.StatusServiceUnavailable, typeCommitStateUnknown, true
	case errors.Is(err, store.ErrNamespaceNotFound):
		return http.StatusNotFound, typeNoSuchNamespace, true
	case errors.Is(err, store.ErrTableNotFound):
		return http.StatusNotFound, typeNoSuchTable, true
	case errors.Is(err, store.ErrViewNotFound):
		return http.StatusNotFound, typeNoSuchView, true
	case errors.Is(err, store.ErrNamespaceAlreadyExists),
		errors.Is(err, store.ErrTableAlreadyExists),
		errors.Is(err, store.ErrViewAlreadyExists):
		return http.StatusConflict, typeAlreadyExists, true
	case errors.Is(err, store.ErrNamespaceNotEmpty):
		return http.StatusConflict, typeNamespaceNotEmpty, true
	case errors.Is(err, store.ErrCommitConflict):
		return http.StatusConflict, typeCommitFailed, true
	case errors.Is(err, store.ErrUnavailable):
		return http.StatusServiceUnavailable, typeServiceUnavailable, true
	default:
		return 0, "", false
	}
}

// writeBadRequest reports a request that could not be decoded or bound.
func writeBadRequest(w http.ResponseWriter, r *http.Request, err error) {
	writeErrorModel(w, r, http.StatusBadRequest, typeBadRequest, err.Error())
}

func writeErrorModel(w http.ResponseWriter, r *http.Request, status int, errType, message string) {
	body := gen.IcebergErrorResponse{
		Error: gen.ErrorModel{
			Code:    status,
			Type:    errType,
			Message: message,
		},
	}

	writeJSON(w, r, status, body)
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if r.Method == http.MethodHead {
		return
	}

	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.FromContext(r.Context()).DebugContext(
			r.Context(),
			"writing response body failed",
			logger.Err(err),
			slog.Int(logger.KeyStatus, status),
		)
	}
}
