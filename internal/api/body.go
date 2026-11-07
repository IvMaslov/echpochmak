package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"echpochmak/internal/catalog"
)

type rawBodyKey struct{}

// withRawBody limits the size of request bodies, reads them whole and keeps
// a copy in the request context, while the generated decoder still reads
// the body as usual. Handlers that need fields the generated types do not
// model decode the copy with [decodeRawBody].
func withRawBody(maxBytes int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body == nil || r.Body == http.NoBody {
			next.ServeHTTP(w, r)

			return
		}

		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBytes))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				writeErrorModel(w, r, http.StatusRequestEntityTooLarge, typeBadRequest,
					fmt.Sprintf("request body exceeds %d bytes", tooLarge.Limit))

				return
			}

			writeBadRequest(w, r, fmt.Errorf("reading request body: %w", err))

			return
		}

		r.Body = io.NopCloser(bytes.NewReader(body))
		r = r.WithContext(context.WithValue(r.Context(), rawBodyKey{}, body))

		next.ServeHTTP(w, r)
	})
}

// decodeRawBody decodes the request body kept by [withRawBody] into v.
func decodeRawBody(ctx context.Context, v any) error {
	body, ok := ctx.Value(rawBodyKey{}).([]byte)
	if !ok || len(body) == 0 {
		return fmt.Errorf("%w: missing request body", catalog.ErrInvalidRequest)
	}

	if err := json.Unmarshal(body, v); err != nil {
		return fmt.Errorf("%w: decoding request body: %w", catalog.ErrInvalidRequest, err)
	}

	return nil
}
