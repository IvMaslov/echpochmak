package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"echpochmak/internal/idempotency"
	"echpochmak/internal/logger"
)

// HeaderIdempotencyKey carries the client's idempotency key.
const HeaderIdempotencyKey = "Idempotency-Key"

// HeaderIdempotentReplay marks a response replayed for a repeated key. It is
// not part of the spec; it helps clients and tests see what happened.
const HeaderIdempotentReplay = "Idempotent-Replay"

// maxStoredBody bounds a response stored for replay. Larger responses are
// not finalized: the claim is released and a retry runs again, which is
// safe for every operation in scope.
const maxStoredBody = 64 << 10

// Routes that honour Idempotency-Key: the mutations this server implements.
const (
	routeCreateNamespace = "POST /v1/{prefix}/namespaces"
	routeDropNamespace   = "DELETE /v1/{prefix}/namespaces/{namespace}"
	routeUpdateProps     = "POST /v1/{prefix}/namespaces/{namespace}/properties"
	routeCreateTable     = "POST /v1/{prefix}/namespaces/{namespace}/tables"
	routeCommitTable     = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"
	routeDropTable       = "DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"
	routeRenameTable     = "POST /v1/{prefix}/tables/rename"
	routeRegisterTable   = "POST /v1/{prefix}/namespaces/{namespace}/register"
	routeUnregister      = "POST /v1/{prefix}/namespaces/{namespace}/tables/{table}/unregister"
	routeTransaction     = "POST /v1/{prefix}/transactions/commit"
	routeCreateView      = "POST /v1/{prefix}/namespaces/{namespace}/views"
	routeReplaceView     = "POST /v1/{prefix}/namespaces/{namespace}/views/{view}"
	routeDropView        = "DELETE /v1/{prefix}/namespaces/{namespace}/views/{view}"
	routeRenameView      = "POST /v1/{prefix}/views/rename"
	routeRegisterView    = "POST /v1/{prefix}/namespaces/{namespace}/register-view"
)

func idempotentRoute(pattern string) bool {
	switch pattern {
	case routeCreateNamespace, routeDropNamespace, routeUpdateProps,
		routeCreateTable, routeCommitTable, routeDropTable, routeRenameTable,
		routeRegisterTable, routeUnregister, routeTransaction,
		routeCreateView, routeReplaceView, routeDropView, routeRenameView, routeRegisterView:
		return true
	default:
		return false
	}
}

// MetadataLoader reads a table metadata file, as stored. Replays of an
// unregister use it.
type MetadataLoader func(ctx context.Context, location string) (json.RawMessage, error)

// withIdempotency applies the Idempotency-Key protocol to the mutations in
// scope. It needs the raw body, so it runs inside withRawBody, and the mux,
// to know which route a request takes before serving it.
func withIdempotency(
	g *idempotency.Guard,
	load MetadataLoader,
	mux *http.ServeMux,
	next http.Handler,
) http.Handler {
	if g == nil {
		return next
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(HeaderIdempotencyKey)
		if key == "" {
			next.ServeHTTP(w, r)

			return
		}

		_, pattern := mux.Handler(r)
		if !idempotentRoute(pattern) {
			next.ServeHTTP(w, r)

			return
		}

		body, _ := r.Context().Value(rawBodyKey{}).([]byte)

		// A staged create has no effect to repeat; it is simply run again.
		if pattern == routeCreateTable && stagedCreate(body) {
			next.ServeHTTP(w, r)

			return
		}

		if _, err := uuid.Parse(key); err != nil || len(key) != 36 {
			writeErrorModel(w, r, http.StatusBadRequest, typeBadRequest, "Idempotency-Key must be a UUID string")

			return
		}

		serveIdempotent(g, load, next, w, r, idempotentRequest{key: key, pattern: pattern, body: body})
	})
}

// idempotentRequest is a request taking part in the protocol.
type idempotentRequest struct {
	key     string
	pattern string
	body    []byte
}

func serveIdempotent(
	g *idempotency.Guard,
	load MetadataLoader,
	next http.Handler,
	w http.ResponseWriter,
	r *http.Request,
	req idempotentRequest,
) {
	key, pattern, body := req.key, req.pattern, req.body
	ctx := r.Context()

	decision, rec, ticket, err := g.Begin(ctx, key, fingerprint(r, body))
	if err != nil {
		setIdempotency(r, idemUnavailable)
		logger.FromContext(ctx).WarnContext(ctx, "idempotency store unavailable", logger.Err(err))
		writeErrorModel(w, r, http.StatusServiceUnavailable, typeServiceUnavailable,
			"the idempotency store is unavailable; retry later")

		return
	}

	switch decision {
	case idempotency.Replay:
		setIdempotency(r, idemReplayed)
		replay(next, load, w, r, rec.Response)
	case idempotency.Mismatch:
		setIdempotency(r, idemMismatch)
		writeErrorModel(w, r, http.StatusUnprocessableEntity, typeUnprocessable,
			"Idempotency-Key was already used for a different request")
	case idempotency.Busy:
		setIdempotency(r, idemBusy)
		w.Header().Set("Retry-After", "1")
		writeErrorModel(w, r, http.StatusServiceUnavailable, typeServiceUnavailable,
			"a request with this Idempotency-Key is in progress; retry later")
	case idempotency.Execute:
		setIdempotency(r, idemFirst)
		rec := &captureWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		g.Finish(ctx, ticket, rec.response(r, pattern, body))
	}
}

// fingerprint identifies a request: method, path and body. A key reused
// with any of them different is a different operation.
func fingerprint(r *http.Request, body []byte) string {
	h := sha256.New()
	h.Write([]byte(r.Method + "\n" + r.URL.EscapedPath() + "\n"))
	h.Write(body)

	return hex.EncodeToString(h.Sum(nil))
}

func stagedCreate(body []byte) bool {
	var req struct {
		StageCreate bool `json:"stage-create"`
	}

	return json.Unmarshal(body, &req) == nil && req.StageCreate
}

// replay answers with a stored response. A table or view response is served
// by loading it now, which the spec allows: a replay may show a newer state
// of the catalog.
func replay(
	next http.Handler,
	load MetadataLoader,
	w http.ResponseWriter,
	r *http.Request,
	resp idempotency.Response,
) {
	w.Header().Set(HeaderIdempotentReplay, "true")

	if resp.ReplayMetadata != "" {
		replayUnregister(load, w, r, resp.ReplayMetadata)

		return
	}

	if resp.ReplayPath != "" {
		get := r.Clone(r.Context())
		get.Method = http.MethodGet
		get.Body = http.NoBody
		get.ContentLength = 0
		get.Header.Del(HeaderIdempotencyKey)

		u := *r.URL
		u.RawPath = resp.ReplayPath

		path, err := url.PathUnescape(resp.ReplayPath)
		if err != nil {
			writeErrorModel(w, r, http.StatusInternalServerError, typeInternalServerError, "corrupt replay path")

			return
		}

		u.Path = path
		get.URL = &u
		next.ServeHTTP(w, get)

		return
	}

	if resp.ContentType != "" {
		w.Header().Set("Content-Type", resp.ContentType)
	}

	if resp.ETag != "" {
		w.Header().Set("ETag", resp.ETag)
	}

	w.WriteHeader(resp.Status)
	_, _ = w.Write(resp.Body)
}

// replayUnregister answers a repeated unregister from the metadata file it
// returned the first time. If that file is gone, the answer is what running
// the unregister again would give: the table does not exist.
func replayUnregister(load MetadataLoader, w http.ResponseWriter, r *http.Request, location string) {
	md, err := load(r.Context(), location)
	if err != nil {
		writeErrorModel(w, r, http.StatusNotFound, typeNoSuchTable,
			"Table does not exist: its metadata file "+location+" is gone")

		return
	}

	body := unregisterResponseBody{MetadataLocation: location, Metadata: md}
	if err := writeRawJSON(w, http.StatusOK, body); err != nil {
		logger.FromContext(r.Context()).DebugContext(r.Context(), "writing a replay failed", logger.Err(err))
	}
}

// captureWriter records a response while writing it.
type captureWriter struct {
	http.ResponseWriter

	status   int
	body     bytes.Buffer
	overflow bool
}

func (c *captureWriter) WriteHeader(status int) {
	c.status = status
	c.ResponseWriter.WriteHeader(status)
}

func (c *captureWriter) Write(p []byte) (int, error) {
	if c.body.Len()+len(p) <= maxStoredBody {
		c.body.Write(p)
	} else {
		c.overflow = true
	}

	return c.ResponseWriter.Write(p) //nolint:wrapcheck // a pass-through writer.
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (c *captureWriter) Unwrap() http.ResponseWriter {
	return c.ResponseWriter
}

// response is what to store for replay. Successful responses that carry
// table or view metadata are replayed by loading the entry again, so their
// large metadata is not stored.
func (c *captureWriter) response(r *http.Request, pattern string, body []byte) idempotency.Response {
	resp := idempotency.Response{
		Status:      c.status,
		ContentType: c.Header().Get("Content-Type"),
		ETag:        c.Header().Get("ETag"),
	}

	if c.status == http.StatusOK {
		if path, location := replaySource(r, pattern, body, c.body.Bytes()); path != "" || location != "" {
			resp.ReplayPath = path
			resp.ReplayMetadata = location

			return resp
		}
	}

	if c.overflow {
		// Too large to store: not finalized, so a retry runs again.
		resp.Status = http.StatusServiceUnavailable

		return resp
	}

	resp.Body = bytes.Clone(c.body.Bytes())

	return resp
}

// replaySource returns where the replay of a successful response comes from:
// the path to GET, or for an unregister the metadata file to read. Both are
// empty when the response itself is stored.
func replaySource(r *http.Request, pattern string, reqBody, respBody []byte) (path, location string) {
	reqPath := strings.TrimSuffix(r.URL.EscapedPath(), "/")

	switch pattern {
	case routeCommitTable, routeReplaceView:
		return reqPath, ""
	case routeCreateTable, routeCreateView:
		if name := bodyName(reqBody); name != "" {
			return reqPath + "/" + url.PathEscape(name), ""
		}
	case routeRegisterTable:
		if name := bodyName(reqBody); name != "" {
			return strings.TrimSuffix(reqPath, "/register") + "/tables/" + url.PathEscape(name), ""
		}
	case routeRegisterView:
		if name := bodyName(reqBody); name != "" {
			return strings.TrimSuffix(reqPath, "/register-view") + "/views/" + url.PathEscape(name), ""
		}
	case routeUnregister:
		var resp struct {
			MetadataLocation string `json:"metadata-location"`
		}

		if json.Unmarshal(respBody, &resp) == nil && resp.MetadataLocation != "" {
			return "", resp.MetadataLocation
		}
	}

	return "", ""
}

// bodyName returns the "name" field of a request body.
func bodyName(body []byte) string {
	var req struct {
		Name string `json:"name"`
	}

	if json.Unmarshal(body, &req) != nil {
		return ""
	}

	return req.Name
}
