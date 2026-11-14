package api

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"echpochmak/internal/logger"
)

// Routes of the API documentation, outside /v1 like the probes.
const (
	pathDocs = "/docs"
	pathSpec = "/openapi.json"
)

// routeConfig is the one operation served outside Endpoints(): clients call
// it before they know the endpoints.
const routeConfig = "GET /v1/config"

// specYAML is the pinned Iceberg REST spec with the codegen overlay applied.
//
//go:embed gen/openapi.gen.yaml
var specYAML []byte

// swaggerUI holds the vendored swagger-ui-dist files; see
// swaggerui/dist/VERSION.
//
//go:embed swaggerui/dist
var swaggerUI embed.FS

// docsPage is the Swagger UI page. The assets are served by this server, so
// the page works without access to a CDN.
const docsPage = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Iceberg REST catalog</title>
<link rel="icon" href="{{.Base}}/favicon-32x32.png">
<link rel="stylesheet" href="{{.Base}}/swagger-ui.css">
</head>
<body>
<div id="swagger-ui"></div>
<script src="{{.Base}}/swagger-ui-bundle.js"></script>
<script>
window.ui = SwaggerUIBundle({
  url: "{{.Spec}}",
  dom_id: "#swagger-ui",
  deepLinking: true,
  tryItOutEnabled: true,
  displayRequestDuration: true
});
</script>
</body>
</html>
`

// newDocsHandler serves the Swagger UI at /docs and the served spec at
// /openapi.json. The served spec keeps only the operations this server
// implements, so "Try it out" never offers one that answers 501.
func newDocsHandler(prefix string) (http.Handler, error) {
	spec, err := servedSpec(specYAML, append(Endpoints(), routeConfig), prefix)
	if err != nil {
		return nil, err
	}

	tmpl, err := template.New("docs").Parse(docsPage)
	if err != nil {
		return nil, fmt.Errorf("parsing the docs page: %w", err)
	}

	var page bytes.Buffer
	if err := tmpl.Execute(&page, struct{ Base, Spec string }{Base: pathDocs, Spec: pathSpec}); err != nil {
		return nil, fmt.Errorf("rendering the docs page: %w", err)
	}

	assets, err := fs.Sub(swaggerUI, "swaggerui/dist")
	if err != nil {
		return nil, fmt.Errorf("swagger ui assets: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+pathSpec, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		writeBytes(w, r, spec)
	})
	mux.HandleFunc("GET "+pathDocs, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, pathDocs+"/", http.StatusMovedPermanently)
	})
	mux.HandleFunc("GET "+pathDocs+"/{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		writeBytes(w, r, page.Bytes())
	})
	mux.Handle("GET "+pathDocs+"/", http.StripPrefix(pathDocs, http.FileServerFS(assets)))

	return mux, nil
}

func writeBytes(w http.ResponseWriter, r *http.Request, data []byte) {
	if _, err := w.Write(data); err != nil {
		logger.FromContext(r.Context()).DebugContext(r.Context(), "writing a docs response failed", logger.Err(err))
	}
}

// servedSpec returns the spec as JSON, reduced to the given operations
// ("METHOD /path", as in Endpoints()), without authentication, for this
// server's origin, and with the catalog prefix filled in.
func servedSpec(raw []byte, operations []string, prefix string) ([]byte, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(raw, &root); err != nil {
		return nil, fmt.Errorf("parsing the spec: %w", err)
	}

	doc, ok := yamlValue(&root).(map[string]any)
	if !ok {
		return nil, errors.New("the spec is not a YAML mapping")
	}

	paths, _ := doc["paths"].(map[string]any)
	doc["paths"] = keepOperations(paths, operations)
	doc["servers"] = []any{map[string]any{"url": "/", "description": "this catalog server"}}

	// The server has no authentication.
	delete(doc, "security")

	if components, ok := doc["components"].(map[string]any); ok {
		delete(components, "securitySchemes")

		if param := nestedMap(components, "parameters", "prefix", "schema"); param != nil {
			param["default"] = prefix
		}
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("encoding the served spec: %w", err)
	}

	return out, nil
}

// httpMethods are the operation keys of an OpenAPI path item.
//
//nolint:gochecknoglobals // a read-only list.
var httpMethods = []string{"get", "put", "post", "delete", "options", "head", "patch", "trace"}

// keepOperations returns the path items of paths with only the listed
// operations, without per-operation security, and drops empty items.
func keepOperations(paths map[string]any, operations []string) map[string]any {
	kept := map[string]any{}

	for path, item := range paths {
		ops, ok := item.(map[string]any)
		if !ok {
			continue
		}

		keptOne := false

		for _, method := range httpMethods {
			op, exists := ops[method].(map[string]any)
			if !exists {
				continue
			}

			if !slices.Contains(operations, strings.ToUpper(method)+" "+path) {
				delete(ops, method)

				continue
			}

			delete(op, "security")

			keptOne = true
		}

		if keptOne {
			kept[path] = ops
		}
	}

	return kept
}

// nestedMap walks maps by keys and returns the innermost one, or nil.
func nestedMap(m map[string]any, keys ...string) map[string]any {
	for _, k := range keys {
		next, ok := m[k].(map[string]any)
		if !ok {
			return nil
		}

		m = next
	}

	return m
}

// yamlValue converts a YAML node to JSON-compatible values: mappings become
// map[string]any whatever their keys, so response codes such as 200 stay
// strings, and aliases are resolved.
func yamlValue(n *yaml.Node) any {
	switch n.Kind {
	case yaml.DocumentNode:
		if len(n.Content) == 0 {
			return nil
		}

		return yamlValue(n.Content[0])
	case yaml.AliasNode:
		return yamlValue(n.Alias)
	case yaml.MappingNode:
		m := make(map[string]any, len(n.Content)/2)
		for i := 0; i+1 < len(n.Content); i += 2 {
			m[n.Content[i].Value] = yamlValue(n.Content[i+1])
		}

		return m
	case yaml.SequenceNode:
		s := make([]any, 0, len(n.Content))
		for _, c := range n.Content {
			s = append(s, yamlValue(c))
		}

		return s
	default:
		var v any
		if err := n.Decode(&v); err != nil {
			return n.Value
		}

		return v
	}
}
