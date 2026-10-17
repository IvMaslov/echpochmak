# ADR-001: Code generation for the Iceberg REST API

- **Status:** accepted
- **Date:** 2026-10-17
- **Context:** Phase 0, Step 2

## Context

The Iceberg REST catalog spec is pinned at commit `79bc69a5` (2026-10-09) in
`api/iceberg/rest-catalog-open-api.yaml`. It is OpenAPI **3.1.1**, about 6,400
lines, with 66 `const`, 8 discriminators, 16 `oneOf`, 6 `anyOf` and 60
`allOf`. We need Go server types and routing generated from it, served through
plain `net/http` so we control the middleware.

## Candidates tried

### ogen v1.24.0: rejected

- Generation first fails on an upstream spec bug (see "Spec fixes" below).
- With that bug patched, ogen still cannot handle the spec. It reports
  "discriminator inference not implemented" and "complex anyOf not
  implemented". With `ignore_not_implemented: [all]` it generates, but it
  **skips 13 core operations**, including create, load and commit table, the
  multi-table transaction commit, register table, the view operations and
  scan planning. Those operations are the product, so ogen fails the
  "models TableUpdate/TableRequirement as usable tagged unions" criterion
  outright.

### oapi-codegen v2.8.0: accepted

- Needs two small fixes (below); then it generates and compiles.
- Generates all **35 operations** as a typed `StrictServerInterface`, with
  request/response objects per status code. This includes `ETag` response
  headers and the `Idempotency-Key` parameter.
- Unions (`TableUpdate`, `TableRequirement`, …) are `json.RawMessage` wrappers
  with typed `AsXxx()`/`FromXxx()` accessors. There is no discriminator
  dispatch helper, so the handler reads `action`/`type` itself. That is fine,
  because we plan to decode updates with `iceberg-go` anyway.
- `std-http-server` registers Go 1.22+ method patterns on our own
  `http.ServeMux` (`GET /v1/{prefix}/namespaces`), so our middleware wraps the
  generated routes and `r.Pattern` gives a low-cardinality route name for logs.
- `encoding/json` ignores unknown request fields, so clients that send
  newer-spec fields still work.

## Spec fixes

The pinned spec stays untouched. Fixes live in `api/iceberg/overlay.yaml`, an
[OpenAPI Overlay](https://github.com/OAI/Overlay-Specification) document:

1. **Name clash:** the path parameter `namespace` and the schema `Namespace`
   both map to the Go name `Namespace`. The overlay sets
   `x-go-name: NamespaceParam` on the parameter.
2. **Upstream bug:** the `CreateTableResponse`, `LoadTableResponse` and
   `CommitTableResponse` headers reference `#/components/parameters/etag`,
   a *parameter* where a *Header Object* is required. The overlay replaces the
   reference with an inline header definition. This is worth reporting
   upstream.

oapi-codegen can apply overlays itself, but only **after** it has loaded and
validated the spec, and fix 2 is exactly what makes loading fail. So
`internal/tools/applyoverlay` applies the overlay to the raw YAML first,
using the same library (`speakeasy-api/openapi/overlay`). It runs in strict
mode: an action whose target no longer matches fails the build. That way, a
spec update that fixes the bug upstream is noticed and the stale fix removed.

Pipeline (`go generate ./internal/api/gen`, via `make generate`):

```
rest-catalog-open-api.yaml ──applyoverlay(overlay.yaml)──► openapi.gen.yaml ──oapi-codegen──► api.gen.go
```

Both outputs are committed. `make check-generate` fails CI when they are stale.
oapi-codegen is pinned as a `tool` dependency in `go.mod`.

## iceberg-go evaluation (for Phase 1)

`github.com/apache/iceberg-go` v0.7.0:

- **Table updates:** all 23 actions in the spec are implemented as
  `table.Update` (`Apply(*MetadataBuilder)`), with JSON decoding of the update
  list. These are add/remove schema, spec, sort order, snapshot, refs,
  properties, statistics and encryption keys, plus location, UUID and format
  version.
- **Requirements:** all 8 `assert-*` types are implemented as
  `table.Requirement` (`Validate(Metadata)`), with JSON decoding.
- **Builder:** `table.MetadataBuilderFromBase(metadata, location)` plus
  `Build()` cover the server-side commit flow (load → validate → apply →
  build). `NewMetadataBuilder` covers create-table.
- **Views:** a `view` package with its own metadata builder and updates.
- **Cost:** a heavy dependency. Its `go.mod` pulls in Arrow and several cloud
  SDKs, and it targets Go 1.25.

**Decision for Phase 1:** use `iceberg-go`'s `table` and `view` packages for
metadata parsing, requirement validation and update application. Use the
generated `gen` types only at the HTTP boundary. Check the binary size
and build time once it's in. If they are a problem, the fallback is to vendor
just the metadata model, which is pure Go and has no Arrow dependency.

## Consequences

- Upgrading the spec: bump `SPEC_COMMIT` in the Makefile, then run
  `make spec-update generate`, review the diff, and fix the overlay if strict
  mode complains.
- Phase 0 handlers return `501 UnsupportedOperationException` for every
  operation, through the strict server's response-error hook, so each Phase 1
  operation is a self-contained change.
- Routes are `/v1/{prefix}/...` only, as in the spec. Clients add a prefix only
  when `GET /v1/config` returns one, so the Phase 1 config endpoint **must**
  return `overrides.prefix`. Otherwise clients call `/v1/namespaces` and get
  404.
