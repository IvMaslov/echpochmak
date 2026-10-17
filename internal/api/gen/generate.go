// Package gen contains server code generated from the Iceberg REST catalog
// OpenAPI spec. Do not edit the generated files by hand; run `make generate`.
package gen

//go:generate go run ../../tools/applyoverlay -spec ../../../api/iceberg/rest-catalog-open-api.yaml -overlay ../../../api/iceberg/overlay.yaml -out openapi.gen.yaml
//go:generate go tool oapi-codegen -config ../../../api/iceberg/oapi-codegen.yaml openapi.gen.yaml
