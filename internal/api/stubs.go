package api

import (
	"context"

	"echpochmak/internal/api/gen"
)

// Operations of the Iceberg REST spec that are not implemented yet return
// errNotImplemented. They are not advertised in /v1/config, so clients do not
// call them.

// GetToken implements [gen.StrictServerInterface].
func (*Server) GetToken(context.Context, gen.GetTokenRequestObject) (gen.GetTokenResponseObject, error) {
	return nil, errNotImplemented
}

// ListFunctions implements [gen.StrictServerInterface].
func (*Server) ListFunctions(
	context.Context,
	gen.ListFunctionsRequestObject,
) (gen.ListFunctionsResponseObject, error) {
	return nil, errNotImplemented
}

// LoadFunction implements [gen.StrictServerInterface].
func (*Server) LoadFunction(
	context.Context,
	gen.LoadFunctionRequestObject,
) (gen.LoadFunctionResponseObject, error) {
	return nil, errNotImplemented
}

// LoadCredentials implements [gen.StrictServerInterface].
func (*Server) LoadCredentials(
	context.Context,
	gen.LoadCredentialsRequestObject,
) (gen.LoadCredentialsResponseObject, error) {
	return nil, errNotImplemented
}

// PlanTableScan implements [gen.StrictServerInterface].
func (*Server) PlanTableScan(
	context.Context,
	gen.PlanTableScanRequestObject,
) (gen.PlanTableScanResponseObject, error) {
	return nil, errNotImplemented
}

// CancelPlanning implements [gen.StrictServerInterface].
func (*Server) CancelPlanning(
	context.Context,
	gen.CancelPlanningRequestObject,
) (gen.CancelPlanningResponseObject, error) {
	return nil, errNotImplemented
}

// FetchPlanningResult implements [gen.StrictServerInterface].
func (*Server) FetchPlanningResult(
	context.Context,
	gen.FetchPlanningResultRequestObject,
) (gen.FetchPlanningResultResponseObject, error) {
	return nil, errNotImplemented
}

// SignRequest implements [gen.StrictServerInterface].
func (*Server) SignRequest(context.Context, gen.SignRequestRequestObject) (gen.SignRequestResponseObject, error) {
	return nil, errNotImplemented
}

// FetchScanTasks implements [gen.StrictServerInterface].
func (*Server) FetchScanTasks(
	context.Context,
	gen.FetchScanTasksRequestObject,
) (gen.FetchScanTasksResponseObject, error) {
	return nil, errNotImplemented
}
