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

// RegisterTable implements [gen.StrictServerInterface].
func (*Server) RegisterTable(
	context.Context,
	gen.RegisterTableRequestObject,
) (gen.RegisterTableResponseObject, error) {
	return nil, errNotImplemented
}

// RegisterView implements [gen.StrictServerInterface].
func (*Server) RegisterView(
	context.Context,
	gen.RegisterViewRequestObject,
) (gen.RegisterViewResponseObject, error) {
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

// UnregisterTable implements [gen.StrictServerInterface].
func (*Server) UnregisterTable(
	context.Context,
	gen.UnregisterTableRequestObject,
) (gen.UnregisterTableResponseObject, error) {
	return nil, errNotImplemented
}

// ListViews implements [gen.StrictServerInterface].
func (*Server) ListViews(context.Context, gen.ListViewsRequestObject) (gen.ListViewsResponseObject, error) {
	return nil, errNotImplemented
}

// CreateView implements [gen.StrictServerInterface].
func (*Server) CreateView(context.Context, gen.CreateViewRequestObject) (gen.CreateViewResponseObject, error) {
	return nil, errNotImplemented
}

// DropView implements [gen.StrictServerInterface].
func (*Server) DropView(context.Context, gen.DropViewRequestObject) (gen.DropViewResponseObject, error) {
	return nil, errNotImplemented
}

// LoadView implements [gen.StrictServerInterface].
func (*Server) LoadView(context.Context, gen.LoadViewRequestObject) (gen.LoadViewResponseObject, error) {
	return nil, errNotImplemented
}

// ViewExists implements [gen.StrictServerInterface].
func (*Server) ViewExists(context.Context, gen.ViewExistsRequestObject) (gen.ViewExistsResponseObject, error) {
	return nil, errNotImplemented
}

// ReplaceView implements [gen.StrictServerInterface].
func (*Server) ReplaceView(context.Context, gen.ReplaceViewRequestObject) (gen.ReplaceViewResponseObject, error) {
	return nil, errNotImplemented
}

// CommitTransaction implements [gen.StrictServerInterface].
func (*Server) CommitTransaction(
	context.Context,
	gen.CommitTransactionRequestObject,
) (gen.CommitTransactionResponseObject, error) {
	return nil, errNotImplemented
}

// RenameView implements [gen.StrictServerInterface].
func (*Server) RenameView(context.Context, gen.RenameViewRequestObject) (gen.RenameViewResponseObject, error) {
	return nil, errNotImplemented
}
