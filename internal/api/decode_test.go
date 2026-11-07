package api

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"echpochmak/internal/api/gen"
)

// TestDecodeAllCommitTypes checks that every requirement and update type of
// the spec decodes from a client request, with its fields intact.
func TestDecodeAllCommitTypes(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("testdata/commit-all-types.json")
	if err != nil {
		t.Fatal(err)
	}

	var body commitTableBody
	if err := decodeRawBody(context.WithValue(t.Context(), rawBodyKey{}, raw), &body); err != nil {
		t.Fatalf("decoding: %v", err)
	}

	var want struct {
		Requirements []struct {
			Type string `json:"type"`
		} `json:"requirements"`
		Updates []struct {
			Action string `json:"action"`
		} `json:"updates"`
	}
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}

	gotTypes := make([]string, 0, len(body.Requirements))
	for _, r := range body.Requirements {
		gotTypes = append(gotTypes, r.GetType())
	}

	wantTypes := make([]string, 0, len(want.Requirements))
	for _, r := range want.Requirements {
		wantTypes = append(wantTypes, r.Type)
	}

	if !slices.Equal(gotTypes, wantTypes) {
		t.Fatalf("requirements = %v, want %v", gotTypes, wantTypes)
	}

	gotActions := make([]string, 0, len(body.Updates))
	for _, u := range body.Updates {
		gotActions = append(gotActions, u.Action())
	}

	wantActions := make([]string, 0, len(want.Updates))
	for _, u := range want.Updates {
		wantActions = append(wantActions, u.Action)
	}

	if !slices.Equal(gotActions, wantActions) || len(gotActions) != 23 {
		t.Fatalf("updates = %v, want all 23 of %v", gotActions, wantActions)
	}

	// Fields survive: re-encoding a requirement keeps its payload.
	enc, err := json.Marshal(body.Requirements[2])
	if err != nil {
		t.Fatal(err)
	}

	var ref struct {
		Ref        string `json:"ref"`
		SnapshotID int64  `json:"snapshot-id"`
	}
	if err := json.Unmarshal(enc, &ref); err != nil || ref.Ref != "main" || ref.SnapshotID != 3055729675574597004 {
		t.Fatalf("assert-ref-snapshot-id re-encoded as %s", enc)
	}
}

// TestGeneratedRequirementLosesFields documents why table writes are decoded
// from the raw body: the generated TableRequirement keeps only the type. If
// this test starts failing, the generated types improved and the raw-body
// path could be reconsidered.
func TestGeneratedRequirementLosesFields(t *testing.T) {
	t.Parallel()

	var req gen.CommitTableRequest
	body := `{"requirements":[{"type":"assert-table-uuid","uuid":"x"}],"updates":[]}`
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		t.Fatal(err)
	}

	enc, err := json.Marshal(req.Requirements[0])
	if err != nil {
		t.Fatal(err)
	}

	if string(enc) != `{"type":"assert-table-uuid"}` {
		t.Fatalf("generated TableRequirement now keeps its fields: %s", enc)
	}
}
