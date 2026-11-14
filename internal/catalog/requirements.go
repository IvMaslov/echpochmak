package catalog

import (
	"encoding/json"
	"fmt"

	"github.com/apache/iceberg-go/table"
)

// requirementFields are the fields of every table requirement type, as sent
// by clients. iceberg-go keeps its requirement types unexported, so they are
// read back through their JSON.
type requirementFields struct {
	Type                    string `json:"type"`
	UUID                    string `json:"uuid"`
	Ref                     string `json:"ref"`
	SnapshotID              *int64 `json:"snapshot-id"`
	LastAssignedFieldID     int    `json:"last-assigned-field-id"`
	CurrentSchemaID         int    `json:"current-schema-id"`
	LastAssignedPartitionID int    `json:"last-assigned-partition-id"`
	DefaultSpecID           int    `json:"default-spec-id"`
	DefaultSortOrderID      int    `json:"default-sort-order-id"`
}

// requirementFailed returns the error for req failing against current: an
// [ErrRequirementFailed] whose message is the Java reference implementation's,
// starting with "Requirement failed:", since Java clients test for it. cause
// is iceberg-go's own error, used when the type is unknown here.
func requirementFailed(req table.Requirement, current table.Metadata, cause error) error {
	if msg, ok := requirementMessage(req, current); ok {
		return withMessage(ErrRequirementFailed, "Requirement failed: %s", msg)
	}

	return fmt.Errorf("%w: %s: %w", ErrRequirementFailed, req.GetType(), cause)
}

func requirementMessage(req table.Requirement, current table.Metadata) (string, bool) {
	raw, err := json.Marshal(req)
	if err != nil {
		return "", false
	}

	var f requirementFields
	if err := json.Unmarshal(raw, &f); err != nil {
		return "", false
	}

	if f.Type == requirementAssertCreate {
		return "table already exists", true
	}

	if current == nil {
		return "table does not exist", true
	}

	switch f.Type {
	case "assert-table-uuid":
		return fmt.Sprintf("UUID does not match: expected %s != %s", current.TableUUID(), f.UUID), true
	case "assert-ref-snapshot-id":
		return refMessage(f, current), true
	case "assert-last-assigned-field-id":
		return fmt.Sprintf("last assigned field id changed: expected id %d != %d",
			f.LastAssignedFieldID, current.LastColumnID()), true
	case "assert-current-schema-id":
		return fmt.Sprintf("current schema changed: expected id %d != %d",
			f.CurrentSchemaID, current.CurrentSchema().ID), true
	case "assert-last-assigned-partition-id":
		last := 0
		if id := current.LastPartitionSpecID(); id != nil {
			last = *id
		}

		return fmt.Sprintf("last assigned partition id changed: expected id %d != %d",
			f.LastAssignedPartitionID, last), true
	case "assert-default-spec-id":
		return fmt.Sprintf("default partition spec changed: expected id %d != %d",
			f.DefaultSpecID, current.DefaultPartitionSpec()), true
	case "assert-default-sort-order-id":
		return fmt.Sprintf("default sort order changed: expected id %d != %d",
			f.DefaultSortOrderID, current.DefaultSortOrder()), true
	default:
		return "", false
	}
}

func refMessage(f requirementFields, current table.Metadata) string {
	var ref *table.SnapshotRef

	for name, r := range current.Refs() {
		if name == f.Ref {
			ref = &r

			break
		}
	}

	switch {
	case ref == nil:
		return fmt.Sprintf("branch or tag %s is missing, expected %d", f.Ref, derefOr(f.SnapshotID, 0))
	case f.SnapshotID == nil:
		return fmt.Sprintf("%s %s was created concurrently", ref.SnapshotRefType, f.Ref)
	default:
		return fmt.Sprintf("%s %s has changed: expected id %d != %d",
			ref.SnapshotRefType, f.Ref, *f.SnapshotID, ref.SnapshotID)
	}
}

func derefOr(p *int64, def int64) int64 {
	if p == nil {
		return def
	}

	return *p
}
