// Command kv-node runs one node of the raft-replicated key-value cluster that
// stores the catalog state.
//
// It is a placeholder until Phase 2; see arch/plan.md.
package main

import (
	"fmt"
	"os"

	"echpochmak/internal/version"
)

func main() {
	v := version.Get()
	fmt.Fprintf(
		os.Stdout,
		"kv-node %s (commit %s): not implemented yet, see arch/plan.md phase 2\n",
		v.Version,
		v.Commit,
	)
}
