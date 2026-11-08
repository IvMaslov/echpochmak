package main

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// printTable writes rows as JSON, or as a table: the header, then one line
// of cells per row.
func printTable[T any](cmd *command, rows []T, header []string, cells func(T) []string) error {
	return cmd.printRows(rows, func(w *tabwriter.Writer) {
		fmt.Fprintln(w, strings.Join(header, "\t"))

		for _, r := range rows {
			fmt.Fprintln(w, strings.Join(cells(r), "\t"))
		}
	})
}
