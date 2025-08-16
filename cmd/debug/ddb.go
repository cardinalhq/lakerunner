// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package debug

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

func GetDDBCmd() *cobra.Command {
	ddbCmd := &cobra.Command{
		Use:   "ddb",
		Short: "DuckDB debugging commands",
	}

	ddbCmd.AddCommand(getExtensionsCmd())
	ddbCmd.AddCommand(getVersionCmd())

	return ddbCmd
}

func getExtensionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "extensions",
		Short: "List DuckDB extensions and their status",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runExtensions(cmd.Context())
		},
	}
}

func getVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Check DuckDB version and validate it matches expected version",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runVersion(cmd.Context())
		},
	}
}

// runExtensions lists DuckDB extensions and their status.
func runExtensions(ctx context.Context) error {
	ddb, err := duckdbx.Open("",
		duckdbx.WithMemoryLimitMB(2048),
	)
	if err != nil {
		return err
	}
	defer ddb.Close()

	c, err := ddb.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	rows, err := c.QueryContext(ctx, "SELECT extension_name, loaded, installed, install_path, extension_version, install_mode, installed_from FROM duckdb_extensions();")
	if err != nil {
		return err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	// Collect all data first to calculate column widths
	var allRows [][]string
	values := make([]any, len(cols))
	scanArgs := make([]any, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		row := make([]string, len(cols))
		for i, val := range values {
			if val == nil {
				row[i] = "<NULL>"
			} else {
				row[i] = fmt.Sprintf("%v", val)
			}
		}
		allRows = append(allRows, row)
	}

	// Calculate column widths
	colWidths := make([]int, len(cols))
	for i, col := range cols {
		colWidths[i] = len(col)
	}
	for _, row := range allRows {
		for i, cell := range row {
			if len(cell) > colWidths[i] {
				colWidths[i] = len(cell)
			}
		}
	}

	// Print header
	fmt.Print("┌")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┐")

	fmt.Print("│")
	for i, col := range cols {
		if i > 0 {
			fmt.Print("│")
		}
		fmt.Printf(" %-*s ", colWidths[i], col)
	}
	fmt.Println("│")

	// Print separator
	fmt.Print("├")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┤")

	// Print rows
	for _, row := range allRows {
		fmt.Print("│")
		for i, cell := range row {
			if i > 0 {
				fmt.Print("│")
			}
			fmt.Printf(" %-*s ", colWidths[i], cell)
		}
		fmt.Println("│")
	}

	// Print bottom border
	fmt.Print("└")
	for i, width := range colWidths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", width+2))
	}
	fmt.Println("┘")

	return nil
}

// runVersion checks DuckDB version and validates it matches expected version.
func runVersion(ctx context.Context) error {
	ddb, err := duckdbx.Open("",
		duckdbx.WithMemoryLimitMB(2048),
	)
	if err != nil {
		return err
	}
	defer ddb.Close()

	c, err := ddb.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	// Get DuckDB version
	var version string
	err = c.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return fmt.Errorf("failed to get DuckDB version: %w", err)
	}

	fmt.Printf("DuckDB version: %s\n", version)

	// Expected version - this should match the version in go.mod or build args
	expectedVersion := "v1.3.2"

	// Check if the running version contains the expected version
	// DuckDB version string is like "v1.3.2 af7bcaf"
	if !strings.Contains(version, expectedVersion) {
		return fmt.Errorf("DuckDB version mismatch: expected %s, got %s", expectedVersion, version)
	}

	return nil
}
