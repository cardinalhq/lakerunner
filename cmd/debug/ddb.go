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
	"os"
	"path/filepath"
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
	s3db, err := duckdbx.NewS3DB()
	if err != nil {
		return err
	}
	defer func() {
		_ = s3db.Close()
		// Clean up the temp database file
		if dbPath := s3db.GetDatabasePath(); dbPath != "" {
			_ = os.RemoveAll(filepath.Dir(dbPath))
		}
	}()

	c, release, err := s3db.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer release()

	rows, err := c.QueryContext(ctx, "SELECT extension_name, loaded, installed, extension_version, install_mode FROM duckdb_extensions();")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

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
				// Special handling for loaded/installed columns to show availability
				if i == 1 { // loaded column
					loaded := fmt.Sprintf("%v", val) == "true"
					if loaded {
						row[i] = "loaded"
					} else {
						// Check if installed (column 2)
						if i+1 < len(values) && fmt.Sprintf("%v", values[i+1]) == "true" {
							row[i] = "available"
						} else {
							row[i] = "not available"
						}
					}
				} else if i == 2 { // installed column - skip since we handle it in loaded column
					continue
				} else {
					row[i] = fmt.Sprintf("%v", val)
				}
			}
		}
		// Remove the installed column from the row since we merged it with loaded
		if len(row) > 2 {
			row = append(row[:2], row[3:]...)
		}
		allRows = append(allRows, row)
	}

	// Update column headers to remove installed and rename loaded to availability
	newCols := make([]string, 0, len(cols)-1)
	for i, col := range cols {
		if i == 1 { // loaded column
			newCols = append(newCols, "availability")
		} else if i == 2 { // installed column - skip
			continue
		} else {
			newCols = append(newCols, col)
		}
	}
	cols = newCols

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
	// Use temporary file-based S3DB pool
	s3db, err := duckdbx.NewS3DB()
	if err != nil {
		return err
	}
	defer func() {
		_ = s3db.Close()
		// Clean up the temp database file
		if dbPath := s3db.GetDatabasePath(); dbPath != "" {
			_ = os.RemoveAll(filepath.Dir(dbPath))
		}
	}()

	c, release, err := s3db.GetConnection(ctx)
	if err != nil {
		return err
	}
	defer release()

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

	// Show LAKERUNNER environment variables
	extensionsPath := os.Getenv("LAKERUNNER_EXTENSIONS_PATH")
	httpfsPath := os.Getenv("LAKERUNNER_HTTPFS_EXTENSION")

	// Only show environment variables section if any are set
	if extensionsPath != "" || httpfsPath != "" {
		fmt.Println("\nLAKERUNNER Environment Variables:")
		if extensionsPath != "" {
			fmt.Printf("  LAKERUNNER_EXTENSIONS_PATH: %s\n", extensionsPath)
		}
		if httpfsPath != "" {
			fmt.Printf("  LAKERUNNER_HTTPFS_EXTENSION: %s\n", httpfsPath)
		}
	}

	// Show mode
	fmt.Println("\nMode:")
	if extensionsPath != "" {
		fmt.Println("  Air-gapped (extensions loaded from local files)")
	} else {
		fmt.Println("  Not air-gapped, network access available")
	}

	// Check expected extensions availability only in air-gapped mode
	if extensionsPath != "" {
		fmt.Println("\nExpected Extensions:")
		expectedExtensions := []string{"httpfs"}

		for _, extName := range expectedExtensions {
			fmt.Printf("  %s: ", extName)

			// Air-gapped mode: check file existence
			specificEnvVar := fmt.Sprintf("LAKERUNNER_%s_EXTENSION", strings.ToUpper(extName))
			extPath := os.Getenv(specificEnvVar)
			if extPath == "" {
				extPath = filepath.Join(extensionsPath, extName+".duckdb_extension")
			}

			if _, err := os.Stat(extPath); err == nil {
				fmt.Println("available")
			} else {
				fmt.Println("NOT FOUND")
			}
		}
	}

	return nil
}
