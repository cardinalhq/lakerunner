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

package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

func init() {
	var tempDir string
	var files []string

	cmd := &cobra.Command{
		Use:   "test-local-files",
		Short: "Test log processing with local files to debug issues",
		RunE: func(_ *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("provide at least one file path to test")
			}
			files = args

			// Use provided tempdir or create one
			if tempDir == "" {
				var err error
				tempDir, err = os.MkdirTemp("", "test-local-files-")
				if err != nil {
					return fmt.Errorf("failed to create temp dir: %w", err)
				}
				defer os.RemoveAll(tempDir)
				fmt.Printf("Using temporary directory: %s\n", tempDir)
			}

			return testLocalFiles(files, tempDir)
		},
	}

	cmd.Flags().StringVar(&tempDir, "temp-dir", "", "Temporary directory for output files (will create if not specified)")
	rootCmd.AddCommand(cmd)
}

func testLocalFiles(filePaths []string, tempDir string) error {
	ctx := context.Background()
	ll := slog.Default()

	for _, filePath := range filePaths {
		fmt.Printf("\n=== Testing file: %s ===\n", filePath)

		// Extract objectID from file path for proper type detection
		objectID := filepath.Base(filePath)

		// Create reader
		reader, err := createLogReader(filePath)
		if err != nil {
			fmt.Printf("ERROR: Failed to create reader: %v\n", err)
			continue
		}

		// Add translator (same as production)
		translator := &LogTranslator{
			orgID:              "test-org",
			bucket:             "test-bucket",
			objectID:           objectID,
			trieClusterManager: fingerprinter.NewTrieClusterManager(0.5),
		}

		reader, err = filereader.NewTranslatingReader(reader, translator)
		if err != nil {
			reader.Close()
			fmt.Printf("ERROR: Failed to create translating reader: %v\n", err)
			continue
		}

		// Create writer manager (same as production)
		wm := newWriterManager(tempDir, "test-org", 20250824, 1000, ll)

		// Process rows (same loop as production)
		rows := make([]filereader.Row, 100)
		for i := range rows {
			rows[i] = make(filereader.Row)
		}

		var totalRowsRead, totalRowsProcessed, totalRowsErrored int64

		for {
			n, err := reader.Read(rows)

			// Process any rows we got, even if EOF

			for i := range n {
				if rows[i] == nil {
					fmt.Printf("ERROR: Row %d is nil\n", i)
					continue
				}

				totalRowsRead++
				if err := wm.processRow(rows[i]); err != nil {
					totalRowsErrored++
					fmt.Printf("ERROR processing row %d: %v\n", i, err)
				} else {
					totalRowsProcessed++
				}
			}

			// Break after processing if we hit EOF or other errors
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Printf("ERROR: Read failed: %v\n", err)
				break
			}
		}

		reader.Close()

		// Close writers and get results
		results, err := wm.closeAll(ctx)
		if err != nil {
			fmt.Printf("ERROR: Failed to close writers: %v\n", err)
			continue
		}

		// Summary
		fmt.Printf("Results:\n")
		fmt.Printf("  Rows read: %d\n", totalRowsRead)
		fmt.Printf("  Rows processed: %d\n", totalRowsProcessed)
		fmt.Printf("  Rows errored: %d\n", totalRowsErrored)
		fmt.Printf("  Output files: %d\n", len(results))

		for i, result := range results {
			fmt.Printf("  File %d: %s (%d records, %d bytes)\n",
				i, result.FileName, result.RecordCount, result.FileSize)
		}

		if len(results) == 0 {
			fmt.Printf("WARNING: No output files generated!\n")
		}
	}

	return nil
}
