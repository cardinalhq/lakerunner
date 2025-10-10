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
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/pipeline"
)

func GetIngestLogsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest-logs <filename>",
		Short: "Test log ingestion pipeline on a file",
		Long: `Test the log ingestion pipeline on a file (CSV, JSON, Parquet, etc.) and output the resulting Parquet file in the current directory.

This command uses the same code path as the production log ingestion system, including:
- File type detection and reader creation
- CSV log translation (for CSV files)
- Standard log translation (for other formats)
- Parquet writing with the logs schema

The output Parquet file will be written to the current directory and can be examined using other debug commands.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			filename := args[0]

			orgID, err := cmd.Flags().GetString("org-id")
			if err != nil {
				return fmt.Errorf("failed to get org-id flag: %w", err)
			}

			bucket, err := cmd.Flags().GetString("bucket")
			if err != nil {
				return fmt.Errorf("failed to get bucket flag: %w", err)
			}

			outputDir, err := cmd.Flags().GetString("output-dir")
			if err != nil {
				return fmt.Errorf("failed to get output-dir flag: %w", err)
			}

			return runIngestLogs(filename, orgID, bucket, outputDir)
		},
	}

	cmd.Flags().String("org-id", "debug-org", "Organization ID to use for ingestion")
	cmd.Flags().String("bucket", "debug-bucket", "Bucket name to use for resource metadata")
	cmd.Flags().String("output-dir", ".", "Directory to write output Parquet file")

	return cmd
}

func runIngestLogs(filename, orgID, bucket, outputDir string) error {
	ctx := context.Background()

	// Validate input file exists
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", filename)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Extract base filename for object ID
	objectID := filepath.Base(filename)

	// Create log reader stack using the same logic as the production system
	reader, err := createLogReaderStack(filename, orgID, bucket, objectID)
	if err != nil {
		return fmt.Errorf("failed to create log reader stack: %w", err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			slog.Warn("Failed to close reader", slog.Any("error", closeErr))
		}
	}()

	// Create parquet writer using the same factory as production
	writer, err := factories.NewLogsWriter(outputDir, 10000)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	totalRows := int64(0)
	batchCount := 0

	// Process batches using the same flow as production
	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("failed to read batch: %w", err)
		}

		batchCount++
		fmt.Printf("Processing batch %d with %d rows\n", batchCount, batch.Len())

		// Write batch to parquet
		if err := writer.WriteBatch(batch); err != nil {
			pipeline.ReturnBatch(batch)
			return fmt.Errorf("failed to write batch to parquet: %w", err)
		}

		totalRows += int64(batch.Len())
		pipeline.ReturnBatch(batch)
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// Print results
	fmt.Printf("\nIngestion complete:\n")
	fmt.Printf("  Total rows processed: %d\n", totalRows)
	fmt.Printf("  Generated files:\n")
	for _, result := range results {
		fmt.Printf("    %s (%d rows, %d bytes)\n", result.FileName, result.RecordCount, result.FileSize)
	}

	return nil
}

// createLogReaderStack creates a reader stack using the same logic as LogIngestProcessor.createLogReaderStack
func createLogReaderStack(filename, orgID, bucket, objectID string) (filereader.Reader, error) {
	// Create base log reader
	reader, err := createLogReader(filename, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to create log reader: %w", err)
	}

	// Choose translator based on file type (same logic as production)
	var translator filereader.RowTranslator
	if strings.HasSuffix(filename, ".parquet") {
		// Use specialized Parquet translator for .parquet files
		translator = metricsprocessing.NewParquetLogTranslator(orgID, bucket, objectID)
	} else {
		// Use standard translator for CSV, JSON, and other formats
		translator = metricsprocessing.NewLogTranslator(orgID, bucket, objectID, nil)
	}

	// Wrap with translating reader
	reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to create translating reader: %w", err)
	}

	return reader, nil
}

// createLogReader creates a log reader using the same logic as LogIngestProcessor.createLogReader
func createLogReader(filename, orgID string) (filereader.Reader, error) {
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      orgID,
	}

	return filereader.ReaderForFileWithOptions(filename, options)
}
