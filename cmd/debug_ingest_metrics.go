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
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
)

func init() {
	var tmpdir string
	var files []string

	cmd := &cobra.Command{
		Use:   "ingest",
		Short: "Debug commands for ingestion",
	}

	metricsCmd := &cobra.Command{
		Use:   "metrics",
		Short: "Debug metrics ingestion processing",
		RunE: func(_ *cobra.Command, _ []string) error {
			return debugIngestMetrics(tmpdir, files)
		},
	}

	metricsCmd.Flags().StringVar(&tmpdir, "tmpdir", "/tmp", "Temporary directory for processing")
	metricsCmd.Flags().StringArrayVar(&files, "file", nil, "Input files to process (can be repeated)")
	_ = metricsCmd.MarkFlagRequired("file")

	cmd.AddCommand(metricsCmd)
	debugCmd.AddCommand(cmd)
}

func debugIngestMetrics(tmpdir string, files []string) error {
	ll := slog.Default()
	ctx := context.Background()

	ll.Info("Debug metrics ingestion",
		slog.String("tmpdir", tmpdir),
		slog.Int("fileCount", len(files)))

	// Create writer manager (similar to real ingestion)
	wm := newMetricWriterManager(tmpdir, "debug-org-id", int32(20250824), 100, ll)

	var totalRowsRead, totalRowsProcessed, totalRowsErrored int64

	// Process each file
	for _, filename := range files {
		ll.Info("Processing file", slog.String("filename", filename))

		// Create reader
		reader, err := createMetricProtoReader(filename)
		if err != nil {
			ll.Error("Failed to create reader", slog.String("filename", filename), slog.Any("error", err))
			continue
		}

		// Add translator (similar to real ingestion)
		translator := &metricsprocessing.MetricTranslator{
			OrgID:    "debug-org-id",
			Bucket:   "debug-bucket",
			ObjectID: filename,
		}
		reader, err = filereader.NewTranslatingReader(reader, translator)
		if err != nil {
			ll.Error("Failed to create translating reader", slog.Any("error", err))
			continue
		}

		// Process all rows from the file
		rows := make([]filereader.Row, 100)
		for i := range rows {
			rows[i] = make(filereader.Row)
		}

		var fileRowsProcessed, fileRowsErrored int64
		for {
			n, err := reader.Read(rows)

			// Process any rows we got, even if EOF
			for i := range n {
				if rows[i] == nil {
					ll.Error("Row is nil - skipping", slog.Int("rowIndex", i))
					continue
				}

				err := wm.processRow(rows[i])
				if err != nil {
					fileRowsErrored++
					ll.Error("Failed to process row",
						slog.Int64("rowNumber", fileRowsProcessed+int64(i)+1),
						slog.String("error", err.Error()),
						slog.Any("rowData", rows[i]))
				} else {
					fileRowsProcessed++
				}
			}

			// Break after processing if we hit EOF or other errors
			if err == io.EOF {
				break
			}
			if err != nil {
				if closeErr := reader.Close(); closeErr != nil {
					ll.Warn("Failed to close reader after read error", slog.Any("error", closeErr))
				}
				return fmt.Errorf("failed to read from file %s: %w", filename, err)
			}
		}

		fileRowsRead := reader.TotalRowsReturned()
		ll.Info("File processing completed",
			slog.String("filename", filename),
			slog.Int64("rowsRead", fileRowsRead),
			slog.Int64("rowsProcessed", fileRowsProcessed),
			slog.Int64("rowsErrored", fileRowsErrored))

		if closeErr := reader.Close(); closeErr != nil {
			ll.Warn("Failed to close reader", slog.Any("error", closeErr))
		}

		totalRowsRead += fileRowsRead
		totalRowsProcessed += fileRowsProcessed
		totalRowsErrored += fileRowsErrored
	}

	// Close all writers and get results
	results, err := wm.closeAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to close writers: %w", err)
	}

	// Print results for debugging
	ll.Info("Processing summary",
		slog.Int64("totalRowsRead", totalRowsRead),
		slog.Int64("totalRowsProcessed", totalRowsProcessed),
		slog.Int64("totalRowsErrored", totalRowsErrored),
		slog.Int("outputFiles", len(results)))

	fmt.Printf("\n=== DEBUG RESULTS ===\n")
	for i, result := range results {
		fmt.Printf("Result %d:\n", i)
		fmt.Printf("  FileName: %s\n", result.FileName)
		fmt.Printf("  RecordCount: %d\n", result.RecordCount)
		fmt.Printf("  FileSize: %d\n", result.FileSize)

		if stats, ok := result.Metadata.(factories.MetricsFileStats); ok {
			fmt.Printf("  MetricsFileStats:\n")
			fmt.Printf("    FirstTS: %d (%s)\n", stats.FirstTS, time.Unix(stats.FirstTS/1000, 0).UTC().Format(time.RFC3339))
			fmt.Printf("    LastTS: %d (%s)\n", stats.LastTS, time.Unix(stats.LastTS/1000, 0).UTC().Format(time.RFC3339))
			fmt.Printf("    Fingerprints: %d fingerprints\n", len(stats.Fingerprints))
			if len(stats.Fingerprints) > 0 {
				fmt.Printf("    First few fingerprints: %v\n", stats.Fingerprints[:min(3, len(stats.Fingerprints))])
			}
		} else {
			fmt.Printf("  Metadata: %T = %+v\n", result.Metadata, result.Metadata)
		}
		fmt.Printf("\n")
	}

	if len(results) == 0 {
		fmt.Printf("No output files generated!\n")
	}

	return nil
}
