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
	"errors"
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
	var recordsPerFile int64
	var targetFileSize int64

	cmd := &cobra.Command{
		Use:   "compact",
		Short: "Debug commands for compaction",
	}

	metricsCmd := &cobra.Command{
		Use:   "metrics",
		Short: "Debug metrics compaction processing",
		RunE: func(_ *cobra.Command, _ []string) error {
			return debugCompactMetrics(tmpdir, files, recordsPerFile, targetFileSize)
		},
	}

	metricsCmd.Flags().StringVar(&tmpdir, "tmpdir", "/tmp", "Temporary directory for processing")
	metricsCmd.Flags().StringArrayVar(&files, "file", nil, "Input files to compact (can be repeated)")
	metricsCmd.Flags().Int64Var(&recordsPerFile, "records-per-file", 10000, "Estimated records per file")
	metricsCmd.Flags().Int64Var(&targetFileSize, "target-size", 1_100_000, "Target file size in bytes")
	_ = metricsCmd.MarkFlagRequired("file")

	cmd.AddCommand(metricsCmd)
	debugCmd.AddCommand(cmd)
}

func debugCompactMetrics(tmpdir string, files []string, recordsPerFile int64, targetFileSize int64) error {
	ll := slog.Default()
	ctx := context.Background()

	ll.Info("Debug metrics compaction",
		slog.String("tmpdir", tmpdir),
		slog.Int("fileCount", len(files)),
		slog.Int64("recordsPerFile", recordsPerFile),
		slog.Int64("targetFileSize", targetFileSize))

	// Create readers for all input files
	var readers []filereader.Reader
	var downloadedFiles []string

	for _, filename := range files {
		ll.Info("Opening file", slog.String("filename", filename))

		reader, err := filereader.ReaderForFile(filename, filereader.SignalTypeMetrics)
		if err != nil {
			ll.Error("Failed to create reader", slog.String("filename", filename), slog.Any("error", err))
			return fmt.Errorf("creating reader for %s: %w", filename, err)
		}

		readers = append(readers, reader)
		downloadedFiles = append(downloadedFiles, filename)
	}

	if len(readers) == 0 {
		return fmt.Errorf("no valid input files")
	}

	defer func() {
		for _, reader := range readers {
			if err := reader.Close(); err != nil {
				ll.Error("Failed to close reader", slog.Any("error", err))
			}
		}
	}()

	// Create PreorderedMultisourceReader to merge sort the files using metrics ordering
	orderedReader, err := filereader.NewPreorderedMultisourceReader(readers, metricsprocessing.MetricsOrderedSelector())
	if err != nil {
		ll.Error("Failed to create ordered reader", slog.Any("error", err))
		return fmt.Errorf("creating ordered reader: %w", err)
	}
	defer orderedReader.Close()

	// Create metrics writer using the factory
	baseName := fmt.Sprintf("debug_compacted_metrics_%d", time.Now().Unix())
	writer, err := factories.NewMetricsWriter(baseName, tmpdir, targetFileSize, recordsPerFile)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return fmt.Errorf("creating metrics writer: %w", err)
	}
	defer func() {
		if _, err := writer.Close(ctx); err != nil {
			ll.Error("Failed to close writer", slog.Any("error", err))
		}
	}()

	// Process all data through the ordered reader and writer
	const batchSize = 1000
	rowsBatch := make([]filereader.Row, batchSize)
	totalRows := int64(0)
	groupStats := make(map[string]int64) // Track rows per group

	for {
		n, err := orderedReader.Read(rowsBatch)
		if err != nil && !errors.Is(err, io.EOF) {
			ll.Error("Failed to read from ordered reader", slog.Any("error", err))
			return fmt.Errorf("reading from ordered reader: %w", err)
		}

		if n == 0 {
			break
		}

		// Write batch to metrics writer and collect group stats
		for i := range n {
			row := rowsBatch[i]

			// Track group statistics
			if name, nameOk := row["_cardinalhq.name"].(string); nameOk {
				if tid, tidOk := row["_cardinalhq.tid"].(int64); tidOk {
					groupKey := fmt.Sprintf("%s:%d", name, tid)
					groupStats[groupKey]++
				}
			}

			if err := writer.Write(row); err != nil {
				ll.Error("Failed to write row", slog.Any("error", err))
				return fmt.Errorf("writing row: %w", err)
			}
			totalRows++
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	// Finish writing and get results
	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return fmt.Errorf("finishing writer: %w", err)
	}

	// Print detailed analysis
	ll.Info("Compaction completed",
		slog.Int64("totalRows", totalRows),
		slog.Int("outputFiles", len(results)),
		slog.Int("inputFiles", len(downloadedFiles)),
		slog.Int("uniqueGroups", len(groupStats)))

	fmt.Printf("\n=== COMPACTION ANALYSIS ===\n")
	fmt.Printf("Input files: %d\n", len(downloadedFiles))
	fmt.Printf("Output files: %d\n", len(results))
	fmt.Printf("Total rows: %d\n", totalRows)
	fmt.Printf("Unique [metric,TID] groups: %d\n", len(groupStats))
	fmt.Printf("Average rows per group: %.1f\n", float64(totalRows)/float64(len(groupStats)))
	fmt.Printf("Target file size: %d bytes\n", targetFileSize)
	fmt.Printf("Records per file estimate: %d\n", recordsPerFile)

	// Show file results
	fmt.Printf("\n=== OUTPUT FILES ===\n")
	var totalOutputSize int64
	for i, result := range results {
		fmt.Printf("File %d: %s\n", i+1, result.FileName)
		fmt.Printf("  Records: %d\n", result.RecordCount)
		fmt.Printf("  Size: %d bytes (%.1f%% of target)\n", result.FileSize, float64(result.FileSize)*100/float64(targetFileSize))
		totalOutputSize += result.FileSize

		if stats, ok := result.Metadata.(factories.MetricsFileStats); ok {
			fmt.Printf("  Time range: %s to %s\n",
				time.Unix(stats.FirstTS/1000, 0).UTC().Format("15:04:05"),
				time.Unix(stats.LastTS/1000, 0).UTC().Format("15:04:05"))
			fmt.Printf("  Fingerprints: %d\n", len(stats.Fingerprints))
		}
	}

	// Show group distribution (top 10 largest groups)
	fmt.Printf("\n=== TOP 10 LARGEST GROUPS ===\n")
	type groupInfo struct {
		key   string
		count int64
	}
	var sortedGroups []groupInfo
	for key, count := range groupStats {
		sortedGroups = append(sortedGroups, groupInfo{key, count})
	}

	// Simple sort by count (descending)
	for i := range sortedGroups {
		for j := i + 1; j < len(sortedGroups); j++ {
			if sortedGroups[j].count > sortedGroups[i].count {
				sortedGroups[i], sortedGroups[j] = sortedGroups[j], sortedGroups[i]
			}
		}
	}

	limit := min(10, len(sortedGroups))
	for i := range limit {
		group := sortedGroups[i]
		fmt.Printf("%d. %s: %d rows\n", i+1, group.key, group.count)
	}

	fmt.Printf("\nTotal output size: %d bytes\n", totalOutputSize)

	return nil
}
