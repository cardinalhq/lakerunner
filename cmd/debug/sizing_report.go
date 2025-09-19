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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

// FileReport represents sizing information for a single Parquet file
type FileReport struct {
	Filename           string
	Records            int64
	ParquetSize        int64
	UncompressedJSON   int64
	CompressedJSON     int64
	BytesPerRecordPQ   float64
	BytesPerRecordJSON float64
	BytesPerRecordGZ   float64
	PQvsJSON           float64 // Reduction percent
	PQvsGZ             float64 // Reduction percent
}

// SummaryReport represents the aggregate sizing information
type SummaryReport struct {
	TotalFiles         int
	TotalRecords       int64
	TotalParquetSize   int64
	TotalJSONSize      int64
	TotalGZSize        int64
	AvgBytesPerRecPQ   float64
	AvgBytesPerRecJSON float64
	AvgBytesPerRecGZ   float64
	OverallPQvsJSON    float64
	OverallPQvsGZ      float64
}

func GetSizingReportCmd() *cobra.Command {
	var (
		detailed bool
		sample   int
	)

	cmd := &cobra.Command{
		Use:   "sizing-report DIR",
		Short: "Generate size comparison report for Parquet files",
		Long:  `Analyze Parquet files to compare their size versus JSON and compressed JSON formats.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			return runSizingReport(cmd.Context(), dir, detailed, sample)
		},
	}

	cmd.Flags().BoolVar(&detailed, "detailed", false, "Show detailed per-file statistics")
	cmd.Flags().IntVar(&sample, "sample", 0, "Randomly sample N files (0 = process all files)")

	return cmd
}

func runSizingReport(ctx context.Context, dir string, detailed bool, sampleSize int) error {
	// Try to read metadata to get signal type
	var signalType string
	metadataPath := filepath.Join(dir, "metadata.json")
	if metadataFile, err := os.Open(metadataPath); err == nil {
		var metadata struct {
			Signal string `json:"signal"`
		}
		if err := json.NewDecoder(metadataFile).Decode(&metadata); err == nil {
			signalType = metadata.Signal
		}
		_ = metadataFile.Close()
	}

	// Find all parquet files
	parquetFiles, err := findParquetFiles(dir)
	if err != nil {
		return fmt.Errorf("failed to find parquet files: %w", err)
	}

	if len(parquetFiles) == 0 {
		return fmt.Errorf("no parquet files found in %s", dir)
	}

	// Apply sampling if requested
	originalCount := len(parquetFiles)
	if sampleSize > 0 && len(parquetFiles) > sampleSize {
		fmt.Printf("Found %d parquet files, randomly sampling %d\n", len(parquetFiles), sampleSize)
		parquetFiles = sampleFiles(parquetFiles, sampleSize)
	} else if sampleSize > 0 && len(parquetFiles) <= sampleSize {
		fmt.Printf("Found %d parquet files (less than or equal to sample size %d, processing all)\n", len(parquetFiles), sampleSize)
	} else {
		fmt.Printf("Found %d parquet files to analyze\n", len(parquetFiles))
	}

	// Determine number of workers (use all available CPUs)
	numWorkers := runtime.NumCPU()
	if len(parquetFiles) < numWorkers {
		numWorkers = len(parquetFiles)
	}
	fmt.Printf("Using %d parallel workers\n", numWorkers)
	if sampleSize > 0 && originalCount > len(parquetFiles) {
		fmt.Printf("Processing %d sampled files out of %d total files\n", len(parquetFiles), originalCount)
	}
	fmt.Printf("\n")

	// Create channels for work distribution
	type workItem struct {
		index int
		path  string
	}

	type result struct {
		index  int
		report *FileReport
		err    error
	}

	workChan := make(chan workItem, len(parquetFiles))
	resultChan := make(chan result, len(parquetFiles))

	// Start worker goroutines
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for work := range workChan {
				report, err := analyzeParquetFile(ctx, work.path)
				resultChan <- result{
					index:  work.index,
					report: report,
					err:    err,
				}
			}
		}(w)
	}

	// Queue all work items
	for i, filePath := range parquetFiles {
		workChan <- workItem{index: i, path: filePath}
	}
	close(workChan)

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	results := make([]result, 0, len(parquetFiles))
	processedCount := 0
	for res := range resultChan {
		results = append(results, res)
		processedCount++
		fmt.Printf("\rProcessed %d/%d files", processedCount, len(parquetFiles))
	}
	fmt.Printf("\n\n")

	// Sort results by original index to maintain order
	sort.Slice(results, func(i, j int) bool {
		return results[i].index < results[j].index
	})

	// Process results and calculate totals
	var reports []FileReport
	var totalRecords int64
	var totalParquetSize int64
	var totalJSONSize int64
	var totalGZSize int64

	for _, res := range results {
		if res.err != nil {
			fmt.Printf("Warning: Failed to analyze %s: %v\n", parquetFiles[res.index], res.err)
			continue
		}
		if res.report != nil {
			reports = append(reports, *res.report)
			totalRecords += res.report.Records
			totalParquetSize += res.report.ParquetSize
			totalJSONSize += res.report.UncompressedJSON
			totalGZSize += res.report.CompressedJSON
		}
	}

	// Generate summary
	summary := SummaryReport{
		TotalFiles:       len(reports),
		TotalRecords:     totalRecords,
		TotalParquetSize: totalParquetSize,
		TotalJSONSize:    totalJSONSize,
		TotalGZSize:      totalGZSize,
	}

	if totalRecords > 0 {
		summary.AvgBytesPerRecPQ = float64(totalParquetSize) / float64(totalRecords)
		summary.AvgBytesPerRecJSON = float64(totalJSONSize) / float64(totalRecords)
		summary.AvgBytesPerRecGZ = float64(totalGZSize) / float64(totalRecords)
	}

	if totalJSONSize > 0 {
		summary.OverallPQvsJSON = (1 - float64(totalParquetSize)/float64(totalJSONSize)) * 100
	}
	if totalGZSize > 0 {
		summary.OverallPQvsGZ = (1 - float64(totalParquetSize)/float64(totalGZSize)) * 100
	}

	// Print reports
	printSizingReport(reports, summary, detailed, signalType)

	return nil
}

func analyzeParquetFile(ctx context.Context, filePath string) (*FileReport, error) {
	// Get file size
	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	parquetSize := stat.Size()

	// Open the parquet file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer func() { _ = file.Close() }()

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[map[string]any](pf, pf.Schema())
	defer func() { _ = reader.Close() }()

	// Process rows and calculate JSON sizes
	var recordCount int64
	var jsonBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&jsonBuffer)
	encoder := json.NewEncoder(gzipWriter)

	// Also track uncompressed size
	var uncompressedSize int64

	batchSize := 1000
	for {
		rows := make([]map[string]any, batchSize)
		for i := range rows {
			rows[i] = make(map[string]any)
		}

		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("error reading parquet rows: %w", err)
		}

		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			row := rows[i]
			// Remove sketch field for metric data - it's not needed for size calculations
			delete(row, "sketch")

			// Remove all null entries from the map
			for key, value := range row {
				if value == nil {
					delete(row, key)
				}
			}

			// Calculate uncompressed JSON size
			jsonBytes, err := json.Marshal(row)
			if err != nil {
				return nil, fmt.Errorf("error marshaling row to JSON: %w", err)
			}
			// Add newline as we would in JSON lines format
			uncompressedSize += int64(len(jsonBytes)) + 1

			// Write to gzip
			if err := encoder.Encode(row); err != nil {
				return nil, fmt.Errorf("error encoding to gzip: %w", err)
			}

			recordCount++
		}

		if err == io.EOF {
			break
		}
	}

	// Close gzip writer to flush remaining data
	if err := gzipWriter.Close(); err != nil {
		return nil, fmt.Errorf("error closing gzip writer: %w", err)
	}

	compressedSize := int64(jsonBuffer.Len())

	report := &FileReport{
		Filename:         filepath.Base(filePath),
		Records:          recordCount,
		ParquetSize:      parquetSize,
		UncompressedJSON: uncompressedSize,
		CompressedJSON:   compressedSize,
	}

	// Calculate per-record sizes
	if recordCount > 0 {
		report.BytesPerRecordPQ = float64(parquetSize) / float64(recordCount)
		report.BytesPerRecordJSON = float64(uncompressedSize) / float64(recordCount)
		report.BytesPerRecordGZ = float64(compressedSize) / float64(recordCount)
	}

	// Calculate reduction percentages
	if uncompressedSize > 0 {
		report.PQvsJSON = (1 - float64(parquetSize)/float64(uncompressedSize)) * 100
	}
	if compressedSize > 0 {
		report.PQvsGZ = (1 - float64(parquetSize)/float64(compressedSize)) * 100
	}

	return report, nil
}

func printSizingReport(reports []FileReport, summary SummaryReport, detailed bool, signalType string) {
	fmt.Printf("%s\n", strings.Repeat("=", 100))
	fmt.Printf("PARQUET SIZING REPORT\n")
	fmt.Printf("%s\n\n", strings.Repeat("=", 100))

	// Print summary first
	fmt.Printf("SUMMARY\n")
	if signalType != "" {
		fmt.Printf("  Signal type: %s\n", signalType)
	}
	fmt.Printf("  Total files analyzed: %d\n", summary.TotalFiles)
	fmt.Printf("  Total records: %s\n", formatNumber(summary.TotalRecords))
	fmt.Printf("\n")

	// Combined metrics table with formats as columns
	fmt.Printf("SIZE COMPARISON\n")
	fmt.Printf("%-25s %15s %15s %15s\n", strings.Repeat("-", 25), strings.Repeat("-", 15), strings.Repeat("-", 15), strings.Repeat("-", 15))
	fmt.Printf("%-25s %15s %15s %15s\n", "Metric", "Parquet", "JSON", "JSON.gz")
	fmt.Printf("%-25s %15s %15s %15s\n", strings.Repeat("-", 25), strings.Repeat("-", 15), strings.Repeat("-", 15), strings.Repeat("-", 15))

	// Total sizes row
	fmt.Printf("%-25s %15s %15s %15s\n", "Total Size",
		formatBytes(summary.TotalParquetSize),
		formatBytes(summary.TotalJSONSize),
		formatBytes(summary.TotalGZSize))

	// Bytes per record row
	fmt.Printf("%-25s %15.1f %15.1f %15.1f\n", "Avg Bytes/Record",
		summary.AvgBytesPerRecPQ,
		summary.AvgBytesPerRecJSON,
		summary.AvgBytesPerRecGZ)

	fmt.Printf("%-25s %15s %15s %15s\n", strings.Repeat("-", 25), strings.Repeat("-", 15), strings.Repeat("-", 15), strings.Repeat("-", 15))
	fmt.Printf("\n")

	// Parquet space savings table
	fmt.Printf("PARQUET SPACE SAVINGS\n")
	fmt.Printf("%-25s %15s\n", strings.Repeat("-", 25), strings.Repeat("-", 15))
	fmt.Printf("%-25s %15s\n", "Comparison", "Reduction")
	fmt.Printf("%-25s %15s\n", strings.Repeat("-", 25), strings.Repeat("-", 15))
	if summary.OverallPQvsJSON >= 0 {
		fmt.Printf("%-25s %14.1f%%\n", "Parquet vs JSON", summary.OverallPQvsJSON)
	} else {
		fmt.Printf("%-25s %14.1f%% larger\n", "Parquet vs JSON", -summary.OverallPQvsJSON)
	}
	if summary.OverallPQvsGZ >= 0 {
		fmt.Printf("%-25s %14.1f%%\n", "Parquet vs JSON.gz", summary.OverallPQvsGZ)
	} else {
		fmt.Printf("%-25s %14.1f%% larger\n", "Parquet vs JSON.gz", -summary.OverallPQvsGZ)
	}
	fmt.Printf("%-25s %15s\n", strings.Repeat("-", 25), strings.Repeat("-", 15))

	if detailed && len(reports) > 0 {
		fmt.Printf("\n%s\n", strings.Repeat("=", 100))
		fmt.Printf("DETAILED FILE STATISTICS\n")
		fmt.Printf("%s\n\n", strings.Repeat("=", 100))

		// Sort reports by filename
		sort.Slice(reports, func(i, j int) bool {
			return reports[i].Filename < reports[j].Filename
		})

		// File sizes table
		fmt.Printf("FILE SIZES\n")
		fmt.Printf("%-30s %10s %12s %12s %12s %10s %10s\n",
			strings.Repeat("-", 30), strings.Repeat("-", 10), strings.Repeat("-", 12),
			strings.Repeat("-", 12), strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 10))
		fmt.Printf("%-30s %10s %12s %12s %12s %10s %10s\n",
			"File", "Records", "Parquet", "JSON", "JSON.gz", "vs JSON", "vs GZ")
		fmt.Printf("%-30s %10s %12s %12s %12s %10s %10s\n",
			strings.Repeat("-", 30), strings.Repeat("-", 10), strings.Repeat("-", 12),
			strings.Repeat("-", 12), strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 10))

		for _, report := range reports {
			fmt.Printf("%-30s %10s %12s %12s %12s %9.1f%% %9.1f%%\n",
				truncateFilename(report.Filename, 30),
				formatNumber(report.Records),
				formatBytes(report.ParquetSize),
				formatBytes(report.UncompressedJSON),
				formatBytes(report.CompressedJSON),
				report.PQvsJSON,
				report.PQvsGZ,
			)
		}
		fmt.Printf("%-30s %10s %12s %12s %12s %10s %10s\n",
			strings.Repeat("-", 30), strings.Repeat("-", 10), strings.Repeat("-", 12),
			strings.Repeat("-", 12), strings.Repeat("-", 12), strings.Repeat("-", 10), strings.Repeat("-", 10))

		// Per-record sizes table
		fmt.Printf("\nPER-RECORD SIZES\n")
		fmt.Printf("%-30s %10s %12s %12s %12s\n",
			strings.Repeat("-", 30), strings.Repeat("-", 10), strings.Repeat("-", 12),
			strings.Repeat("-", 12), strings.Repeat("-", 12))
		fmt.Printf("%-30s %10s %12s %12s %12s\n",
			"File", "Records", "Parquet", "JSON", "JSON.gz")
		fmt.Printf("%-30s %10s %12s %12s %12s\n",
			strings.Repeat("-", 30), strings.Repeat("-", 10), strings.Repeat("-", 12),
			strings.Repeat("-", 12), strings.Repeat("-", 12))

		for _, report := range reports {
			fmt.Printf("%-30s %10s %11.1f %11.1f %11.1f\n",
				truncateFilename(report.Filename, 30),
				formatNumber(report.Records),
				report.BytesPerRecordPQ,
				report.BytesPerRecordJSON,
				report.BytesPerRecordGZ,
			)
		}
		fmt.Printf("%-30s %10s %12s %12s %12s\n",
			strings.Repeat("-", 30), strings.Repeat("-", 10), strings.Repeat("-", 12),
			strings.Repeat("-", 12), strings.Repeat("-", 12))
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 100))
}

func formatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func formatNumber(n int64) string {
	str := fmt.Sprintf("%d", n)
	result := ""
	for i, ch := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result += ","
		}
		result += string(ch)
	}
	return result
}

func truncateFilename(filename string, maxLen int) string {
	if len(filename) <= maxLen {
		return filename
	}
	return filename[:maxLen-3] + "..."
}

// sampleFiles randomly samples n files from the input slice
func sampleFiles(files []string, n int) []string {
	if n >= len(files) {
		return files
	}

	// Create a copy to avoid modifying the original slice
	shuffled := make([]string, len(files))
	copy(shuffled, files)

	// Fisher-Yates shuffle for the first n elements
	for i := 0; i < n; i++ {
		j := i + rand.IntN(len(shuffled)-i)
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	}

	// Return the first n elements
	return shuffled[:n]
}
