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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func GetAnalyzeCmd() *cobra.Command {
	var (
		metricName string
	)

	cmd := &cobra.Command{
		Use:   "analyze DIR",
		Short: "Analyze downloaded Parquet files",
		Long:  `Analyze downloaded Parquet files to track metric availability and detect gaps.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := args[0]
			return runAnalyze(cmd.Context(), dir, metricName)
		},
	}

	cmd.Flags().StringVar(&metricName, "metric", "", "Metric name to analyze (required)")
	_ = cmd.MarkFlagRequired("metric")

	return cmd
}

// AnalysisMetadata contains the metadata from the download
type AnalysisMetadata struct {
	Signal         string    `json:"signal"`
	OrganizationID string    `json:"organization_id"`
	StartTimeMs    int64     `json:"start_time_ms"`
	EndTimeMs      int64     `json:"end_time_ms"`
	StartDateint   int32     `json:"start_dateint"`
	EndDateint     int32     `json:"end_dateint"`
	FrequencyMs    *int32    `json:"frequency_ms,omitempty"`
	DateGathered   time.Time `json:"date_gathered"`
	SegmentCount   int       `json:"segment_count"`
}

// MetricDataPoint represents a single metric observation
type MetricDataPoint struct {
	Timestamp int64
	TID       string
	Value     float64
	Name      string
}

// TimeSeriesAnalysis tracks analysis for a single time series (TID)
type TimeSeriesAnalysis struct {
	TID              string
	FirstSeen        int64
	LastSeen         int64
	DataPoints       int
	ExpectedInterval int64 // in milliseconds
	Gaps             []TimeGap
	ActualIntervals  map[int64]int     // histogram of actual intervals
	Timestamps       []int64           // sorted timestamps for detailed analysis
	DetectedPattern  *ReportingPattern // Detected reporting pattern if regular but sparse
}

// ReportingPattern represents a detected regular reporting pattern
type ReportingPattern struct {
	Frequency   int64   // Detected frequency in milliseconds
	Confidence  float64 // Confidence in the pattern (0-1)
	Regularity  float64 // How regular the pattern is (0-1)
	Description string  // Human-readable description
}

// TimeGap represents a gap in the time series
type TimeGap struct {
	StartMs       int64
	EndMs         int64
	MissingPoints int
}

func runAnalyze(ctx context.Context, dir, metricName string) error {
	// Read metadata to get time range
	metadataPath := filepath.Join(dir, "metadata.json")
	metadata, err := readAnalysisMetadata(metadataPath)
	if err != nil {
		return fmt.Errorf("failed to read metadata: %w", err)
	}

	if metadata.Signal != "metrics" {
		return fmt.Errorf("analyze command currently only supports metrics data")
	}

	expectedInterval := int64(10000) // default 10s
	if metadata.FrequencyMs != nil {
		expectedInterval = int64(*metadata.FrequencyMs)
	}

	fmt.Printf("Analyzing metric: %s\n", metricName)
	fmt.Printf("Time range: %s to %s\n",
		time.UnixMilli(metadata.StartTimeMs).Format(time.RFC3339),
		time.UnixMilli(metadata.EndTimeMs).Format(time.RFC3339))
	fmt.Printf("Expected interval: %dms\n\n", expectedInterval)

	// Find all parquet files
	parquetFiles, err := findParquetFiles(dir)
	if err != nil {
		return fmt.Errorf("failed to find parquet files: %w", err)
	}

	if len(parquetFiles) == 0 {
		return fmt.Errorf("no parquet files found in %s", dir)
	}

	fmt.Printf("Found %d parquet files to analyze\n\n", len(parquetFiles))

	// Analyze the metric across all files
	analysis, err := analyzeMetric(ctx, parquetFiles, metricName, metadata.StartTimeMs, metadata.EndTimeMs, expectedInterval)
	if err != nil {
		return fmt.Errorf("failed to analyze metric: %w", err)
	}

	// Print analysis results
	printAnalysisResults(metricName, analysis, expectedInterval)

	return nil
}

func readAnalysisMetadata(path string) (*AnalysisMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	var metadata AnalysisMetadata
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func findParquetFiles(dir string) ([]string, error) {
	var files []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if strings.HasSuffix(path, ".parquet") {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Sort files to process them in order
	sort.Strings(files)
	return files, nil
}

func analyzeMetric(ctx context.Context, files []string, metricName string, startMs, endMs int64, expectedInterval int64) (map[string]*TimeSeriesAnalysis, error) {
	// Map of TID -> TimeSeriesAnalysis
	timeSeries := make(map[string]*TimeSeriesAnalysis)

	fmt.Printf("Creating readers for %d files...\n", len(files))

	// Create readers for all files
	var filteredReaders []filereader.Reader
	var openFiles []*os.File
	var shouldCloseReaders = true // Track if we should close readers in defer
	defer func() {
		// Only close readers if merge-sorted reader wasn't created
		// (merge-sorted reader takes ownership and closes them)
		if shouldCloseReaders {
			for _, r := range filteredReaders {
				_ = r.Close()
			}
		}
		for _, f := range openFiles {
			_ = f.Close()
		}
	}()

	for _, filePath := range files {
		f, err := os.Open(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %w", filePath, err)
		}
		openFiles = append(openFiles, f)

		stat, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to stat %s: %w", filePath, err)
		}

		// Create base parquet reader
		baseReader, err := filereader.NewCookedMetricParquetReader(f, stat.Size(), 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to create reader for %s: %w", filePath, err)
		}

		// Wrap with filtering reader for the specific metric
		filteredReader := filereader.NewMetricFilteringReader(baseReader, metricName)
		filteredReaders = append(filteredReaders, filteredReader)
	}

	fmt.Printf("Creating merge-sorted reader...\n")

	// Create merge-sorted reader from filtered readers
	var finalReader filereader.Reader
	if len(filteredReaders) == 1 {
		finalReader = filteredReaders[0]
	} else if len(filteredReaders) > 1 {
		// Use the metric sort key provider (name, tid, timestamp)
		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		mergedReader, err := filereader.NewMergesortReader(ctx, filteredReaders, keyProvider, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to create merge-sorted reader: %w", err)
		}
		finalReader = mergedReader
		// Merge-sorted reader takes ownership of the filtered readers
		shouldCloseReaders = false
		defer func() {
			_ = mergedReader.Close()
		}()
	}

	fmt.Printf("Processing filtered data...\n")

	// Process all rows (already filtered by metric name)
	rowCount := int64(0)
	matches := int64(0)
	var lastTID string
	var lastTimestamp int64

	for {
		batch, err := finalReader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read batch: %w", err)
		}

		// Process each row in the batch
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			rowCount++

			if rowCount%10000 == 0 {
				fmt.Printf("\rProcessing data: %d rows analyzed...", rowCount)
			}

			// All rows are already filtered for our target metric

			// Get TID (can be string or int64)
			tidVal, exists := row[wkk.RowKeyCTID]
			if !exists {
				continue
			}
			var tid string
			switch v := tidVal.(type) {
			case string:
				tid = v
			case int64:
				tid = fmt.Sprintf("%d", v)
			case int32:
				tid = fmt.Sprintf("%d", v)
			case float64:
				tid = fmt.Sprintf("%.0f", v)
			default:
				continue
			}

			// Get timestamp
			tsVal, exists := row[wkk.RowKeyCTimestamp]
			if !exists {
				continue
			}
			var timestamp int64
			switch v := tsVal.(type) {
			case int64:
				timestamp = v
			case int32:
				timestamp = int64(v)
			case float64:
				timestamp = int64(v)
			default:
				continue
			}

			// Filter by time range
			if timestamp < startMs || timestamp >= endMs {
				continue
			}

			// Get or create time series for this TID
			ts, exists := timeSeries[tid]
			if !exists {
				ts = &TimeSeriesAnalysis{
					TID:              tid,
					FirstSeen:        timestamp,
					LastSeen:         timestamp,
					ExpectedInterval: expectedInterval,
					ActualIntervals:  make(map[int64]int),
					Timestamps:       make([]int64, 0, 1000),
				}
				timeSeries[tid] = ts
			}

			// Track intervals if same TID
			if tid == lastTID && lastTimestamp > 0 {
				interval := timestamp - lastTimestamp
				if interval > 0 {
					ts.ActualIntervals[interval]++

					// Detect gaps (more than 1.5x expected interval)
					if interval > expectedInterval*3/2 {
						missingPoints := int((interval / expectedInterval) - 1)
						ts.Gaps = append(ts.Gaps, TimeGap{
							StartMs:       lastTimestamp,
							EndMs:         timestamp,
							MissingPoints: missingPoints,
						})
					}
				}
			}

			// Update time series stats
			if timestamp < ts.FirstSeen {
				ts.FirstSeen = timestamp
			}
			if timestamp > ts.LastSeen {
				ts.LastSeen = timestamp
			}
			ts.DataPoints++
			ts.Timestamps = append(ts.Timestamps, timestamp)
			matches++

			lastTID = tid
			lastTimestamp = timestamp
		}
	}

	fmt.Printf("\n\nProcessed %d total rows, found %d data points for metric: %s\n", rowCount, matches, metricName)

	// Perform detailed analysis on each time series
	for _, ts := range timeSeries {
		analyzeTimeSeriesDetailed(ts, expectedInterval)
	}

	return timeSeries, nil
}

func analyzeTimeSeriesDetailed(ts *TimeSeriesAnalysis, expectedInterval int64) {
	// Sort timestamps
	sort.Slice(ts.Timestamps, func(i, j int) bool {
		return ts.Timestamps[i] < ts.Timestamps[j]
	})

	if len(ts.Timestamps) < 2 {
		return
	}

	// Detect reporting pattern if data seems sparse
	duration := ts.LastSeen - ts.FirstSeen
	if duration > 0 {
		expectedPoints := int((duration / expectedInterval)) + 1
		completeness := float64(ts.DataPoints) / float64(expectedPoints)

		// If less than 50% complete, look for a pattern
		if completeness < 0.5 && ts.DataPoints > 2 {
			ts.DetectedPattern = detectReportingPattern(ts.Timestamps, expectedInterval)
		}
	}

	// Clear previous analysis
	ts.Gaps = nil
	ts.ActualIntervals = make(map[int64]int)

	// Analyze intervals and detect gaps
	for i := 1; i < len(ts.Timestamps); i++ {
		interval := ts.Timestamps[i] - ts.Timestamps[i-1]
		if interval > 0 {
			// Round interval to nearest expected interval for grouping
			roundedInterval := ((interval + expectedInterval/2) / expectedInterval) * expectedInterval
			ts.ActualIntervals[roundedInterval]++

			// Detect significant gaps (more than 2x expected interval)
			if interval > expectedInterval*2 {
				missingPoints := int((interval / expectedInterval) - 1)
				ts.Gaps = append(ts.Gaps, TimeGap{
					StartMs:       ts.Timestamps[i-1],
					EndMs:         ts.Timestamps[i],
					MissingPoints: missingPoints,
				})
			}
		}
	}

	// Update first and last seen
	ts.FirstSeen = ts.Timestamps[0]
	ts.LastSeen = ts.Timestamps[len(ts.Timestamps)-1]
}

type SummaryStats struct {
	TotalTimeSeries   int
	HealthyTimeSeries int
	WithGaps          int
	WithMissingData   int
	WithExtraData     int
	WithPatterns      int // Time series with detected regular reporting patterns
	CommonIntervals   map[int64]int
	AnomalousTS       []*TimeSeriesAnalysis
}

func printAnalysisResults(metricName string, analysis map[string]*TimeSeriesAnalysis, expectedInterval int64) {
	if len(analysis) == 0 {
		fmt.Printf("No data found for metric: %s\n", metricName)
		return
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
	fmt.Printf("ANALYSIS SUMMARY for metric: %s\n", metricName)
	fmt.Printf("%s\n\n", strings.Repeat("=", 80))

	// Calculate summary statistics
	summary := calculateSummary(analysis, expectedInterval)

	// Print overall statistics
	fmt.Printf("OVERVIEW\n")
	fmt.Printf("  Total time series: %d\n", summary.TotalTimeSeries)
	fmt.Printf("  Expected interval: %dms\n", expectedInterval)

	if summary.TotalTimeSeries > 0 {
		// Find common time range
		var minFirst, maxLast int64
		first := true
		for _, ts := range analysis {
			if first {
				minFirst = ts.FirstSeen
				maxLast = ts.LastSeen
				first = false
			} else {
				if ts.FirstSeen > minFirst {
					minFirst = ts.FirstSeen
				}
				if ts.LastSeen < maxLast {
					maxLast = ts.LastSeen
				}
			}
		}
		fmt.Printf("  Common time range: %s to %s\n",
			time.UnixMilli(minFirst).Format("15:04:05"),
			time.UnixMilli(maxLast).Format("15:04:05"))
		expectedPoints := int((maxLast-minFirst)/expectedInterval) + 1
		fmt.Printf("  Expected data points per series: ~%d\n", expectedPoints)
	}

	fmt.Printf("\nHEALTH STATUS\n")
	fmt.Printf("  Healthy (no issues): %d (%.1f%%)\n",
		summary.HealthyTimeSeries,
		float64(summary.HealthyTimeSeries)*100/float64(summary.TotalTimeSeries))
	if summary.WithPatterns > 0 {
		fmt.Printf("  With detected reporting patterns: %d\n", summary.WithPatterns)
	}
	fmt.Printf("  With gaps: %d\n", summary.WithGaps)
	fmt.Printf("  Missing significant data: %d\n", summary.WithMissingData)
	fmt.Printf("  Extra data points: %d\n", summary.WithExtraData)

	// Show interval distribution if interesting
	if len(summary.CommonIntervals) > 1 || (len(summary.CommonIntervals) == 1 && !hasKey(summary.CommonIntervals, expectedInterval)) {
		fmt.Printf("\nINTERVAL DISTRIBUTION\n")
		intervals := make([]int64, 0, len(summary.CommonIntervals))
		for interval := range summary.CommonIntervals {
			intervals = append(intervals, interval)
		}
		sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })

		for _, interval := range intervals {
			count := summary.CommonIntervals[interval]
			if interval == expectedInterval {
				fmt.Printf("  %dms: %d occurrences (expected)\n", interval, count)
			} else {
				fmt.Printf("  %dms: %d occurrences\n", interval, count)
			}
		}
	}

	// Show time series with detected patterns (including normal patterns)
	fmt.Printf("\nTIME SERIES PATTERNS\n")
	patternCount := 0
	maxShow := 10
	for _, ts := range analysis {
		if ts.DetectedPattern != nil && patternCount < maxShow {
			fmt.Printf("  TID: %s - %s (confidence: %.1f%%, regularity: %.1f%%)\n",
				ts.TID,
				ts.DetectedPattern.Description,
				ts.DetectedPattern.Confidence*100,
				ts.DetectedPattern.Regularity*100)
			patternCount++
		}
	}
	if patternCount == 0 {
		fmt.Printf("  No patterns detected (insufficient data)\n")
	} else if summary.WithPatterns > maxShow {
		fmt.Printf("  ... and %d more time series\n", summary.WithPatterns-maxShow)
	}

	// Show anomalous time series
	if len(summary.AnomalousTS) > 0 {
		fmt.Printf("\nANOMALOUS TIME SERIES (showing up to 10)\n")
		maxShow := 10
		if len(summary.AnomalousTS) < maxShow {
			maxShow = len(summary.AnomalousTS)
		}

		for i := 0; i < maxShow; i++ {
			ts := summary.AnomalousTS[i]
			duration := ts.LastSeen - ts.FirstSeen
			expectedPoints := int((duration / expectedInterval)) + 1
			completeness := float64(ts.DataPoints) * 100 / float64(expectedPoints)

			fmt.Printf("\n  TID: %s\n", ts.TID)
			fmt.Printf("    Points: %d/%d (%.1f%% complete)\n", ts.DataPoints, expectedPoints, completeness)
			fmt.Printf("    Time: %s to %s\n",
				time.UnixMilli(ts.FirstSeen).Format("15:04:05"),
				time.UnixMilli(ts.LastSeen).Format("15:04:05"))

			// Show detected reporting pattern if found
			if ts.DetectedPattern != nil {
				fmt.Printf("    Detected Pattern: %s (confidence: %.1f%%, regularity: %.1f%%)\n",
					ts.DetectedPattern.Description,
					ts.DetectedPattern.Confidence*100,
					ts.DetectedPattern.Regularity*100)
			}

			if len(ts.Gaps) > 0 {
				// Don't report gaps as anomalies if we detected a regular pattern with high confidence
				if ts.DetectedPattern == nil || ts.DetectedPattern.Confidence < 0.8 {
					fmt.Printf("    Gaps: %d\n", len(ts.Gaps))
					// Show first few gaps
					for j, gap := range ts.Gaps {
						if j >= 3 {
							fmt.Printf("      ... and %d more\n", len(ts.Gaps)-3)
							break
						}
						fmt.Printf("      %s-%s (%d missing)\n",
							time.UnixMilli(gap.StartMs).Format("15:04:05"),
							time.UnixMilli(gap.EndMs).Format("15:04:05"),
							gap.MissingPoints)
					}
				} else {
					fmt.Printf("    Note: Gaps are expected with the detected reporting pattern\n")
				}
			}
		}

		if len(summary.AnomalousTS) > maxShow {
			fmt.Printf("\n  ... and %d more anomalous time series\n", len(summary.AnomalousTS)-maxShow)
		}
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 80))
}

func calculateSummary(analysis map[string]*TimeSeriesAnalysis, expectedInterval int64) *SummaryStats {
	summary := &SummaryStats{
		TotalTimeSeries: len(analysis),
		CommonIntervals: make(map[int64]int),
		AnomalousTS:     make([]*TimeSeriesAnalysis, 0),
	}

	// Analyze each time series

	for _, ts := range analysis {
		duration := ts.LastSeen - ts.FirstSeen
		expectedPoints := int((duration / expectedInterval)) + 1

		// Allow for 10% variance in data points, plus edge effects (±2 points)
		allowedVariance := expectedPoints/10 + 2
		dataPointDiff := ts.DataPoints - expectedPoints

		isAnomalous := false

		// Count detected patterns
		if ts.DetectedPattern != nil {
			summary.WithPatterns++
		}

		// Check for gaps (but not if we have a high-confidence pattern)
		if len(ts.Gaps) > 0 {
			// If we have a detected pattern with high confidence, gaps are expected
			if ts.DetectedPattern == nil || ts.DetectedPattern.Confidence < 0.8 {
				summary.WithGaps++
				isAnomalous = true
			}
		}

		// Check for missing data (but not if we have a detected pattern)
		if dataPointDiff < -allowedVariance {
			// If we have a detected pattern with good confidence, sparse data is expected
			if ts.DetectedPattern == nil || ts.DetectedPattern.Confidence < 0.6 {
				summary.WithMissingData++
				isAnomalous = true
			}
		}

		// Check for extra data
		if dataPointDiff > allowedVariance {
			summary.WithExtraData++
			isAnomalous = true
		}

		// Count interval distributions
		for interval, count := range ts.ActualIntervals {
			summary.CommonIntervals[interval] += count
		}

		if isAnomalous {
			summary.AnomalousTS = append(summary.AnomalousTS, ts)
		} else {
			summary.HealthyTimeSeries++
		}
	}

	// Sort anomalous time series by severity (most gaps first, then most missing data)
	sort.Slice(summary.AnomalousTS, func(i, j int) bool {
		if len(summary.AnomalousTS[i].Gaps) != len(summary.AnomalousTS[j].Gaps) {
			return len(summary.AnomalousTS[i].Gaps) > len(summary.AnomalousTS[j].Gaps)
		}
		// Compare data completeness
		dur1 := summary.AnomalousTS[i].LastSeen - summary.AnomalousTS[i].FirstSeen
		dur2 := summary.AnomalousTS[j].LastSeen - summary.AnomalousTS[j].FirstSeen
		exp1 := (dur1 / expectedInterval) + 1
		exp2 := (dur2 / expectedInterval) + 1
		comp1 := float64(summary.AnomalousTS[i].DataPoints) / float64(exp1)
		comp2 := float64(summary.AnomalousTS[j].DataPoints) / float64(exp2)
		return comp1 < comp2
	})

	return summary
}

func hasKey(m map[int64]int, key int64) bool {
	_, exists := m[key]
	return exists
}

// detectReportingPattern analyzes timestamps to detect if there's a regular reporting pattern
func detectReportingPattern(timestamps []int64, expectedInterval int64) *ReportingPattern {
	if len(timestamps) < 2 {
		return nil
	}

	// Calculate all intervals
	intervals := make([]int64, 0, len(timestamps)-1)
	for i := 1; i < len(timestamps); i++ {
		interval := timestamps[i] - timestamps[i-1]
		if interval > 0 {
			intervals = append(intervals, interval)
		}
	}

	if len(intervals) < 2 {
		return nil
	}

	// Find the most common interval (mode)
	intervalCounts := make(map[int64]int)
	for _, interval := range intervals {
		intervalCounts[interval]++
	}

	var modeInterval int64
	var modeCount int
	for interval, count := range intervalCounts {
		if count > modeCount {
			modeInterval = interval
			modeCount = count
		}
	}

	// Calculate regularity (how consistent the intervals are)
	regularity := float64(modeCount) / float64(len(intervals))

	// Calculate confidence based on number of samples and regularity
	confidence := regularity
	if len(intervals) < 10 {
		confidence *= float64(len(intervals)) / 10.0
	}

	// Generate description
	var description string
	if modeInterval%60000 == 0 {
		minutes := modeInterval / 60000
		description = fmt.Sprintf("Reports every %d minute", minutes)
		if minutes > 1 {
			description += "s"
		}
	} else if modeInterval%1000 == 0 {
		seconds := modeInterval / 1000
		description = fmt.Sprintf("Reports every %d second", seconds)
		if seconds > 1 {
			description += "s"
		}
	} else {
		description = fmt.Sprintf("Reports every %dms", modeInterval)
	}

	// Add note if this matches the expected pattern
	if modeInterval >= expectedInterval*9/10 && modeInterval <= expectedInterval*11/10 {
		description += " (expected)"
	}

	return &ReportingPattern{
		Frequency:   modeInterval,
		Confidence:  confidence,
		Regularity:  regularity,
		Description: description,
	}
}
