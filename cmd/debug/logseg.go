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
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func GetLogSegCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "segment-journal",
		Short: "Segment journal debugging utilities",
		Long:  `Utilities for debugging segment journal entries and downloading associated files.`,
	}

	cmd.AddCommand(getLogSegFetchSubCmd())
	cmd.AddCommand(getLogSegCheckSubCmd())

	return cmd
}

func getLogSegFetchSubCmd() *cobra.Command {
	var segLogID int64

	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch segment journal files and metadata",
		Long:  `Downloads source and destination files for a segment journal entry and organizes them in directories.`,
		RunE: func(c *cobra.Command, _ []string) error {
			ctx := context.Background()

			// Open database connections using admin-friendly connections
			lrdbStore, err := dbopen.LRDBStoreForAdmin(ctx)
			if err != nil {
				return fmt.Errorf("failed to connect to lrdb: %w", err)
			}

			configStore, err := dbopen.ConfigDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to connect to configdb: %w", err)
			}

			// Get the segment journal entry
			var segLog lrdb.SegmentJournal
			if segLogID == 0 {
				// Get the latest record
				segLog, err = lrdbStore.GetLatestSegmentJournal(ctx)
				if err != nil {
					if err == sql.ErrNoRows {
						return fmt.Errorf("no segment_journal entries found")
					}
					return fmt.Errorf("failed to query latest segment_journal: %w", err)
				}
				segLogID = segLog.ID
				fmt.Printf("Using latest segment_journal entry: ID %d\n", segLogID)
			} else {
				// Get specific record by ID
				segLog, err = lrdbStore.GetSegmentJournalByID(ctx, segLogID)
				if err != nil {
					if err == sql.ErrNoRows {
						return fmt.Errorf("segment_journal entry with id %d not found", segLogID)
					}
					return fmt.Errorf("failed to query segment_journal: %w", err)
				}
			}

			// Create directory structure
			dirName := fmt.Sprintf("seglog-%d", segLog.ID)
			if err := os.MkdirAll(filepath.Join(dirName, "source"), 0755); err != nil {
				return fmt.Errorf("failed to create source directory: %w", err)
			}
			if err := os.MkdirAll(filepath.Join(dirName, "dest"), 0755); err != nil {
				return fmt.Errorf("failed to create dest directory: %w", err)
			}

			// Get storage profile for this organization
			storageProvider := storageprofile.NewStorageProfileProvider(configStore)
			profile, err := storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, segLog.OrganizationID, segLog.InstanceNum)
			if err != nil {
				return fmt.Errorf("failed to get storage profile for org %s instance %d: %w", segLog.OrganizationID, segLog.InstanceNum, err)
			}

			// Initialize cloud managers and storage client
			awsManager, err := awsclient.NewManager(ctx)
			if err != nil {
				return fmt.Errorf("failed to create AWS manager: %w", err)
			}

			cloudManagers := &cloudstorage.CloudManagers{
				AWS: awsManager,
			}

			storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
			if err != nil {
				return fmt.Errorf("failed to create storage client: %w", err)
			}

			// Download source files
			fmt.Printf("Downloading %d source files...\n", len(segLog.SourceObjectKeys))
			for _, objectKey := range segLog.SourceObjectKeys {
				fileName := filepath.Base(objectKey)

				fmt.Printf("  %s -> %s\n", objectKey, fileName)
				tmpFile, _, _, err := storageClient.DownloadObject(ctx, filepath.Join(dirName, "source"), profile.Bucket, objectKey)
				if err != nil {
					fmt.Printf("    Warning: failed to download %s: %v\n", objectKey, err)
					continue
				}

				// Rename to clean filename
				finalPath := filepath.Join(dirName, "source", fileName)
				if err := os.Rename(tmpFile, finalPath); err != nil {
					fmt.Printf("    Warning: failed to rename %s to %s: %v\n", tmpFile, finalPath, err)
				}
			}

			// Download destination files
			fmt.Printf("Downloading %d destination files...\n", len(segLog.DestObjectKeys))
			for _, objectKey := range segLog.DestObjectKeys {
				fileName := filepath.Base(objectKey)

				fmt.Printf("  %s -> %s\n", objectKey, fileName)
				tmpFile, _, _, err := storageClient.DownloadObject(ctx, filepath.Join(dirName, "dest"), profile.Bucket, objectKey)
				if err != nil {
					fmt.Printf("    Warning: failed to download %s: %v\n", objectKey, err)
					continue
				}

				// Rename to clean filename
				finalPath := filepath.Join(dirName, "dest", fileName)
				if err := os.Rename(tmpFile, finalPath); err != nil {
					fmt.Printf("    Warning: failed to rename %s to %s: %v\n", tmpFile, finalPath, err)
				}
			}

			// Write metadata JSON
			metadataFile := filepath.Join(dirName, fmt.Sprintf("seglog-%d.json", segLog.ID))
			metadataJSON, err := json.MarshalIndent(segLog, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal metadata: %w", err)
			}

			if err := os.WriteFile(metadataFile, metadataJSON, 0644); err != nil {
				return fmt.Errorf("failed to write metadata file: %w", err)
			}

			fmt.Printf("Segment log %d files organized in: %s\n", segLog.ID, dirName)
			fmt.Printf("Metadata written to: %s\n", metadataFile)

			return nil
		},
	}

	cmd.Flags().Int64Var(&segLogID, "id", 0, "Segment log ID to fetch (if not specified, fetches the latest record)")

	return cmd
}

func getLogSegCheckSubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "check <directory>",
		Short: "Validate segment journal data from directory",
		Long:  `Analyzes segment journal data in a directory. Finds the JSON file with the same name as the directory and validates the operation, providing detailed reports for metrics compaction.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			directory := args[0]

			// Find and read the JSON file
			jsonFile, err := findSegmentJournalJSON(directory)
			if err != nil {
				return fmt.Errorf("failed to find JSON file: %w", err)
			}

			fmt.Printf("Using JSON file: %s\n", jsonFile)
			fmt.Printf("Working directory: %s\n\n", directory)

			// Change to the directory for relative file paths
			originalDir, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("failed to get current directory: %w", err)
			}
			defer func() {
				_ = os.Chdir(originalDir)
			}()

			if err := os.Chdir(directory); err != nil {
				return fmt.Errorf("failed to change to directory %s: %w", directory, err)
			}

			// Read and parse the JSON file
			jsonData, err := os.ReadFile(filepath.Base(jsonFile))
			if err != nil {
				return fmt.Errorf("failed to read JSON file: %w", err)
			}

			var segLog lrdb.SegmentJournal
			if err := json.Unmarshal(jsonData, &segLog); err != nil {
				return fmt.Errorf("failed to parse JSON: %w", err)
			}

			// Determine signal and action names for display
			signalName := getSignalName(segLog.Signal)
			actionName := getActionName(segLog.Action)

			fmt.Printf("Segment Journal Check Report\n")
			fmt.Printf("============================\n")
			fmt.Printf("ID: %d\n", segLog.ID)
			fmt.Printf("Signal: %s (%d)\n", signalName, segLog.Signal)
			fmt.Printf("Action: %s (%d)\n", actionName, segLog.Action)
			fmt.Printf("Organization ID: %s\n", segLog.OrganizationID)
			fmt.Printf("Date: %d\n", segLog.Dateint)
			fmt.Printf("\n")

			// Check if this is metrics + compact
			if segLog.Signal == 2 && segLog.Action == 2 {
				return checkMetricsCompact(&segLog)
			} else {
				// For other types, just report basic info
				fmt.Printf("Operation Type: %s %s\n", signalName, actionName)
				fmt.Printf("This operation type is not currently validated by the check command.\n")
				fmt.Printf("Basic info:\n")
				fmt.Printf("  Source files: %d\n", segLog.SourceCount)
				fmt.Printf("  Destination files: %d\n", segLog.DestCount)
				fmt.Printf("  Source records: %d\n", segLog.SourceTotalRecords)
				fmt.Printf("  Destination records: %d\n", segLog.DestTotalRecords)
				fmt.Printf("  Source size: %d bytes\n", segLog.SourceTotalSize)
				fmt.Printf("  Destination size: %d bytes\n", segLog.DestTotalSize)
			}

			return nil
		},
	}

	return cmd
}

func checkMetricsCompact(segLog *lrdb.SegmentJournal) error {
	fmt.Printf("Metrics Compaction Validation\n")
	fmt.Printf("=============================\n")

	var hasErrors bool

	// 1. Check frequencies match
	fmt.Printf("1. Frequency Check:\n")
	if segLog.SourceFrequencyMs == segLog.DestFrequencyMs {
		fmt.Printf("   Source and destination frequencies match: %d ms\n", segLog.SourceFrequencyMs)
	} else {
		fmt.Printf("   Frequency mismatch! Source: %d ms, Destination: %d ms\n",
			segLog.SourceFrequencyMs, segLog.DestFrequencyMs)
		hasErrors = true
	}

	// 2. Check time ranges match
	fmt.Printf("\n2. Time Range Check:\n")
	if segLog.SourceMinTimestamp == segLog.DestMinTimestamp {
		fmt.Printf("   Start times match: %d\n", segLog.SourceMinTimestamp)
	} else {
		fmt.Printf("   Start time mismatch! Source: %d, Destination: %d\n",
			segLog.SourceMinTimestamp, segLog.DestMinTimestamp)
		hasErrors = true
	}

	if segLog.SourceMaxTimestamp == segLog.DestMaxTimestamp {
		fmt.Printf("   End times match: %d\n", segLog.SourceMaxTimestamp)
	} else {
		fmt.Printf("   End time mismatch! Source: %d, Destination: %d\n",
			segLog.SourceMaxTimestamp, segLog.DestMaxTimestamp)
		hasErrors = true
	}

	// 3. Check record counts
	fmt.Printf("\n3. Record Count Check:\n")
	if segLog.DestTotalRecords <= segLog.SourceTotalRecords {
		recordDiff := segLog.SourceTotalRecords - segLog.DestTotalRecords
		reductionPct := float64(recordDiff) / float64(segLog.SourceTotalRecords) * 100

		if recordDiff == 0 {
			fmt.Printf("   Record counts match exactly: %d\n", segLog.SourceTotalRecords)
		} else {
			fmt.Printf("   Destination records ≤ source records\n")
			fmt.Printf("   Source: %d records\n", segLog.SourceTotalRecords)
			fmt.Printf("   Destination: %d records\n", segLog.DestTotalRecords)
			fmt.Printf("   Difference: %d records (%.2f%% reduction)\n", recordDiff, reductionPct)
		}
	} else {
		fmt.Printf("   Destination has more records than source!\n")
		fmt.Printf("   Source: %d records\n", segLog.SourceTotalRecords)
		fmt.Printf("   Destination: %d records\n", segLog.DestTotalRecords)
		fmt.Printf("   Excess: %d records\n", segLog.DestTotalRecords-segLog.SourceTotalRecords)
		hasErrors = true
	}

	// 4. Reduction summary
	fmt.Printf("\n4. Compaction Summary:\n")

	// Bytes reduction
	if segLog.SourceTotalSize > 0 {
		byteDiff := segLog.SourceTotalSize - segLog.DestTotalSize
		byteReductionPct := float64(byteDiff) / float64(segLog.SourceTotalSize) * 100
		fmt.Printf("   Bytes: %d → %d (%d bytes, %.2f%% reduction)\n",
			segLog.SourceTotalSize, segLog.DestTotalSize, byteDiff, byteReductionPct)
	}

	// Segment count reduction
	segCountDiff := segLog.SourceCount - segLog.DestCount
	if segLog.SourceCount > 0 {
		segReductionPct := float64(segCountDiff) / float64(segLog.SourceCount) * 100
		fmt.Printf("   Segments: %d → %d (%d segments, %.2f%% reduction)\n",
			segLog.SourceCount, segLog.DestCount, segCountDiff, segReductionPct)
	}

	// 5. Enhanced file analysis
	fmt.Printf("\n5. Enhanced File Analysis:\n")
	fileAnalysisErr := analyzeMetricFiles(segLog)
	if fileAnalysisErr != nil {
		fmt.Printf("   File analysis failed: %v\n", fileAnalysisErr)
		hasErrors = true
	} else {
		fmt.Printf("   File analysis completed successfully\n")
	}

	fmt.Printf("\n")
	if hasErrors {
		fmt.Printf("Validation completed with ERRORS\n")
		return fmt.Errorf("validation failed - see errors above")
	} else {
		fmt.Printf("Validation completed successfully\n")
	}

	return nil
}

func getSignalName(signal int16) string {
	switch signal {
	case 1:
		return "logs"
	case 2:
		return "metrics"
	case 3:
		return "traces"
	default:
		return "unknown"
	}
}

func getActionName(action int16) string {
	switch action {
	case 1:
		return "ingest"
	case 2:
		return "compact"
	case 3:
		return "rollup"
	default:
		return "unknown"
	}
}

// MetricKey represents a unique metric identifier (name, TID, timestamp)
type MetricKey struct {
	Name      string
	TID       int64
	Timestamp int64
}

// FileMetrics contains analysis results for a single file
type FileMetrics struct {
	Filename    string
	Keys        map[MetricKey]bool
	SortVersion int16
	IsSorted    bool
	SortErrors  []string
	RowCount    int64
}

// analyzeMetricFiles performs detailed analysis of source and destination files
func analyzeMetricFiles(segLog *lrdb.SegmentJournal) error {
	// Analyze source files
	fmt.Printf("   5.1 Source File Analysis:\n")
	sourceMetrics := make(map[string]*FileMetrics)
	allSourceKeys := make(map[MetricKey]bool)

	for _, objKey := range segLog.SourceObjectKeys {
		filename := filepath.Base(objKey)
		localPath := filepath.Join("source", filename)
		fm, err := analyzeParquetFile(localPath)
		if err != nil {
			fmt.Printf("     Warning: %s - %v\n", filename, err)
			continue
		}
		sourceMetrics[filename] = fm

		// Collect all keys from source files
		for key := range fm.Keys {
			allSourceKeys[key] = true
		}

		fmt.Printf("     %s: %d records, %d unique keys\n", filename, fm.RowCount, len(fm.Keys))
		if !fm.IsSorted {
			fmt.Printf("       WARNING: File is not properly sorted\n")
			for _, errMsg := range fm.SortErrors {
				fmt.Printf("         %s\n", errMsg)
			}
		}
	}

	// Analyze destination files
	fmt.Printf("   5.2 Destination File Analysis:\n")
	destMetrics := make(map[string]*FileMetrics)
	allDestKeys := make(map[MetricKey]bool)

	for _, objKey := range segLog.DestObjectKeys {
		filename := filepath.Base(objKey)
		localPath := filepath.Join("dest", filename)
		fm, err := analyzeParquetFile(localPath)
		if err != nil {
			fmt.Printf("     Warning: %s - %v\n", filename, err)
			continue
		}
		destMetrics[filename] = fm

		// Collect all keys from destination files
		for key := range fm.Keys {
			allDestKeys[key] = true
		}

		fmt.Printf("     %s: %d records, %d unique keys\n", filename, fm.RowCount, len(fm.Keys))
		if !fm.IsSorted {
			fmt.Printf("       WARNING: File is not properly sorted\n")
			for _, errMsg := range fm.SortErrors {
				fmt.Printf("         %s\n", errMsg)
			}
		}
	}

	// 5.3 Key preservation analysis
	fmt.Printf("   5.3 Key Preservation Analysis:\n")
	missingKeys := make([]MetricKey, 0)
	extraKeys := make([]MetricKey, 0)

	// Find missing keys (in source but not in destination)
	for key := range allSourceKeys {
		if !allDestKeys[key] {
			missingKeys = append(missingKeys, key)
		}
	}

	// Find extra keys (in destination but not in source)
	for key := range allDestKeys {
		if !allSourceKeys[key] {
			extraKeys = append(extraKeys, key)
		}
	}

	if len(missingKeys) == 0 && len(extraKeys) == 0 {
		fmt.Printf("     Key preservation: PASS - All %d source keys found in destination\n", len(allSourceKeys))
	} else {
		if len(missingKeys) > 0 {
			fmt.Printf("     Key preservation: FAIL - %d keys missing from destination\n", len(missingKeys))
			for i, key := range missingKeys {
				if i < 10 { // Show first 10 missing keys
					fmt.Printf("       Missing: name=%s, tid=%d, timestamp=%d\n", key.Name, key.TID, key.Timestamp)
				} else if i == 10 {
					fmt.Printf("       ... and %d more\n", len(missingKeys)-10)
					break
				}
			}
		}
		if len(extraKeys) > 0 {
			fmt.Printf("     Extra keys in destination: %d\n", len(extraKeys))
			for i, key := range extraKeys {
				if i < 10 { // Show first 10 extra keys
					fmt.Printf("       Extra: name=%s, tid=%d, timestamp=%d\n", key.Name, key.TID, key.Timestamp)
				} else if i == 10 {
					fmt.Printf("       ... and %d more\n", len(extraKeys)-10)
					break
				}
			}
		}
		return fmt.Errorf("key preservation failed: %d missing, %d extra", len(missingKeys), len(extraKeys))
	}

	return nil
}

// analyzeParquetFile analyzes a single Parquet file for metrics
func analyzeParquetFile(filePath string) (*FileMetrics, error) {
	if !fileExists(filePath) {
		return nil, fmt.Errorf("file not found: %s", filePath)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[map[string]any](pf, pf.Schema())
	defer reader.Close()

	fm := &FileMetrics{
		Filename:   filepath.Base(filePath),
		Keys:       make(map[MetricKey]bool),
		IsSorted:   true,
		SortErrors: make([]string, 0),
	}

	// Get sort version from filename if present
	fm.SortVersion = extractSortVersionFromFilename(fm.Filename)

	// Read all rows to collect keys and check sorting
	var lastKey *MetricKey
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

		// Process each row in this batch
		for i := 0; i < n; i++ {
			row := rows[i]
			fm.RowCount++

			// Extract metric key
			key := extractMetricKey(row)
			if key != nil {
				fm.Keys[*key] = true

				// Check sorting if we have CurrentSortVersion
				if fm.SortVersion == lrdb.CurrentMetricSortVersion {
					if lastKey != nil {
						if !isMetricKeyLessOrEqual(*lastKey, *key) {
							fm.IsSorted = false
							fm.SortErrors = append(fm.SortErrors,
								fmt.Sprintf("Row %d: sort order violation - previous: [%s,%d,%d], current: [%s,%d,%d]",
									fm.RowCount, lastKey.Name, lastKey.TID, lastKey.Timestamp,
									key.Name, key.TID, key.Timestamp))
						}
					}
					lastKey = key
				}
			}
		}

		if err == io.EOF {
			break
		}
	}

	return fm, nil
}

// extractMetricKey extracts the metric name, TID, and timestamp from a row
func extractMetricKey(row map[string]any) *MetricKey {
	// Convert map[string]any to filereader.Row format
	frRow := make(filereader.Row)
	for k, v := range row {
		frRow[wkk.NewRowKey(k)] = v
	}

	// Extract name
	name, nameOk := frRow[wkk.RowKeyCName].(string)
	if !nameOk {
		return nil
	}

	// Extract TID
	tid, tidOk := frRow[wkk.RowKeyCTID].(int64)
	if !tidOk {
		// Try string conversion for legacy files
		if tidStr, ok := frRow[wkk.RowKeyCTID].(string); ok {
			if tidInt64, err := stringToInt64(tidStr); err == nil {
				tid = tidInt64
				tidOk = true
			}
		}
	}
	if !tidOk {
		return nil
	}

	// Extract timestamp
	timestamp, tsOk := frRow[wkk.RowKeyCTimestamp].(int64)
	if !tsOk {
		return nil
	}

	return &MetricKey{
		Name:      name,
		TID:       tid,
		Timestamp: timestamp,
	}
}

// isMetricKeyLessOrEqual checks if key1 <= key2 according to metric sort order
func isMetricKeyLessOrEqual(key1, key2 MetricKey) bool {
	// Sort order: [name, tid, timestamp]
	if key1.Name < key2.Name {
		return true
	}
	if key1.Name > key2.Name {
		return false
	}

	// Names are equal, check TID
	if key1.TID < key2.TID {
		return true
	}
	if key1.TID > key2.TID {
		return false
	}

	// Names and TIDs are equal, check timestamp
	return key1.Timestamp <= key2.Timestamp
}

// extractSortVersionFromFilename extracts sort version from filename if present
func extractSortVersionFromFilename(filename string) int16 {
	// Look for version patterns in filename like "_v2" or "_sort2"
	if strings.Contains(filename, "_v2") || strings.Contains(filename, "_sort2") {
		return lrdb.MetricSortVersionNameTidTimestampV2
	}
	if strings.Contains(filename, "_v1") || strings.Contains(filename, "_sort1") {
		return lrdb.MetricSortVersionNameTidTimestamp
	}
	// Default to current version for new files
	return lrdb.CurrentMetricSortVersion
}

// stringToInt64 converts a string to int64, handling various formats
func stringToInt64(s string) (int64, error) {
	return stringToInt64Helper(s, 10)
}

func stringToInt64Helper(s string, base int) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty string")
	}
	// Use a simple conversion approach
	result := int64(0)
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("invalid character: %c", r)
		}
		result = result*10 + int64(r-'0')
	}
	return result, nil
}

// fileExists checks if a file exists and is not a directory
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// findSegmentJournalJSON finds the JSON file with the same name as the directory
func findSegmentJournalJSON(directory string) (string, error) {
	// Clean the directory path
	directory = filepath.Clean(directory)

	// Check if directory exists
	info, err := os.Stat(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("directory does not exist: %s", directory)
		}
		return "", fmt.Errorf("failed to stat directory: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("path is not a directory: %s", directory)
	}

	// Get the directory name (basename)
	dirName := filepath.Base(directory)

	// Look for JSON file with same name as directory
	jsonFile := filepath.Join(directory, dirName+".json")
	if fileExists(jsonFile) {
		return jsonFile, nil
	}

	// Also try looking for any JSON file that looks like segment log format
	files, err := os.ReadDir(directory)
	if err != nil {
		return "", fmt.Errorf("failed to read directory: %w", err)
	}

	// Look for files matching seglog-*.json pattern
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		name := file.Name()
		if strings.HasPrefix(name, "seglog-") && strings.HasSuffix(name, ".json") {
			return filepath.Join(directory, name), nil
		}
	}

	return "", fmt.Errorf("no suitable JSON file found in directory %s (looked for %s.json or seglog-*.json)", directory, dirName)
}
