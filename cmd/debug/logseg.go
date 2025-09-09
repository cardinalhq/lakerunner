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
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/accumulation"
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
	cmd.AddCommand(getLogSegTestSubCmd())

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

func getLogSegTestSubCmd() *cobra.Command {
	var outputDir string

	cmd := &cobra.Command{
		Use:   "test <segment-journal-dir>",
		Short: "Test compaction on a segment journal directory",
		Long:  `Tests compaction logic on source files from a segment journal directory and compares with expected output.`,
		Args:  cobra.ExactArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			ctx := context.Background()
			seglogDir := args[0]

			// Default output directory if not specified
			if outputDir == "" {
				outputDir = filepath.Join(seglogDir, "test-output")
			}

			// Create output directory
			if err := os.MkdirAll(outputDir, 0755); err != nil {
				return fmt.Errorf("failed to create output directory: %w", err)
			}

			fmt.Printf("Testing compaction on: %s\n", seglogDir)
			fmt.Printf("Output directory: %s\n", outputDir)

			// Read the seglog metadata to determine signal type
			metadataFiles, err := filepath.Glob(filepath.Join(seglogDir, "seglog-*.json"))
			if err != nil || len(metadataFiles) == 0 {
				return fmt.Errorf("no seglog metadata file found in %s", seglogDir)
			}

			metadataContent, err := os.ReadFile(metadataFiles[0])
			if err != nil {
				return fmt.Errorf("failed to read metadata file: %w", err)
			}

			var metadata lrdb.SegmentJournal
			if err := json.Unmarshal(metadataContent, &metadata); err != nil {
				return fmt.Errorf("failed to parse metadata: %w", err)
			}

			// Check signal type - we only support metrics compaction (signal=2)
			if metadata.Signal != 2 {
				return fmt.Errorf("unsupported signal type: %d (only metrics compaction supported)", metadata.Signal)
			}

			// Use the testable metric compactor
			compactor := accumulation.NewTestableCompactor()

			// Process the files
			result, err := compactor.CompactSeglog34(ctx, seglogDir, outputDir)
			if err != nil {
				return fmt.Errorf("compaction failed: %w", err)
			}

			// Print results
			fmt.Printf("\n=== Compaction Results ===\n")
			fmt.Printf("Input files: %s/source/*.parquet\n", seglogDir)
			fmt.Printf("Output files: %d\n", len(result.OutputFiles))
			for i, file := range result.OutputFiles {
				fmt.Printf("  %d: %s\n", i+1, file)
			}
			fmt.Printf("Total records: %d\n", result.TotalRecords)
			fmt.Printf("Total file size: %d bytes\n", result.TotalFileSize)

			// Compare with expected output from metadata
			fmt.Printf("\n=== Comparison ===\n")
			fmt.Printf("Expected source records (metadata): %d\n", metadata.SourceTotalRecords)
			fmt.Printf("Expected dest records (metadata): %d\n", metadata.DestTotalRecords)
			fmt.Printf("Our compaction produced: %d\n", result.TotalRecords)

			// Check if we match the original output
			if result.TotalRecords == metadata.DestTotalRecords {
				fmt.Printf("Record counts match original output (potential data loss preserved)\n")
			} else if result.TotalRecords == metadata.SourceTotalRecords {
				fmt.Printf("All source records preserved (data loss fixed!)\n")
			} else {
				fmt.Printf("Different record count than both source and original dest\n")
			}

			// Calculate data loss metrics
			originalDataLoss := metadata.SourceTotalRecords - metadata.DestTotalRecords
			ourDataLoss := metadata.SourceTotalRecords - result.TotalRecords

			fmt.Printf("\n=== Data Loss Analysis ===\n")
			fmt.Printf("Original compaction lost: %d records (%.1f%%)\n",
				originalDataLoss,
				float64(originalDataLoss)/float64(metadata.SourceTotalRecords)*100)
			fmt.Printf("Our compaction lost: %d records (%.1f%%)\n",
				ourDataLoss,
				float64(ourDataLoss)/float64(metadata.SourceTotalRecords)*100)

			if ourDataLoss == 0 {
				fmt.Printf("Perfect! No data loss in our compaction\n")
			} else if ourDataLoss < originalDataLoss {
				fmt.Printf("Improved! Reduced data loss by %d records\n", originalDataLoss-ourDataLoss)
			} else if ourDataLoss == originalDataLoss {
				fmt.Printf("Same data loss as original\n")
			} else {
				fmt.Printf("Worse! Increased data loss by %d records\n", ourDataLoss-originalDataLoss)
			}

			// Compare with expected dest files if available
			expectedDir := filepath.Join(seglogDir, "dest")
			if entries, err := os.ReadDir(expectedDir); err == nil && len(entries) > 0 {
				fmt.Printf("\n=== Expected Output Files ===\n")
				fmt.Printf("Expected files: %d\n", len(entries))
				for _, entry := range entries {
					if filepath.Ext(entry.Name()) == ".parquet" {
						fmt.Printf("  - %s\n", entry.Name())
					}
				}
			}

			// Enhanced analysis: Schema and record-level comparison
			if err := performEnhancedAnalysis(ctx, seglogDir, result.OutputFiles, metadata); err != nil {
				fmt.Printf("Warning: Enhanced analysis failed: %v\n", err)
			}

			return nil
		},
	}

	cmd.Flags().StringVarP(&outputDir, "output", "o", "", "Output directory for test results (default: <segment-journal-dir>/test-output)")

	return cmd
}

// getFirstRowSchema extracts the schema (keys) from the first row of a parquet file
func getFirstRowSchema(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	reader, err := filereader.NewCookedMetricParquetReader(file, stat.Size(), 1)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	batch, err := reader.Next(context.Background())
	if err != nil {
		if err == io.EOF {
			return []string{}, nil // Empty file
		}
		return nil, fmt.Errorf("read batch: %w", err)
	}

	if batch.Len() == 0 {
		return []string{}, nil // No rows
	}

	// Extract keys from the first row
	firstRow := batch.Get(0)
	var keys []string

	// Get keys from the map (convert RowKey to string)
	for key := range firstRow {
		keys = append(keys, wkk.RowKeyValue(key))
	}

	sort.Strings(keys)
	return keys, nil
}

// compareSchemas compares two schema arrays and returns differences
func compareSchemas(schema1, schema2 []string, name1, name2 string) (onlyIn1, onlyIn2 []string) {
	fmt.Printf("\n=== Schema Comparison ===\n")
	fmt.Printf("%s fields: %d\n", name1, len(schema1))
	fmt.Printf("%s fields: %d\n", name2, len(schema2))

	// Convert to maps for easier comparison
	map1 := make(map[string]bool)
	map2 := make(map[string]bool)

	for _, key := range schema1 {
		map1[key] = true
	}
	for _, key := range schema2 {
		map2[key] = true
	}

	// Find differences
	var common []string

	for key := range map1 {
		if map2[key] {
			common = append(common, key)
		} else {
			onlyIn1 = append(onlyIn1, key)
		}
	}

	for key := range map2 {
		if !map1[key] {
			onlyIn2 = append(onlyIn2, key)
		}
	}

	sort.Strings(common)
	sort.Strings(onlyIn1)
	sort.Strings(onlyIn2)

	fmt.Printf("Common fields: %d\n", len(common))
	if len(onlyIn1) > 0 {
		fmt.Printf("Only in %s: %v\n", name1, onlyIn1)
	}
	if len(onlyIn2) > 0 {
		fmt.Printf("Only in %s: %v\n", name2, onlyIn2)
	}

	if len(onlyIn1) == 0 && len(onlyIn2) == 0 {
		fmt.Printf("Schemas match perfectly!\n")
	} else {
		fmt.Printf("Schema differences detected\n")
	}

	return onlyIn1, onlyIn2
}

// analyzeSchemaDifferencesInFiles analyzes which source files contain the differing schema keys
func analyzeSchemaDifferencesInFiles(sourceDir string, diffKeys []string) error {
	if len(diffKeys) == 0 {
		return nil
	}

	fmt.Printf("\n=== Schema Difference File Analysis ===\n")
	fmt.Printf("Analyzing which source files contain these differing keys: %v\n", diffKeys)

	// Read all parquet files in source directory
	entries, err := os.ReadDir(sourceDir)
	if err != nil {
		return fmt.Errorf("read source directory: %w", err)
	}

	var sourceFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && len(entry.Name()) > 8 && entry.Name()[len(entry.Name())-8:] == ".parquet" {
			sourceFiles = append(sourceFiles, filepath.Join(sourceDir, entry.Name()))
		}
	}

	// Track which files contain each key
	keyToFiles := make(map[string][]string)

	for _, filePath := range sourceFiles {
		schema, err := getFirstRowSchema(filePath)
		if err != nil {
			fmt.Printf("Failed to get schema for %s: %v\n", filepath.Base(filePath), err)
			continue
		}

		schemaMap := make(map[string]bool)
		for _, key := range schema {
			schemaMap[key] = true
		}

		// Check which diff keys are in this file
		for _, diffKey := range diffKeys {
			if schemaMap[diffKey] {
				keyToFiles[diffKey] = append(keyToFiles[diffKey], filepath.Base(filePath))
			}
		}
	}

	// Report findings
	for _, diffKey := range diffKeys {
		files := keyToFiles[diffKey]
		if len(files) == 0 {
			fmt.Printf("Key '%s': Not found in any source file\n", diffKey)
		} else if len(files) == len(sourceFiles) {
			fmt.Printf("Key '%s': Present in ALL %d source files\n", diffKey, len(files))
		} else {
			fmt.Printf("Key '%s': Present in %d/%d source files: %v\n",
				diffKey, len(files), len(sourceFiles), files)
		}
	}

	return nil
}

// RecordKey represents a unique key for identifying records
type RecordKey struct {
	Timestamp int64
	Hash      string // We'll use a simple string representation for now
}

// loadRecordKeys loads record keys from a parquet file for comparison
func loadRecordKeys(filePath string) ([]RecordKey, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	reader, err := filereader.NewCookedMetricParquetReader(file, stat.Size(), 1000)
	if err != nil {
		return nil, fmt.Errorf("create reader: %w", err)
	}
	defer reader.Close()

	var keys []RecordKey

	for {
		batch, err := reader.Next(context.Background())
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read batch: %w", err)
		}

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)

			// Extract timestamp and create a simple hash
			var timestamp int64
			var hash string

			// Look for timestamp field using RowKey
			timestampKey := wkk.NewRowKey("TimestampMs")
			if ts, ok := row[timestampKey].(int64); ok {
				timestamp = ts
			}

			// Create a simple hash based on key fields
			// Use string representation of the entire row as a simple hash
			hash = fmt.Sprintf("%v", row)

			key := RecordKey{
				Timestamp: timestamp,
				Hash:      hash,
			}
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// analyzeRecordDifferences compares record sets and identifies patterns in missing data
func analyzeRecordDifferences(expectedKeys, actualKeys []RecordKey) {
	fmt.Printf("\n=== Record-Level Analysis ===\n")

	// Convert to maps for faster lookup
	expectedMap := make(map[RecordKey]bool)
	actualMap := make(map[RecordKey]bool)

	for _, key := range expectedKeys {
		expectedMap[key] = true
	}
	for _, key := range actualKeys {
		actualMap[key] = true
	}

	// Find missing and extra records
	var missing, extra []RecordKey

	for key := range expectedMap {
		if !actualMap[key] {
			missing = append(missing, key)
		}
	}

	for key := range actualMap {
		if !expectedMap[key] {
			extra = append(extra, key)
		}
	}

	fmt.Printf("Missing records: %d\n", len(missing))
	fmt.Printf("Extra records: %d\n", len(extra))

	if len(missing) == 0 {
		fmt.Printf("No missing records!\n")
		return
	}

	// Sort missing records by timestamp to analyze patterns
	sort.Slice(missing, func(i, j int) bool {
		return missing[i].Timestamp < missing[j].Timestamp
	})

	// Analyze patterns in missing data
	fmt.Printf("\n=== Missing Data Pattern Analysis ===\n")

	if len(missing) > 0 {
		firstMissing := missing[0].Timestamp
		lastMissing := missing[len(missing)-1].Timestamp

		fmt.Printf("Missing data timestamp range: %d to %d\n", firstMissing, lastMissing)

		// Check if missing data is in contiguous blocks
		blocks := findContiguousBlocks(missing)
		fmt.Printf("Missing data appears in %d block(s):\n", len(blocks))

		for i, block := range blocks {
			fmt.Printf("  Block %d: %d records from %d to %d\n",
				i+1, len(block), block[0].Timestamp, block[len(block)-1].Timestamp)
		}

		if len(blocks) == 1 {
			fmt.Printf("📍 Pattern: Missing data is in ONE contiguous block\n")
		} else if len(blocks) < len(missing)/2 {
			fmt.Printf("📍 Pattern: Missing data is mostly in large blocks\n")
		} else {
			fmt.Printf("📍 Pattern: Missing data is scattered throughout\n")
		}

		// Show first few and last few missing records for manual inspection
		showCount := 3
		if len(missing) < showCount*2 {
			showCount = len(missing) / 2
		}

		if showCount > 0 {
			fmt.Printf("\nFirst %d missing records:\n", showCount)
			for i := 0; i < showCount && i < len(missing); i++ {
				fmt.Printf("  %d: %s\n", missing[i].Timestamp, missing[i].Hash)
			}

			if len(missing) > showCount {
				fmt.Printf("\nLast %d missing records:\n", showCount)
				start := len(missing) - showCount
				for i := start; i < len(missing); i++ {
					fmt.Printf("  %d: %s\n", missing[i].Timestamp, missing[i].Hash)
				}
			}
		}
	}
}

// findContiguousBlocks groups consecutive records into blocks (simplified version)
func findContiguousBlocks(records []RecordKey) [][]RecordKey {
	if len(records) == 0 {
		return nil
	}

	var blocks [][]RecordKey
	currentBlock := []RecordKey{records[0]}

	for i := 1; i < len(records); i++ {
		// If timestamps are close (within reasonable range), consider them part of the same block
		if records[i].Timestamp-currentBlock[len(currentBlock)-1].Timestamp < 60000 { // 1 minute threshold
			currentBlock = append(currentBlock, records[i])
		} else {
			blocks = append(blocks, currentBlock)
			currentBlock = []RecordKey{records[i]}
		}
	}

	blocks = append(blocks, currentBlock)
	return blocks
}

// performEnhancedAnalysis performs schema comparison and record-level analysis
func performEnhancedAnalysis(ctx context.Context, seglogDir string, ourOutputFiles []string, metadata lrdb.SegmentJournal) error {
	expectedDir := filepath.Join(seglogDir, "dest")

	// Find expected output files
	expectedEntries, err := os.ReadDir(expectedDir)
	if err != nil {
		return fmt.Errorf("read expected directory: %w", err)
	}

	var expectedFiles []string
	for _, entry := range expectedEntries {
		if strings.HasSuffix(entry.Name(), ".parquet") {
			expectedFiles = append(expectedFiles, filepath.Join(expectedDir, entry.Name()))
		}
	}

	if len(expectedFiles) == 0 {
		return fmt.Errorf("no expected parquet files found")
	}

	if len(ourOutputFiles) == 0 {
		return fmt.Errorf("no output files to compare")
	}

	// Compare schemas
	expectedSchema, err := getFirstRowSchema(expectedFiles[0])
	if err != nil {
		return fmt.Errorf("get expected schema: %w", err)
	}

	ourSchema, err := getFirstRowSchema(ourOutputFiles[0])
	if err != nil {
		return fmt.Errorf("get our schema: %w", err)
	}

	_, onlyInOurs := compareSchemas(expectedSchema, ourSchema, "Expected", "Our Output")

	// Analyze which source files contain the differing keys
	if len(onlyInOurs) > 0 {
		sourceDir := filepath.Join(seglogDir, "source")
		if err := analyzeSchemaDifferencesInFiles(sourceDir, onlyInOurs); err != nil {
			fmt.Printf("Failed to analyze schema differences in files: %v\n", err)
		}
	}

	// Only do record-level analysis if schemas match
	if len(expectedSchema) == len(ourSchema) {
		schemaMatch := true
		expectedMap := make(map[string]bool)
		for _, key := range expectedSchema {
			expectedMap[key] = true
		}
		for _, key := range ourSchema {
			if !expectedMap[key] {
				schemaMatch = false
				break
			}
		}

		if schemaMatch {
			fmt.Printf("\nSchemas match - performing detailed record analysis...\n")

			// Load record keys from both files
			expectedKeys, err := loadRecordKeys(expectedFiles[0])
			if err != nil {
				return fmt.Errorf("load expected record keys: %w", err)
			}

			ourKeys, err := loadRecordKeys(ourOutputFiles[0])
			if err != nil {
				return fmt.Errorf("load our record keys: %w", err)
			}

			analyzeRecordDifferences(expectedKeys, ourKeys)
		} else {
			fmt.Printf("Schemas don't match - skipping record-level analysis\n")
		}
	} else {
		fmt.Printf("Schema sizes differ - skipping record-level analysis\n")
	}

	return nil
}
