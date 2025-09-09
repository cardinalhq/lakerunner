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
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
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
	var jsonFile string

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Validate segment journal data from JSON file",
		Long:  `Analyzes segment journal JSON file and validates the operation, providing detailed reports for metrics compaction.`,
		RunE: func(c *cobra.Command, _ []string) error {
			// Read and parse the JSON file
			jsonData, err := os.ReadFile(jsonFile)
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

	cmd.Flags().StringVar(&jsonFile, "file", "", "Path to the segment journal JSON file (required)")
	cmd.MarkFlagRequired("file")

	return cmd
}

func checkMetricsCompact(segLog *lrdb.SegmentJournal) error {
	fmt.Printf("Metrics Compaction Validation\n")
	fmt.Printf("=============================\n")

	var hasErrors bool

	// 1. Check frequencies match
	fmt.Printf("1. Frequency Check:\n")
	if segLog.SourceFrequencyMs == segLog.DestFrequencyMs {
		fmt.Printf("   ✓ Source and destination frequencies match: %d ms\n", segLog.SourceFrequencyMs)
	} else {
		fmt.Printf("   ✗ Frequency mismatch! Source: %d ms, Destination: %d ms\n",
			segLog.SourceFrequencyMs, segLog.DestFrequencyMs)
		hasErrors = true
	}

	// 2. Check time ranges match
	fmt.Printf("\n2. Time Range Check:\n")
	if segLog.SourceMinTimestamp == segLog.DestMinTimestamp {
		fmt.Printf("   ✓ Start times match: %d\n", segLog.SourceMinTimestamp)
	} else {
		fmt.Printf("   ✗ Start time mismatch! Source: %d, Destination: %d\n",
			segLog.SourceMinTimestamp, segLog.DestMinTimestamp)
		hasErrors = true
	}

	if segLog.SourceMaxTimestamp == segLog.DestMaxTimestamp {
		fmt.Printf("   ✓ End times match: %d\n", segLog.SourceMaxTimestamp)
	} else {
		fmt.Printf("   ✗ End time mismatch! Source: %d, Destination: %d\n",
			segLog.SourceMaxTimestamp, segLog.DestMaxTimestamp)
		hasErrors = true
	}

	// 3. Check record counts
	fmt.Printf("\n3. Record Count Check:\n")
	if segLog.DestTotalRecords <= segLog.SourceTotalRecords {
		recordDiff := segLog.SourceTotalRecords - segLog.DestTotalRecords
		reductionPct := float64(recordDiff) / float64(segLog.SourceTotalRecords) * 100

		if recordDiff == 0 {
			fmt.Printf("   ✓ Record counts match exactly: %d\n", segLog.SourceTotalRecords)
		} else {
			fmt.Printf("   ✓ Destination records ≤ source records\n")
			fmt.Printf("     Source: %d records\n", segLog.SourceTotalRecords)
			fmt.Printf("     Destination: %d records\n", segLog.DestTotalRecords)
			fmt.Printf("     Difference: %d records (%.2f%% reduction)\n", recordDiff, reductionPct)
		}
	} else {
		fmt.Printf("   ✗ Destination has more records than source!\n")
		fmt.Printf("     Source: %d records\n", segLog.SourceTotalRecords)
		fmt.Printf("     Destination: %d records\n", segLog.DestTotalRecords)
		fmt.Printf("     Excess: %d records\n", segLog.DestTotalRecords-segLog.SourceTotalRecords)
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

	fmt.Printf("\n")
	if hasErrors {
		fmt.Printf("❌ Validation completed with ERRORS\n")
		return fmt.Errorf("validation failed - see errors above")
	} else {
		fmt.Printf("✅ Validation completed successfully\n")
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
