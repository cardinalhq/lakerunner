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
