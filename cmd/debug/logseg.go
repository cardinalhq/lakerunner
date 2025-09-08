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
		Use:   "logseg",
		Short: "Segment log debugging utilities",
		Long:  `Utilities for debugging segment log entries and downloading associated files.`,
	}

	cmd.AddCommand(getLogSegFetchSubCmd())

	return cmd
}

func getLogSegFetchSubCmd() *cobra.Command {
	var segLogID int64

	cmd := &cobra.Command{
		Use:   "fetch",
		Short: "Fetch segment log files and metadata",
		Long:  `Downloads source and destination files for a segment log entry and organizes them in directories.`,
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

			// Get the segment log entry
			var segLog lrdb.SegLog
			if segLogID == 0 {
				// Get the latest record
				segLog, err = lrdbStore.GetLatestSegLog(ctx)
				if err != nil {
					if err == sql.ErrNoRows {
						return fmt.Errorf("no seg_log entries found")
					}
					return fmt.Errorf("failed to query latest seg_log: %w", err)
				}
				segLogID = segLog.ID
				fmt.Printf("Using latest seg_log entry: ID %d\n", segLogID)
			} else {
				// Get specific record by ID
				segLog, err = lrdbStore.GetSegLogByID(ctx, segLogID)
				if err != nil {
					if err == sql.ErrNoRows {
						return fmt.Errorf("seg_log entry with id %d not found", segLogID)
					}
					return fmt.Errorf("failed to query seg_log: %w", err)
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
			for i, objectKey := range segLog.SourceObjectKeys {
				fileName := fmt.Sprintf("source_%d_%s", i, filepath.Base(objectKey))

				fmt.Printf("  %s -> %s\n", objectKey, fileName)
				_, _, _, err := storageClient.DownloadObject(ctx, filepath.Join(dirName, "source"), profile.Bucket, objectKey)
				if err != nil {
					fmt.Printf("    Warning: failed to download %s: %v\n", objectKey, err)
				}
			}

			// Download destination files
			fmt.Printf("Downloading %d destination files...\n", len(segLog.DestObjectKeys))
			for i, objectKey := range segLog.DestObjectKeys {
				fileName := fmt.Sprintf("dest_%d_%s", i, filepath.Base(objectKey))

				fmt.Printf("  %s -> %s\n", objectKey, fileName)
				_, _, _, err := storageClient.DownloadObject(ctx, filepath.Join(dirName, "dest"), profile.Bucket, objectKey)
				if err != nil {
					fmt.Printf("    Warning: failed to download %s: %v\n", objectKey, err)
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
