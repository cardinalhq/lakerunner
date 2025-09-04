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
	"log/slog"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/cmd/ingesttraces"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// SlotHourBoundary combines slot ID with hour boundary for trace compaction
type SlotHourBoundary struct {
	SlotID       int
	HourBoundary helpers.HourBoundary
}

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-traces",
		Short: "Ingest traces from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-traces"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "traces"),
				attribute.String("action", "ingest"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewIngestLoopContext(doneCtx, "traces")
			if err != nil {
				return fmt.Errorf("failed to create ingest loop context: %w", err)
			}

			for {
				select {
				case <-doneCtx.Done():
					slog.Info("Ingest traces command done")
					return nil
				default:
				}

				err := IngestLoopWithBatch(loop, nil, traceIngestBatch)
				if err != nil {
					slog.Error("Error in ingest loop", slog.Any("error", err))
				}
			}
		},
	}

	rootCmd.AddCommand(cmd)
}

func traceIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	cloudManagers *cloudstorage.CloudManagers, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	if len(items) == 0 {
		return fmt.Errorf("empty batch")
	}

	ll.Info("Processing traces batch", slog.Int("batchSize", len(items)))

	// Get storage profile
	var profile storageprofile.StorageProfile
	var err error

	firstItem := items[0]
	if collectorName := helpers.ExtractCollectorName(firstItem.ObjectID); collectorName != "" {
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, firstItem.OrganizationID, collectorName)
	} else {
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
	}
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Create cloud storage client
	storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
	if err != nil {
		return fmt.Errorf("failed to create storage client for provider %s: %w", profile.CloudProvider, err)
	}

	// Collect all trace file results from all items, grouped by slot
	slotResults := make(map[int][]ingesttraces.TraceFileResult)

	for _, inf := range items {
		ll.Info("Processing batch item",
			slog.String("itemID", inf.ID.String()),
			slog.String("objectID", inf.ObjectID),
			slog.Int64("fileSize", inf.FileSize))

		itemTmpdir := fmt.Sprintf("%s/item_%s", tmpdir, inf.ID.String())
		if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
			return fmt.Errorf("creating item tmpdir: %w", err)
		}

		tmpfilename, _, is404, err := storageClient.DownloadObject(ctx, itemTmpdir, inf.Bucket, inf.ObjectID)
		if err != nil {
			return fmt.Errorf("failed to download file %s from %s: %w", inf.ObjectID, profile.CloudProvider, err)
		}
		if is404 {
			ll.Warn("Object not found in cloud storage, skipping",
				slog.String("cloudProvider", profile.CloudProvider),
				slog.String("itemID", inf.ID.String()),
				slog.String("objectID", inf.ObjectID))
			continue
		}

		// Convert file if not already in otel-raw format
		if !strings.HasPrefix(inf.ObjectID, "otel-raw/") {
			if strings.HasPrefix(inf.ObjectID, "db/") {
				continue
			}

			if !strings.HasSuffix(inf.ObjectID, ".binpb") {
				ll.Warn("Unsupported file type for traces, skipping", slog.String("objectID", inf.ObjectID))
				continue
			}

			results, err := ingesttraces.ConvertProtoFile(tmpfilename, itemTmpdir, inf.Bucket, inf.ObjectID, rpfEstimate, ingest_dateint, inf.OrganizationID.String())
			if err != nil {
				return fmt.Errorf("failed to convert trace file %s: %w", inf.ObjectID, err)
			}

			// Group results by slot
			for _, result := range results {
				slotResults[result.SlotID] = append(slotResults[result.SlotID], result)
			}
		}
	}

	if len(slotResults) == 0 {
		ll.Info("No trace files to process in batch")
		return nil
	}

	// Process each slot and upload combined results
	for slotID, results := range slotResults {
		if len(results) == 0 {
			continue
		}

		ll.Info("Processing slot from batch",
			slog.Int("slotID", slotID),
			slog.Int("fileCount", len(results)))

		// For simplicity, upload each result file separately
		// This could be optimized to merge files per slot in the future
		for _, result := range results {
			segmentID := s3helper.GenerateID()
			hour := int16(0) // Hour doesn't matter for slot-based traces
			dbObjectID := helpers.MakeDBObjectID(firstItem.OrganizationID, firstItem.CollectorName, ingest_dateint, hour, segmentID, "traces")

			if err := storageClient.UploadObject(ctx, firstItem.Bucket, dbObjectID, result.FileName); err != nil {
				return fmt.Errorf("failed to upload trace file to %s: %w", profile.CloudProvider, err)
			}

			_ = os.Remove(result.FileName)

			err = mdb.InsertTraceSegment(ctx, lrdb.InsertTraceSegmentDirectParams{
				OrganizationID: firstItem.OrganizationID,
				Dateint:        ingest_dateint,
				IngestDateint:  ingest_dateint,
				SegmentID:      segmentID,
				InstanceNum:    firstItem.InstanceNum,
				SlotID:         int32(result.SlotID),
				StartTs:        result.MinTimestamp,
				EndTs:          result.MaxTimestamp,
				RecordCount:    result.RecordCount,
				FileSize:       result.FileSize,
				CreatedBy:      lrdb.CreatedByIngest,
				Fingerprints:   []int64{}, // TODO: Extract fingerprints
			})
			if err != nil {
				return fmt.Errorf("failed to insert trace segment: %w", err)
			}

			ll.Info("Inserted trace segment from batch",
				slog.Int64("segmentID", segmentID),
				slog.Int("slotID", result.SlotID),
				slog.Int64("recordCount", result.RecordCount),
				slog.Int64("fileSize", result.FileSize),
				slog.Int64("startTs", result.MinTimestamp),
				slog.Int64("endTs", result.MaxTimestamp))
		}
	}

	return nil
}
