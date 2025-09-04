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
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
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

			// Kafka is required for ingestion
			cfg, err := config.Load()
			if err != nil {
				return fmt.Errorf("failed to load config: %w", err)
			}

			// Also set trace partitions from config
			if cfg.Traces.Partitions > 0 {
				ingesttraces.SetNumTracePartitions(cfg.Traces.Partitions)
			}

			kafkaFactory := fly.NewFactory(&cfg.Fly)
			if !kafkaFactory.IsEnabled() {
				return fmt.Errorf("Kafka is required for ingestion but is not enabled")
			}

			slog.Info("Starting traces ingestion with Kafka consumer")

			consumer, err := NewKafkaIngestConsumer(kafkaFactory, cfg, "traces", "lakerunner.ingest.traces")
			if err != nil {
				return fmt.Errorf("failed to create Kafka consumer: %w", err)
			}
			defer func() {
				if err := consumer.Close(); err != nil {
					slog.Error("Error closing Kafka consumer", slog.Any("error", err))
				}
			}()

			return consumer.Run(doneCtx)
		},
	}

	rootCmd.AddCommand(cmd)
}

func traceIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	cloudManagers *cloudstorage.CloudManagers, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	ll.Info("Processing trace item")

	// Convert IngestItem to Inqueue for compatibility with existing code
	inqueueItems := ConvertIngestItemsToInqueue([]IngestItem{item})

	// Get storage profile
	var profile storageprofile.StorageProfile
	var err error

	if collectorName := helpers.ExtractCollectorName(item.ObjectID); collectorName != "" {
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, item.OrganizationID, collectorName)
	} else {
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, item.OrganizationID, item.InstanceNum)
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

	ll.Info("Processing batch item",
		slog.String("objectID", item.ObjectID),
		slog.Int64("fileSize", item.FileSize))

	itemTmpdir := fmt.Sprintf("%s/item", tmpdir)
	if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
		return fmt.Errorf("creating item tmpdir: %w", err)
	}

	tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, itemTmpdir, s3client, item.Bucket, item.ObjectID)
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", item.ObjectID, err)
	}
	if is404 {
		ll.Warn("S3 object not found, skipping", slog.String("objectID", item.ObjectID))
		return nil
	}

	// Convert file if not already in otel-raw format
	if !strings.HasPrefix(item.ObjectID, "otel-raw/") {
		if strings.HasPrefix(item.ObjectID, "db/") {
			return nil
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

		// Group results by slot
		for _, result := range results {
			slotResults[result.SlotID] = append(slotResults[result.SlotID], result)
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
			dbObjectID := helpers.MakeDBObjectID(item.OrganizationID, "", ingest_dateint, hour, segmentID, "traces")

			if err := storageClient.UploadObject(ctx, firstItem.Bucket, dbObjectID, result.FileName); err != nil {
				return fmt.Errorf("failed to upload trace file to %s: %w", profile.CloudProvider, err)
			}

			_ = os.Remove(result.FileName)

			err = mdb.InsertTraceSegment(ctx, lrdb.InsertTraceSegmentDirectParams{
				OrganizationID: item.OrganizationID,
				Dateint:        ingest_dateint,
				IngestDateint:  ingest_dateint,
				SegmentID:      segmentID,
				InstanceNum:    item.InstanceNum,
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
