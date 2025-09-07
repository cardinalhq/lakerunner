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

package ingestion

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	// Cache for RPF estimates with 5-minute TTL
	rpfEstimateCache     *ttlcache.Cache[string, int64]
	rpfEstimateCacheOnce sync.Once
)

// initRPFEstimateCache initializes the TTL cache for RPF estimates
func initRPFEstimateCache() {
	rpfEstimateCache = ttlcache.New(
		ttlcache.WithTTL[string, int64](5 * time.Minute),
	)
	go rpfEstimateCache.Start() // Start background cleanup
}

// ProcessAccumulatedBatch processes an accumulated batch of metrics with multiple readers
func ProcessAccumulatedBatch(ctx context.Context, args ingest.ProcessBatchArgs, manager *AccumulatorManager, cfg Config, kafkaProducer fly.Producer) error {
	ll := logctx.FromContext(ctx)

	if !manager.HasData() {
		ll.Debug("No data to process in accumulation manager")
		// Still need to update Kafka offsets if any
		offsetUpdates := manager.GetKafkaOffsetUpdates(args.KafkaOffset.ConsumerGroup, args.KafkaOffset.Topic)
		if len(offsetUpdates) > 0 {
			for _, update := range offsetUpdates {
				if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
					ConsumerGroup:       update.ConsumerGroup,
					Topic:               update.Topic,
					Partition:           update.Partition,
					LastProcessedOffset: update.Offset,
				}); err != nil {
					return fmt.Errorf("failed to update Kafka offset: %w", err)
				}
			}
		}
		return nil
	}

	// Collect all segment parameters from all (org, instance) accumulators
	var allSegmentParams []lrdb.InsertMetricSegmentParams
	var totalInputSize, totalOutputSize int64
	var totalInputRows, totalOutputRows int64
	var inputFileCount int
	var maxSlotCount int32

	// Process each (org, instance) accumulator independently
	for key, accumulator := range manager.GetAccumulators() {
		if len(accumulator.readers) == 0 {
			continue
		}

		ll.Debug("Processing accumulator",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("readerCount", len(accumulator.readers)))

		// Get RPF estimate for this org/instance
		rpfEstimate := getRPFEstimate(ctx, args, key.OrganizationID)

		// Process this accumulator
		results, inputRows, err := FlushAccumulator(ctx, accumulator, args.TmpDir, args.IngestDateint, rpfEstimate)
		if err != nil {
			return fmt.Errorf("failed to flush accumulator for %v: %w", key, err)
		}
		totalInputRows += inputRows

		if len(results) == 0 {
			ll.Warn("No output files generated for accumulator",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Int("instanceNum", int(key.InstanceNum)))
			continue
		}

		// Get storage profile for this org/instance
		profile, err := getStorageProfileForOrgInstance(ctx, args.StorageProvider, key, accumulator.readerMetadata, cfg)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for %v: %w", key, err)
		}

		// Create blob client for uploading
		blobclient, err := cloudstorage.NewClient(ctx, args.CloudManager, profile)
		if err != nil {
			return fmt.Errorf("failed to get S3 client for %v: %w", key, err)
		}

		// Prepare upload parameters
		uploadParams := metricsprocessing.UploadParams{
			OrganizationID: profile.OrganizationID.String(),
			InstanceNum:    profile.InstanceNum,
			Dateint:        0,     // Will be calculated from timestamps
			FrequencyMs:    10000, // 10 second blocks
			IngestDateint:  args.IngestDateint,
			CollectorName:  profile.CollectorName,
			Bucket:         profile.Bucket,
			CreatedBy:      lrdb.CreatedByIngest,
		}

		// Create and upload segments for this (org, instance)
		segmentParams, err := createAndUploadSegments(ctx, blobclient, results, uploadParams)
		if err != nil {
			return fmt.Errorf("failed to create segments for %v: %w", key, err)
		}

		allSegmentParams = append(allSegmentParams, segmentParams...)

		// Update metrics
		for _, metadata := range accumulator.readerMetadata {
			totalInputSize += metadata.FileSize
			inputFileCount++
		}
		for _, result := range results {
			totalOutputSize += result.FileSize
			totalOutputRows += result.RecordCount
		}
		// Track max slot count from segments
		for _, segParams := range segmentParams {
			if segParams.SlotCount > maxSlotCount {
				maxSlotCount = segParams.SlotCount
			}
		}
	}

	if len(allSegmentParams) == 0 {
		ll.Warn("No segments created from accumulated batch")
		// Still update Kafka offsets
		offsetUpdates := manager.GetKafkaOffsetUpdates(args.KafkaOffset.ConsumerGroup, args.KafkaOffset.Topic)
		for _, update := range offsetUpdates {
			if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
				ConsumerGroup:       update.ConsumerGroup,
				Topic:               update.Topic,
				Partition:           update.Partition,
				LastProcessedOffset: update.Offset,
			}); err != nil {
				return fmt.Errorf("failed to update Kafka offset: %w", err)
			}
		}
		return nil
	}

	// Calculate size reduction percentage (e.g., 0.5 for 50% reduction)
	var sizeReduction float64
	if totalInputSize > 0 {
		sizeReduction = 1.0 - (float64(totalOutputSize) / float64(totalInputSize))
	}

	ll.Info("Accumulated batch processing summary",
		slog.Int("accumulatorCount", len(manager.GetAccumulators())),
		slog.Int("inputSegmentCount", inputFileCount),
		slog.Int("outputSegmentCount", len(allSegmentParams)),
		slog.Int64("totalInputBytes", totalInputSize),
		slog.Int64("totalOutputBytes", totalOutputSize),
		slog.Int64("inputRows", totalInputRows),
		slog.Int64("outputRows", totalOutputRows),
		slog.Float64("sizeReduction", sizeReduction),
		slog.Int("slotCount", int(maxSlotCount)))

	// Get all Kafka offset updates
	offsetUpdates := manager.GetKafkaOffsetUpdates(args.KafkaOffset.ConsumerGroup, args.KafkaOffset.Topic)

	// Execute atomic transaction: insert all segments + update all Kafka offsets
	batch := lrdb.MetricSegmentBatch{
		Segments:     allSegmentParams,
		KafkaOffsets: offsetUpdates,
	}

	criticalCtx := context.WithoutCancel(ctx)
	if err := args.DB.InsertMetricSegmentBatchWithKafkaOffsets(criticalCtx, batch); err != nil {
		return fmt.Errorf("failed to insert metric segments with Kafka offsets: %w", err)
	}

	// Send notifications to Kafka topics (replaced MCQ)
	if kafkaProducer != nil {
		compactionTopic := "lakerunner.segments.metrics.compact"
		rollupTopic := "lakerunner.segments.metrics.rollup"

		for _, segParams := range allSegmentParams {
			// Create the notification message
			notification := messages.MetricSegmentNotificationMessage{
				OrganizationID: segParams.OrganizationID,
				DateInt:        segParams.Dateint,
				FrequencyMs:    segParams.FrequencyMs,
				SegmentID:      segParams.SegmentID,
				InstanceNum:    segParams.InstanceNum,
				SlotID:         segParams.SlotID,
				SlotCount:      segParams.SlotCount,
				RecordCount:    segParams.RecordCount,
				FileSize:       segParams.FileSize,
				QueuedAt:       time.Now(),
			}

			// Marshal the message
			msgBytes, err := notification.Marshal()
			if err != nil {
				ll.Error("Failed to marshal segment notification",
					slog.Int64("segmentID", segParams.SegmentID),
					slog.Any("error", err))
				continue // Continue with other segments
			}

			// Calculate rollup interval start time for consistent key generation
			rollupStartTime := (segParams.StartTs / int64(segParams.FrequencyMs)) * int64(segParams.FrequencyMs)

			compactionMessage := fly.Message{
				Key:   fmt.Appendf(nil, "%s-%d-%d", segParams.OrganizationID.String(), segParams.Dateint, segParams.SegmentID),
				Value: msgBytes,
			}

			rollupMessage := fly.Message{
				Key:   fmt.Appendf(nil, "%s-%d-%d-%d", segParams.OrganizationID.String(), segParams.Dateint, segParams.FrequencyMs, rollupStartTime),
				Value: msgBytes,
			}

			// Send to compaction topic
			if err := kafkaProducer.Send(criticalCtx, compactionTopic, compactionMessage); err != nil {
				ll.Error("Failed to send compaction notification to Kafka",
					slog.Int64("segmentID", segParams.SegmentID),
					slog.Any("error", err))
				// Continue with other segments
			}

			// Send to rollup topic
			if err := kafkaProducer.Send(criticalCtx, rollupTopic, rollupMessage); err != nil {
				ll.Error("Failed to send rollup notification to Kafka",
					slog.Int64("segmentID", segParams.SegmentID),
					slog.Any("error", err))
				// Continue with other segments
			}
		}
		ll.Debug("Sent compaction notifications to Kafka",
			slog.Int("count", len(allSegmentParams)))
	} else {
		ll.Warn("No Kafka producer provided - compaction notifications will not be sent")
	}

	return nil
}

// getStorageProfileForOrgInstance gets the storage profile for a specific org/instance
func getStorageProfileForOrgInstance(
	ctx context.Context,
	sp storageprofile.StorageProfileProvider,
	key OrgInstanceKey,
	metadata []ReaderMetadata,
	cfg Config,
) (storageprofile.StorageProfile, error) {
	// Use first item's metadata for profile lookup
	if len(metadata) == 0 {
		return storageprofile.StorageProfile{}, fmt.Errorf("no metadata available")
	}

	firstItem := metadata[0].IngestItem

	if cfg.SingleInstanceMode {
		return sp.GetLowestInstanceStorageProfile(ctx, key.OrganizationID, firstItem.Bucket)
	}

	if collectorName := helpers.ExtractCollectorName(firstItem.ObjectID); collectorName != "" {
		return sp.GetStorageProfileForOrganizationAndCollector(ctx, key.OrganizationID, collectorName)
	}

	return sp.GetStorageProfileForOrganizationAndInstance(ctx, key.OrganizationID, int16(key.InstanceNum))
}

// ProcessFileToSortedReader processes a single file to a sorted reader
func ProcessFileToSortedReader(ctx context.Context, item ingest.IngestItem, tmpDir string, storageClient cloudstorage.Client, exemplarProcessor *exemplar.Processor, mdb lrdb.StoreFull) (filereader.Reader, ReaderMetadata, error) {
	ll := logctx.FromContext(ctx)

	// Download file directly to the shared tmpDir
	tmpfilename, _, is404, err := storageClient.DownloadObject(ctx, tmpDir, item.Bucket, item.ObjectID)
	if err != nil {
		return nil, ReaderMetadata{}, fmt.Errorf("failed to download file %s: %w", item.ObjectID, err)
	}
	if is404 {
		return nil, ReaderMetadata{}, fmt.Errorf("object not found: %s", item.ObjectID)
	}

	ll.Debug("Downloaded input file",
		slog.String("objectID", item.ObjectID),
		slog.Int64("fileSize", item.FileSize))

	opts := filereader.ReaderOptions{
		BatchSize: 1000,
	}

	// Create proto reader
	reader, err := CreateMetricProtoReader(tmpfilename, opts)
	if err != nil {
		return nil, ReaderMetadata{}, fmt.Errorf("failed to create proto reader: %w", err)
	}

	// Process exemplars from this reader if exemplar processor is provided (do this before wrapping with other readers)
	if exemplarProcessor != nil {
		if err := processExemplarsFromReader(ctx, reader, exemplarProcessor, item.OrganizationID.String(), mdb); err != nil {
			// Just log error and continue - don't fail the whole file
			ll.Warn("Failed to process exemplars from file",
				slog.String("objectID", item.ObjectID),
				slog.Any("error", err))
		}
	}

	// Add translation
	translator := &MetricTranslator{
		OrgID:    item.OrganizationID.String(),
		Bucket:   item.Bucket,
		ObjectID: item.ObjectID,
	}
	reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	if err != nil {
		reader.Close()
		return nil, ReaderMetadata{}, fmt.Errorf("failed to create translating reader: %w", err)
	}

	// Add disk-based sorting
	keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
	reader, err = filereader.NewDiskSortingReader(reader, keyProvider, 1000)
	if err != nil {
		reader.Close()
		return nil, ReaderMetadata{}, fmt.Errorf("failed to create sorting reader: %w", err)
	}

	metadata := ReaderMetadata{
		ObjectID:       item.ObjectID,
		OrganizationID: item.OrganizationID,
		InstanceNum:    int32(item.InstanceNum),
		Bucket:         item.Bucket,
		FileSize:       item.FileSize,
		IngestItem:     item,
	}

	return reader, metadata, nil
}

// getRPFEstimate gets the RPF estimate for a specific org/instance using metric_pack_estimate table with caching
func getRPFEstimate(ctx context.Context, args ingest.ProcessBatchArgs, orgID uuid.UUID) int64 {
	// Initialize cache once
	rpfEstimateCacheOnce.Do(initRPFEstimateCache)

	// Create cache key - use orgID since estimates are per-org, not per-instance
	cacheKey := fmt.Sprintf("%s:10000", orgID.String()) // 10000ms frequency

	// Check cache first
	if item := rpfEstimateCache.Get(cacheKey); item != nil {
		return item.Value()
	}

	// Cache miss - query the database
	estimates, err := args.DB.GetMetricPackEstimateForOrg(ctx, lrdb.GetMetricPackEstimateForOrgParams{
		OrganizationID: orgID,
		FrequencyMs:    10000, // 10 second blocks
	})

	var result int64 = 40_000 // Default fallback

	if err == nil {
		// If we have results, prefer the specific org estimate over the default
		for _, estimate := range estimates {
			// First result should be the specific org if it exists (due to ORDER BY organization_id DESC)
			if estimate.OrganizationID == orgID && estimate.TargetRecords != nil {
				result = *estimate.TargetRecords
				break
			}
		}

		// If no specific org estimate but we have a default (all zeros)
		if result == 40_000 && len(estimates) > 0 && estimates[len(estimates)-1].TargetRecords != nil {
			result = *estimates[len(estimates)-1].TargetRecords // Last one should be default
		}
	}

	// Cache the result (even if it's the fallback value)
	rpfEstimateCache.Set(cacheKey, result, ttlcache.DefaultTTL)

	return result
}
