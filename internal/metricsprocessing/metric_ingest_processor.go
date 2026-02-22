// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package metricsprocessing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/expressionindex"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricIngestProcessor implements the Processor interface for raw metric ingestion
type MetricIngestProcessor struct {
	store           MetricIngestStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	kafkaProducer   fly.Producer
	config          *config.Config
}

// newMetricIngestProcessor creates a new metric ingest processor instance
func newMetricIngestProcessor(
	cfg *config.Config,
	store MetricIngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, kafkaProducer fly.Producer) *MetricIngestProcessor {
	return &MetricIngestProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		kafkaProducer:   kafkaProducer,
		config:          cfg,
	}
}

// validateMetricIngestMessages validates the key and messages for consistency
func validateMetricIngestMessages(key messages.IngestKey, msgs []*messages.ObjStoreNotificationMessage) error {
	if len(msgs) == 0 {
		return &GroupValidationError{
			Field:   "message_count",
			Message: "message list cannot be empty",
		}
	}

	// Get expected values from the key
	expectedOrg := key.OrganizationID
	expectedInstance := key.InstanceNum

	// Validate each message against the expected values
	for i, msg := range msgs {
		if msg.OrganizationID != expectedOrg {
			return &GroupValidationError{
				Field:    "organization_id",
				Expected: expectedOrg,
				Got:      msg.OrganizationID,
				Message:  fmt.Sprintf("message %d has inconsistent organization ID", i),
			}
		}

		if msg.InstanceNum != expectedInstance {
			return &GroupValidationError{
				Field:    "instance_num",
				Expected: expectedInstance,
				Got:      msg.InstanceNum,
				Message:  fmt.Sprintf("message %d has inconsistent instance number", i),
			}
		}
	}

	return nil
}

// ProcessBundle implements the Processor interface and performs raw metric ingestion
func (p *MetricIngestProcessor) ProcessBundle(ctx context.Context, key messages.IngestKey, msgs []*messages.ObjStoreNotificationMessage, partition int32, offset int64) error {
	ll := logctx.FromContext(ctx).With(
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)))

	ll.Info("Starting metric ingestion",
		slog.Int("messageCount", len(msgs)))

	if err := validateMetricIngestMessages(key, msgs); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}
	if refresher := expressionindex.MaybeGlobalCatalogRefresher(); refresher != nil {
		if err := refresher.MaybeRefreshOrg(ctx, key.OrganizationID); err != nil {
			ll.Warn("Failed to refresh expression catalog", slog.Any("error", err))
		}
	}

	// Create temporary directory for this ingestion run
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	ctx, storageSpan := boxerTracer.Start(ctx, "metrics.ingest.setup_storage", trace.WithAttributes(
		attribute.String("organization_id", key.OrganizationID.String()),
		attribute.Int("instance_num", int(key.InstanceNum)),
	))

	srcProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, key.OrganizationID, key.InstanceNum)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to get storage profile")
		storageSpan.End()
		return fmt.Errorf("get storage profile: %w", err)
	}

	inputClient, err := cloudstorage.NewClient(ctx, p.cmgr, srcProfile)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to create input client")
		storageSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}

	dstProfile := srcProfile
	if p.config.Metrics.Ingestion.SingleInstanceMode {
		dstProfile, err = p.storageProvider.GetLowestInstanceStorageProfile(ctx, srcProfile.OrganizationID, srcProfile.Bucket)
		if err != nil {
			storageSpan.RecordError(err)
			storageSpan.SetStatus(codes.Error, "failed to get lowest instance profile")
			storageSpan.End()
			return fmt.Errorf("get lowest instance storage profile: %w", err)
		}
	}

	outputClient, err := cloudstorage.NewClient(ctx, p.cmgr, dstProfile)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to create output client")
		storageSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}
	storageSpan.End()

	var downloadedFiles []string
	var totalInputSize int64

	ctx, downloadSpan := boxerTracer.Start(ctx, "metrics.ingest.download_files", trace.WithAttributes(
		attribute.Int("file_count", len(msgs)),
	))

	for _, msg := range msgs {
		ll.Debug("Processing raw metric file",
			slog.String("objectID", msg.ObjectID),
			slog.Int64("fileSize", msg.FileSize))

		tmpFilename, _, is404, err := inputClient.DownloadObject(ctx, tmpDir, msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to download file", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue // Skip this file but continue with others
		}
		if is404 {
			ll.Warn("Object not found, skipping", slog.String("objectID", msg.ObjectID))
			continue
		}

		downloadedFiles = append(downloadedFiles, tmpFilename)
		totalInputSize += msg.FileSize
	}

	downloadSpan.SetAttributes(
		attribute.Int("files_downloaded", len(downloadedFiles)),
		attribute.Int64("total_input_size", totalInputSize),
	)
	downloadSpan.End()

	if len(downloadedFiles) == 0 {
		ll.Info("No files downloaded successfully")
		return nil
	}

	// Process all files using pure DuckDB pipeline
	// This handles reading, 10s aggregation, dateint partitioning, and parquet export
	ctx, processSpan := boxerTracer.Start(ctx, "metrics.ingest.duckdb_process")
	duckDBResult, err := processMetricIngestWithDuckDB(ctx, downloadedFiles, key.OrganizationID.String(), tmpDir)
	if err != nil {
		processSpan.RecordError(err)
		processSpan.SetStatus(codes.Error, "failed to process with DuckDB")
		processSpan.End()
		return fmt.Errorf("failed to process metrics with DuckDB: %w", err)
	}
	processSpan.SetAttributes(
		attribute.Int("dateint_bins", len(duckDBResult.DateintBins)),
		attribute.Int64("total_rows", duckDBResult.TotalRows),
		attribute.Int("failed_partitions", len(duckDBResult.FailedPartitions)),
	)
	processSpan.End()

	// Check for partition failures - this indicates partial data loss
	if len(duckDBResult.FailedPartitions) > 0 {
		// If all partitions failed, return error to trigger retry
		if len(duckDBResult.DateintBins) == 0 {
			return fmt.Errorf("all %d dateint partitions failed to process", len(duckDBResult.FailedPartitions))
		}
		// Some partitions succeeded but others failed - return error for retry
		// to avoid partial data ingestion without the caller knowing
		return fmt.Errorf("partial failure: %d partitions succeeded, %d failed",
			len(duckDBResult.DateintBins), len(duckDBResult.FailedPartitions))
	}

	if len(duckDBResult.DateintBins) == 0 {
		ll.Info("No output files generated")
		return nil
	}

	// Upload dateint bins to S3 and create segment parameters
	ctx, uploadSpan := boxerTracer.Start(ctx, "metrics.ingest.upload_segments")
	segmentParams, err := p.uploadDuckDBResults(ctx, outputClient, duckDBResult.DateintBins, dstProfile)
	if err != nil {
		uploadSpan.RecordError(err)
		uploadSpan.SetStatus(codes.Error, "failed to upload segments")
		uploadSpan.End()
		return fmt.Errorf("failed to upload DuckDB results: %w", err)
	}
	uploadSpan.SetAttributes(attribute.Int("segment_count", len(segmentParams)))
	uploadSpan.End()

	totalInputRecords := duckDBResult.TotalRows

	criticalCtx := context.WithoutCancel(ctx)
	ctx, dbSpan := boxerTracer.Start(ctx, "metrics.ingest.insert_segments", trace.WithAttributes(
		attribute.Int("segment_count", len(segmentParams)),
	))

	if err := p.store.InsertMetricSegmentsBatch(criticalCtx, segmentParams); err != nil {
		// Log detailed segment information for debugging
		segmentIDs := make([]int64, len(segmentParams))
		var totalRecords, totalSize int64
		for i, seg := range segmentParams {
			segmentIDs[i] = seg.SegmentID
			totalRecords += seg.RecordCount
			totalSize += seg.FileSize
		}

		dbSpan.RecordError(err)
		dbSpan.SetStatus(codes.Error, "failed to insert segments")
		dbSpan.End()

		ll.Error("Failed to insert metric segments with Kafka offsets",
			slog.Any("error", err),
			slog.Int("segmentCount", len(segmentParams)),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("totalSize", totalSize),
			slog.Any("segment_ids", segmentIDs))

		// Log individual segment keys for debugging database failures
		for i, seg := range segmentParams {
			ll.Error("InsertMetricSegment segment details",
				slog.Int("segment_index", i),
				slog.String("organization_id", seg.OrganizationID.String()),
				slog.Int("dateint", int(seg.Dateint)),
				slog.Int("frequency_ms", int(seg.FrequencyMs)),
				slog.Int64("segment_id", seg.SegmentID),
				slog.Int("instance_num", int(seg.InstanceNum)),
			)
		}

		return fmt.Errorf("failed to insert metric segments with Kafka offsets: %w", err)
	}
	dbSpan.End()

	// Send notifications to Kafka topics
	_, kafkaSpan := boxerTracer.Start(ctx, "metrics.ingest.publish_kafka", trace.WithAttributes(
		attribute.Int("segment_count", len(segmentParams)),
	))

	if p.kafkaProducer != nil {
		compactionTopic := p.config.TopicRegistry.GetTopic(config.TopicBoxerMetricsCompact)
		rollupTopic := p.config.TopicRegistry.GetTopic(config.TopicBoxerMetricsRollup)

		for _, segParams := range segmentParams {
			rollupStartTime := (segParams.StartTs / int64(segParams.FrequencyMs)) * int64(segParams.FrequencyMs)
			segmentStartTime := time.Unix(rollupStartTime/1000, (rollupStartTime%1000)*1000000)

			compactionNotification := messages.MetricCompactionMessage{
				Version:        1,
				OrganizationID: segParams.OrganizationID,
				DateInt:        segParams.Dateint,
				FrequencyMs:    segParams.FrequencyMs,
				SegmentID:      segParams.SegmentID,
				InstanceNum:    segParams.InstanceNum,
				Records:        segParams.RecordCount,
				FileSize:       segParams.FileSize,
				QueuedAt:       time.Now(),
			}

			compactionMsgBytes, err := compactionNotification.Marshal()
			if err != nil {
				kafkaSpan.RecordError(err)
				kafkaSpan.SetStatus(codes.Error, "failed to marshal compaction message")
				kafkaSpan.End()
				return fmt.Errorf("failed to marshal compaction notification: %w", err)
			}

			compactionMessage := fly.Message{
				Key:   fmt.Appendf(nil, "%s-%d-%d", segParams.OrganizationID.String(), segParams.Dateint, segParams.StartTs/300000),
				Value: compactionMsgBytes,
			}

			if err := p.kafkaProducer.Send(criticalCtx, compactionTopic, compactionMessage); err != nil {
				kafkaSpan.RecordError(err)
				kafkaSpan.SetStatus(codes.Error, "failed to send compaction to kafka")
				kafkaSpan.End()
				return fmt.Errorf("failed to send compaction notification to Kafka: %w", err)
			} else {
				ll.Debug("Sent compaction notification", slog.Any("message", compactionNotification))
			}

			// Create rollup message if this frequency can be rolled up
			var rollupMsgBytes []byte
			var rollupMessage fly.Message
			targetFrequency, ok := config.GetTargetRollupFrequency(segParams.FrequencyMs)
			if ok {
				rollupNotification := messages.MetricRollupMessage{
					Version:           1,
					OrganizationID:    segParams.OrganizationID,
					DateInt:           segParams.Dateint,
					SourceFrequencyMs: segParams.FrequencyMs,
					TargetFrequencyMs: targetFrequency,
					SegmentID:         segParams.SegmentID,
					InstanceNum:       segParams.InstanceNum,
					Records:           segParams.RecordCount,
					FileSize:          segParams.FileSize,
					SegmentStartTime:  segmentStartTime,
					QueuedAt:          time.Now(),
				}

				rollupMsgBytes, err = rollupNotification.Marshal()
				if err != nil {
					kafkaSpan.RecordError(err)
					kafkaSpan.SetStatus(codes.Error, "failed to marshal rollup message")
					kafkaSpan.End()
					return fmt.Errorf("failed to marshal rollup notification: %w", err)
				}

				rollupMessage = fly.Message{
					Key:   fmt.Appendf(nil, "%s-%d-%d-%d", segParams.OrganizationID.String(), segParams.Dateint, targetFrequency, rollupStartTime),
					Value: rollupMsgBytes,
				}

				if err := p.kafkaProducer.Send(criticalCtx, rollupTopic, rollupMessage); err != nil {
					kafkaSpan.RecordError(err)
					kafkaSpan.SetStatus(codes.Error, "failed to send rollup to kafka")
					kafkaSpan.End()
					return fmt.Errorf("failed to send rollup notification to Kafka: %w", err)
				} else {
					ll.Debug("Sent rollup notification", slog.Any("message", rollupNotification))
				}
			}
		}
	} else {
		ll.Warn("No Kafka producer provided - segment notifications will not be sent")
	}
	kafkaSpan.End()

	var totalOutputRecords, totalOutputSize int64
	for _, params := range segmentParams {
		totalOutputRecords += params.RecordCount
		totalOutputSize += params.FileSize
	}

	reportTelemetry(ctx, "metrics", "ingestion", int64(len(msgs)), int64(len(segmentParams)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Metric ingestion completed successfully",
		slog.Int("inputFiles", len(msgs)),
		slog.Int64("totalFileSize", totalInputSize),
		slog.Int("outputSegments", len(segmentParams)))

	return nil
}

// ProcessBundleFromQueue implements the BundleProcessor interface for work queue integration
func (p *MetricIngestProcessor) ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error {
	ll := logctx.FromContext(ctx)

	// Extract bundle from work item spec
	var bundle messages.MetricIngestBundle
	specBytes, err := json.Marshal(workItem.Spec())
	if err != nil {
		return fmt.Errorf("failed to marshal work item spec: %w", err)
	}

	if err := json.Unmarshal(specBytes, &bundle); err != nil {
		return fmt.Errorf("failed to unmarshal metric ingest bundle: %w", err)
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Skipping empty bundle")
		return nil
	}

	// Extract key from first message
	firstMsg := bundle.Messages[0]
	key := firstMsg.GroupingKey().(messages.IngestKey)

	// Call the existing ProcessBundle with 0 for partition and offset (not needed anymore)
	return p.ProcessBundle(ctx, key, bundle.Messages, 0, 0)
}

// GetTargetRecordCount returns the target file size limit (20MB) for accumulation
func (p *MetricIngestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return 5 * 1024 * 1024 // 5MB file size limit instead of record count
}

// ShouldEmitImmediately returns false - metric ingest always uses normal grouping.
func (p *MetricIngestProcessor) ShouldEmitImmediately(msg *messages.ObjStoreNotificationMessage) bool {
	return false
}

// uploadDuckDBResults uploads DuckDB-generated parquet files to S3 and creates segment parameters
func (p *MetricIngestProcessor) uploadDuckDBResults(ctx context.Context, storageClient cloudstorage.Client, dateintBins map[int32]*DateintBinResult, storageProfile storageprofile.StorageProfile) ([]lrdb.InsertMetricSegmentParams, error) {
	ll := logctx.FromContext(ctx)

	if len(dateintBins) == 0 {
		return nil, nil
	}

	// Generate unique batch IDs for all bins
	batchSegmentIDs := idgen.GenerateBatchIDs(len(dateintBins))

	var segmentParams []lrdb.InsertMetricSegmentParams
	i := 0

	for _, binResult := range dateintBins {
		if binResult.RecordCount == 0 {
			continue
		}

		segmentID := batchSegmentIDs[i]
		i++

		metadata := binResult.Metadata

		uploadPath := helpers.MakeDBObjectID(
			storageProfile.OrganizationID,
			storageProfile.CollectorName,
			binResult.Dateint,
			metadata.Hour,
			segmentID,
			"metrics",
		)

		// Upload file to S3
		if err := storageClient.UploadObject(ctx, storageProfile.Bucket, uploadPath, binResult.OutputFile); err != nil {
			return nil, fmt.Errorf("failed to upload file %s to %s: %w", binResult.OutputFile, uploadPath, err)
		}

		ll.Info("Uploaded metric segment",
			slog.String("bucket", storageProfile.Bucket),
			slog.String("objectID", uploadPath),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", binResult.RecordCount),
			slog.Int64("fileSize", binResult.FileSize),
			slog.Int("hour", int(metadata.Hour)))

		// Create segment parameters for database insertion
		params := lrdb.InsertMetricSegmentParams{
			OrganizationID: storageProfile.OrganizationID,
			Dateint:        binResult.Dateint,
			FrequencyMs:    10000, // 10 seconds
			SegmentID:      segmentID,
			InstanceNum:    storageProfile.InstanceNum,
			StartTs:        metadata.StartTs,
			EndTs:          metadata.EndTs,
			RecordCount:    binResult.RecordCount,
			FileSize:       binResult.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Fingerprints:   metadata.Fingerprints,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			LabelNameMap:   metadata.LabelNameMap,
			MetricNames:    metadata.MetricNames,
			MetricTypes:    metadata.MetricTypes,
		}

		segmentParams = append(segmentParams, params)
	}

	ll.Info("DuckDB segment upload completed",
		slog.Int("totalSegments", len(segmentParams)))

	return segmentParams, nil
}
