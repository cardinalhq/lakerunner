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

package metricsprocessing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/workqueue"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TimeBin represents a 60-second file group containing 10s aggregated data points
type TimeBin struct {
	StartTs int64 // Start timestamp of the file group (inclusive)
	EndTs   int64 // End timestamp of the file group (exclusive)
	Writer  parquetwriter.ParquetWriter
	Results []parquetwriter.Result // Results after writer is closed (can be multiple files)
}

// TimeBinManager manages multiple file groups (60s-aligned files containing 10s data)
type TimeBinManager struct {
	bins        map[int64]*TimeBin // Key is start timestamp (60s aligned)
	tmpDir      string
	rpfEstimate int64
	schema      *filereader.ReaderSchema
}

// MetricIngestProcessor implements the Processor interface for raw metric ingestion
type MetricIngestProcessor struct {
	store             MetricIngestStore
	storageProvider   storageprofile.StorageProfileProvider
	cmgr              cloudstorage.ClientProvider
	kafkaProducer     fly.Producer
	exemplarProcessor *exemplars.Processor
	config            *config.Config
}

// newMetricIngestProcessor creates a new metric ingest processor instance
func newMetricIngestProcessor(
	cfg *config.Config,
	store MetricIngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, kafkaProducer fly.Producer) *MetricIngestProcessor {
	exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
	exemplarProcessor.SetMetricsCallback(func(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row) error {
		return processMetricsExemplarsDirect(ctx, organizationID, rows, store)
	})

	return &MetricIngestProcessor{
		store:             store,
		storageProvider:   storageProvider,
		cmgr:              cmgr,
		kafkaProducer:     kafkaProducer,
		exemplarProcessor: exemplarProcessor,
		config:            cfg,
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

	var readers []filereader.Reader
	var readersToClose []filereader.Reader
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

		reader, err := p.createReaderStack(tmpFilename, msg.OrganizationID.String(), msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to create reader stack", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue
		}

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
		totalInputSize += msg.FileSize
	}

	downloadSpan.SetAttributes(
		attribute.Int("files_downloaded", len(readers)),
		attribute.Int64("total_input_size", totalInputSize),
	)
	downloadSpan.End()

	// Cleanup readers on exit
	defer func() {
		for _, reader := range readersToClose {
			if closeErr := reader.Close(); closeErr != nil {
				ll.Warn("Failed to close reader during cleanup", slog.Any("error", closeErr))
			}
		}
	}()

	if len(readers) == 0 {
		ll.Info("No files processed successfully")
		return nil
	}

	ctx, readerSpan := boxerTracer.Start(ctx, "metrics.ingest.create_unified_reader")
	finalReader, err := p.createUnifiedReader(ctx, readers)
	if err != nil {
		readerSpan.RecordError(err)
		readerSpan.SetStatus(codes.Error, "failed to create unified reader")
		readerSpan.End()
		return fmt.Errorf("failed to create unified reader: %w", err)
	}
	readerSpan.End()

	ctx, processSpan := boxerTracer.Start(ctx, "metrics.ingest.process_rows")
	timeBins, totalInputRecords, err := p.processRowsWithTimeBinning(ctx, finalReader, tmpDir, srcProfile)
	if err != nil {
		processSpan.RecordError(err)
		processSpan.SetStatus(codes.Error, "failed to process rows")
		processSpan.End()
		return fmt.Errorf("failed to process rows: %w", err)
	}
	processSpan.SetAttributes(attribute.Int("time_bins", len(timeBins)))
	processSpan.End()

	if len(timeBins) == 0 {
		ll.Info("No output files generated")
		return nil
	}

	ctx, uploadSpan := boxerTracer.Start(ctx, "metrics.ingest.upload_segments")
	segmentParams, err := p.uploadAndCreateSegments(ctx, outputClient, timeBins, dstProfile)
	if err != nil {
		uploadSpan.RecordError(err)
		uploadSpan.SetStatus(codes.Error, "failed to upload segments")
		uploadSpan.End()
		return fmt.Errorf("failed to upload and create segments: %w", err)
	}
	uploadSpan.SetAttributes(attribute.Int("segment_count", len(segmentParams)))
	uploadSpan.End()

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

// createReaderStack creates a reader stack: DiskSort(Translation(OTELMetricProto(file)))
func (p *MetricIngestProcessor) createReaderStack(tmpFilename, orgID, bucket, objectID string) (filereader.Reader, error) {
	// Determine file type from extension for logging
	var fileType string
	switch {
	case strings.HasSuffix(tmpFilename, ".binpb.gz"):
		fileType = "binpb.gz"
	case strings.HasSuffix(tmpFilename, ".binpb"):
		fileType = "binpb"
	default:
		fileType = "unknown"
	}

	slog.Info("Reading metric file",
		"fileType", fileType,
		"objectID", objectID,
		"bucket", bucket)

	// Use the sorting reader that combines proto parsing, translation, and in-memory sorting
	reader, err := createSortingMetricProtoReader(tmpFilename, orgID, bucket, objectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create sorting proto reader: %w", err)
	}

	return reader, nil
}

// createUnifiedReader creates a unified reader from multiple readers
func (p *MetricIngestProcessor) createUnifiedReader(ctx context.Context, readers []filereader.Reader) (filereader.Reader, error) {
	var finalReader filereader.Reader

	if len(readers) == 1 {
		finalReader = readers[0]
	} else {
		keyProvider := filereader.GetCurrentMetricSortKeyProvider()
		multiReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to create multi-source reader: %w", err)
		}
		finalReader = multiReader
	}

	// Add aggregation with 10-second window
	finalReader, err := filereader.NewAggregatingMetricsReader(finalReader, 10000, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregating reader: %w", err)
	}

	return finalReader, nil
}

// processRowsWithTimeBinning groups 10s aggregated data into 60s-aligned files
func (p *MetricIngestProcessor) processRowsWithTimeBinning(ctx context.Context, reader filereader.Reader, tmpDir string, storageProfile storageprofile.StorageProfile) (map[int64]*TimeBin, int64, error) {
	ll := logctx.FromContext(ctx)

	// Get schema from reader (GetSchema returns a copy)
	schema := reader.GetSchema()

	// Add columns that will be injected by MetricTranslator
	// These columns are added to every row but aren't in the OTEL schema
	schema.AddColumn(wkk.RowKeyCCustomerID, wkk.RowKeyCCustomerID, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTelemetryType, wkk.RowKeyCTelemetryType, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTID, wkk.RowKeyCTID, filereader.DataTypeInt64, true)

	// Get RPF estimate for this org/instance
	rpfEstimate := p.store.GetMetricEstimate(ctx, storageProfile.OrganizationID, 10000) // 10 second blocks

	ll.Info("Starting file grouping with estimated records per file",
		slog.Int64("recordsPerFile", rpfEstimate))

	// Create time bin manager
	binManager := &TimeBinManager{
		bins:        make(map[int64]*TimeBin),
		tmpDir:      tmpDir,
		rpfEstimate: rpfEstimate,
		schema:      schema,
	}

	var totalRowsRead, totalRowsProcessed int64

	// Process all rows from the reader
	for {
		batch, readErr := reader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, 0, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		if batch != nil {
			totalRowsRead += int64(batch.Len())
			// Process each row in the batch
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				if row == nil {
					continue
				}

				// Extract timestamp to determine which bin this row belongs to
				ts, ok := row[wkk.RowKeyCTimestamp].(int64)
				if !ok {
					ll.Warn("Row missing timestamp, skipping", slog.Int("rowIndex", i))
					continue
				}

				// Group 10s aggregated data into 60s-aligned files
				// Since data is already aggregated to 10s, we group 6 data points per file
				fileGroupStartTs := (ts / 60000) * 60000

				// Get or create time bin
				bin, err := binManager.getOrCreateBin(ctx, fileGroupStartTs)
				if err != nil {
					ll.Error("Failed to get/create time bin", slog.Int64("fileGroupStartTs", fileGroupStartTs), slog.Any("error", err))
					continue
				}

				// Process exemplar before taking the row
				if p.exemplarProcessor != nil {
					_ = p.exemplarProcessor.ProcessMetricsFromRow(ctx, storageProfile.OrganizationID, row)
				}

				takenRow := batch.TakeRow(i)
				if takenRow == nil {
					continue
				}
				singleRowBatch := pipeline.GetBatch()
				singleRowBatch.AppendRow(takenRow)

				// Write to the bin's writer
				if err := bin.Writer.WriteBatch(singleRowBatch); err != nil {
					ll.Error("Failed to write row to file group",
						slog.Int64("fileGroupStartTs", fileGroupStartTs),
						slog.Any("error", err))
				} else {
					totalRowsProcessed++
				}

				pipeline.ReturnBatch(singleRowBatch)
			}
			pipeline.ReturnBatch(batch)
		}

		if readErr == io.EOF {
			break
		}
	}

	ll.Info("File grouping completed",
		slog.Int64("rowsRead", totalRowsRead),
		slog.Int64("rowsProcessed", totalRowsProcessed),
		slog.Int("fileGroupsCreated", len(binManager.bins)))

	// Close all writers and collect results
	for binStartTs, bin := range binManager.bins {
		results, err := bin.Writer.Close(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to close writer for bin %d: %w", binStartTs, err)
		}

		if len(results) > 0 {
			bin.Results = append(bin.Results, results...)
		}
	}

	return binManager.bins, totalRowsRead, nil
}

// getOrCreateBin gets or creates a time bin for the given start timestamp
func (manager *TimeBinManager) getOrCreateBin(_ context.Context, binStartTs int64) (*TimeBin, error) {
	if bin, exists := manager.bins[binStartTs]; exists {
		return bin, nil
	}

	writer, err := factories.NewMetricsWriter(manager.tmpDir, manager.schema, manager.rpfEstimate)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer for time bin: %w", err)
	}

	bin := &TimeBin{
		StartTs: binStartTs,
		EndTs:   binStartTs + 60000, // 60 seconds
		Writer:  writer,
	}

	manager.bins[binStartTs] = bin
	return bin, nil
}

// uploadAndCreateSegments uploads time bins to S3 and creates segment parameters
func (p *MetricIngestProcessor) uploadAndCreateSegments(ctx context.Context, storageClient cloudstorage.Client, timeBins map[int64]*TimeBin, storageProfile storageprofile.StorageProfile) ([]lrdb.InsertMetricSegmentParams, error) {
	ll := logctx.FromContext(ctx)

	var segmentParams []lrdb.InsertMetricSegmentParams
	var totalOutputRecords, totalOutputSize int64

	type validResult struct {
		binStartTs int64
		result     parquetwriter.Result
		metadata   *fileMetadata
	}
	var validResults []validResult

	// Collect all valid results from all bins
	for binStartTs, bin := range timeBins {
		if len(bin.Results) == 0 {
			ll.Debug("Skipping empty file group", slog.Int64("fileGroupStartTs", binStartTs))
			continue
		}

		// Process each result from this bin
		for _, result := range bin.Results {
			if result.RecordCount == 0 {
				continue
			}

			metadata, err := extractFileMetadata(ctx, result)
			if err != nil {
				return nil, fmt.Errorf("failed to extract file metadata for bin %d: %w", binStartTs, err)
			}

			validResults = append(validResults, validResult{
				binStartTs: binStartTs,
				result:     result,
				metadata:   metadata,
			})
		}
	}

	// Generate unique batch IDs for all valid results to avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(validResults))

	for i, valid := range validResults {
		result := valid.result
		metadata := valid.metadata

		segmentID := batchSegmentIDs[i]

		uploadPath := helpers.MakeDBObjectID(
			storageProfile.OrganizationID,
			storageProfile.CollectorName,
			metadata.Dateint,
			metadata.Hour,
			segmentID,
			"metrics",
		)

		// Upload file to S3
		uploadErr := storageClient.UploadObject(ctx, storageProfile.Bucket, uploadPath, result.FileName)
		if uploadErr != nil {
			return nil, fmt.Errorf("failed to upload file %s to %s: %w", result.FileName, uploadPath, uploadErr)
		}

		ll.Debug("Uploaded segment",
			slog.String("uploadPath", uploadPath),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", result.RecordCount),
			slog.Int64("fileSize", result.FileSize))

		// Create segment parameters for database insertion using extracted metadata
		params := lrdb.InsertMetricSegmentParams{
			OrganizationID: storageProfile.OrganizationID,
			Dateint:        metadata.Dateint,
			FrequencyMs:    10000, // 10 seconds
			SegmentID:      segmentID,
			InstanceNum:    storageProfile.InstanceNum,
			StartTs:        metadata.StartTs,
			EndTs:          metadata.EndTs,
			RecordCount:    result.RecordCount,
			FileSize:       result.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Fingerprints:   metadata.Fingerprints,
			SortVersion:    lrdb.CurrentMetricSortVersion,
			LabelNameMap:   metadata.LabelNameMap,
			MetricNames:    metadata.MetricNames,
			MetricTypes:    metadata.MetricTypes,
		}

		ll.Debug("Created segment params",
			slog.Int64("segmentID", segmentID),
			slog.Bool("hasLabelNameMap", metadata.LabelNameMap != nil),
			slog.Int("labelNameMapSize", len(metadata.LabelNameMap)))

		segmentParams = append(segmentParams, params)
		totalOutputRecords += result.RecordCount
		totalOutputSize += result.FileSize
	}

	ll.Info("Segment upload completed",
		slog.Int("totalSegments", len(segmentParams)))

	return segmentParams, nil
}
