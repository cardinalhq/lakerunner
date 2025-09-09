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
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// IngestStore defines database operations needed for ingestion
type IngestStore interface {
	InsertMetricSegmentBatchWithKafkaOffsets(ctx context.Context, batch lrdb.MetricSegmentBatch) error
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// TimeBin represents a 60-second time bin with its writer and metadata
type TimeBin struct {
	StartTs int64 // Start timestamp of the bin (inclusive)
	EndTs   int64 // End timestamp of the bin (exclusive)
	Writer  parquetwriter.ParquetWriter
	Result  *parquetwriter.Result // Result after writer is closed
}

// TimeBinManager manages multiple time bins
type TimeBinManager struct {
	bins        map[int64]*TimeBin // Key is start timestamp (60s aligned)
	tmpDir      string
	rpfEstimate int64
}

// MetricIngestProcessor implements the Processor interface for raw metric ingestion
type MetricIngestProcessor struct {
	store           IngestStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	kafkaProducer   fly.Producer
}

// NewMetricIngestProcessor creates a new metric ingest processor instance
func NewMetricIngestProcessor(store IngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, kafkaProducer fly.Producer) *MetricIngestProcessor {
	return &MetricIngestProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		kafkaProducer:   kafkaProducer,
	}

}

// validateIngestGroupConsistency ensures all messages in an ingest group have consistent fields
func validateIngestGroupConsistency(group *AccumulationGroup[messages.IngestKey]) error {
	if len(group.Messages) == 0 {
		return &GroupValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	// Get expected values from the group key
	expectedOrg := group.Key.OrganizationID
	expectedInstance := group.Key.InstanceNum

	// Validate each message against the expected values
	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.ObjStoreNotificationMessage)
		if !ok {
			return &GroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not an ObjStoreNotificationMessage", i),
			}
		}

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

// Process implements the Processor interface and performs raw metric ingestion
func (p *MetricIngestProcessor) Process(ctx context.Context, group *AccumulationGroup[messages.IngestKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	// Calculate group age from Hunter timestamp
	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting metric ingestion",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	// Step 0: Validate that all messages in the group have consistent fields
	if err := validateIngestGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Create temporary directory for this ingestion run
	tmpDir, err := os.MkdirTemp("", "ingest-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	// Step 1: Get storage profile
	storageProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	// Step 2: Create storage client
	storageClient, err := cloudstorage.NewClient(ctx, p.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	// Step 3: Download files and create readers
	var readers []filereader.Reader
	var readersToClose []filereader.Reader
	var totalInputSize int64

	nowDateInt := int32(time.Now().UTC().Year()*10000 + int(time.Now().UTC().Month())*100 + time.Now().UTC().Day())

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.ObjStoreNotificationMessage)
		if !ok {
			continue // Skip non-ObjStoreNotificationMessage messages
		}

		ll.Debug("Processing raw metric file",
			slog.String("objectID", msg.ObjectID),
			slog.Int64("fileSize", msg.FileSize))

		// Step 3a: Download file
		tmpFilename, _, is404, err := storageClient.DownloadObject(ctx, tmpDir, msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to download file", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue // Skip this file but continue with others
		}
		if is404 {
			ll.Warn("Object not found, skipping", slog.String("objectID", msg.ObjectID))
			continue
		}

		// Step 3b: Create reader stack: DiskSort(Translation(OTELMetricProto(file)))
		reader, err := p.createReaderStack(tmpFilename, msg.OrganizationID.String(), msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to create reader stack", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue
		}

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
		totalInputSize += msg.FileSize
	}

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

	// Step 4: Create unified reader pipeline
	finalReader, err := p.createUnifiedReader(ctx, readers)
	if err != nil {
		return fmt.Errorf("failed to create unified reader: %w", err)
	}

	// Step 5-8: Process rows with time-based binning
	timeBins, err := p.processRowsWithTimeBinning(ctx, finalReader, tmpDir, storageProfile)
	if err != nil {
		return fmt.Errorf("failed to process rows: %w", err)
	}

	if len(timeBins) == 0 {
		ll.Info("No output files generated")
		return nil
	}

	segmentParams, err := p.uploadAndCreateSegments(ctx, storageClient, nowDateInt, timeBins, storageProfile)
	if err != nil {
		return fmt.Errorf("failed to upload and create segments: %w", err)
	}

	// Convert KafkaCommitData to KafkaOffsetUpdate slice
	var kafkaOffsets []lrdb.KafkaOffsetUpdate
	if kafkaCommitData != nil {
		for partition, offset := range kafkaCommitData.Offsets {
			kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetUpdate{
				ConsumerGroup: kafkaCommitData.ConsumerGroup,
				Topic:         kafkaCommitData.Topic,
				Partition:     partition,
				Offset:        offset,
			})
		}
	}

	batch := lrdb.MetricSegmentBatch{
		Segments:     segmentParams,
		KafkaOffsets: kafkaOffsets,
	}

	criticalCtx := context.WithoutCancel(ctx)
	if err := p.store.InsertMetricSegmentBatchWithKafkaOffsets(criticalCtx, batch); err != nil {
		// Log detailed segment information for debugging
		segmentIDs := make([]int64, len(segmentParams))
		var totalRecords, totalSize int64
		for i, seg := range segmentParams {
			segmentIDs[i] = seg.SegmentID
			totalRecords += seg.RecordCount
			totalSize += seg.FileSize
		}

		ll.Error("Failed to insert metric segments with Kafka offsets",
			slog.Any("error", err),
			slog.Any("segmentIDs", segmentIDs),
			slog.Int("segmentCount", len(segmentParams)),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("totalSize", totalSize),
			slog.String("organizationID", group.Key.OrganizationID.String()),
			slog.Int("instanceNum", int(group.Key.InstanceNum)))

		return fmt.Errorf("failed to insert metric segments with Kafka offsets: %w", err)
	}

	// Send notifications to Kafka topics
	if p.kafkaProducer != nil {
		compactionTopic := "lakerunner.segments.metrics.compact"
		rollupTopic := "lakerunner.segments.metrics.rollup"

		for _, segParams := range segmentParams {
			// Calculate rollup interval start time for consistent key generation
			rollupStartTime := (segParams.StartTs / int64(segParams.FrequencyMs)) * int64(segParams.FrequencyMs)
			segmentStartTime := time.Unix(rollupStartTime/1000, (rollupStartTime%1000)*1000000)

			// Create compaction message
			compactionNotification := messages.MetricCompactionMessage{
				Version:        1,
				OrganizationID: segParams.OrganizationID,
				DateInt:        segParams.Dateint,
				FrequencyMs:    segParams.FrequencyMs,
				SegmentID:      segParams.SegmentID,
				InstanceNum:    segParams.InstanceNum,
				SlotID:         segParams.SlotID,
				SlotCount:      segParams.SlotCount,
				Records:        segParams.RecordCount,
				FileSize:       segParams.FileSize,
				QueuedAt:       time.Now(),
			}

			// Marshal compaction message
			compactionMsgBytes, err := compactionNotification.Marshal()
			if err != nil {
				return fmt.Errorf("failed to marshal compaction notification: %w", err)
			}

			// Create rollup message if this frequency can be rolled up
			var rollupMsgBytes []byte
			if isRollupSourceFrequency(segParams.FrequencyMs) {
				targetFrequency, _ := getTargetRollupFrequency(segParams.FrequencyMs)

				rollupNotification := messages.MetricRollupMessage{
					Version:           1,
					OrganizationID:    segParams.OrganizationID,
					DateInt:           segParams.Dateint,
					SourceFrequencyMs: segParams.FrequencyMs,
					TargetFrequencyMs: targetFrequency,
					SegmentID:         segParams.SegmentID,
					InstanceNum:       segParams.InstanceNum,
					SlotID:            segParams.SlotID,
					SlotCount:         segParams.SlotCount,
					Records:           segParams.RecordCount,
					FileSize:          segParams.FileSize,
					SegmentStartTime:  segmentStartTime,
					QueuedAt:          time.Now(),
				}

				rollupMsgBytes, err = rollupNotification.Marshal()
				if err != nil {
					return fmt.Errorf("failed to marshal rollup notification: %w", err)
				}
			}

			compactionMessage := fly.Message{
				Key:   []byte(fmt.Sprintf("%s-%d-%d", segParams.OrganizationID.String(), segParams.Dateint, segParams.SegmentID)),
				Value: compactionMsgBytes,
			}

			rollupMessage := fly.Message{
				Key:   []byte(fmt.Sprintf("%s-%d-%d-%d", segParams.OrganizationID.String(), segParams.Dateint, segParams.FrequencyMs, rollupStartTime)),
				Value: rollupMsgBytes,
			}

			// Send to compaction topic
			if err := p.kafkaProducer.Send(criticalCtx, compactionTopic, compactionMessage); err != nil {
				return fmt.Errorf("failed to send compaction notification to Kafka: %w", err)
			}

			// Send to rollup topic only if rollup message was created
			if rollupMsgBytes != nil {
				if err := p.kafkaProducer.Send(criticalCtx, rollupTopic, rollupMessage); err != nil {
					return fmt.Errorf("failed to send rollup notification to Kafka: %w", err)
				}
			}
		}
		ll.Debug("Sent segment notifications to Kafka topics",
			slog.Int("count", len(segmentParams)))
	} else {
		ll.Warn("No Kafka producer provided - segment notifications will not be sent")
	}

	// Calculate output metrics for telemetry
	var totalOutputRecords, totalOutputSize int64
	for _, params := range segmentParams {
		totalOutputRecords += params.RecordCount
		totalOutputSize += params.FileSize
	}

	// Report telemetry - ingestion transforms files into segments
	ReportTelemetry(ctx, "ingestion", int64(len(group.Messages)), int64(len(segmentParams)), 0, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Metric ingestion completed successfully",
		slog.Int("inputFiles", len(group.Messages)),
		slog.Int64("totalFileSize", totalInputSize),
		slog.Int("outputSegments", len(segmentParams)))

	return nil
}

// GetTargetRecordCount returns the target file size limit (20MB) for accumulation
func (p *MetricIngestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return 5 * 1024 * 1024 // 5MB file size limit instead of record count
}

// createReaderStack creates a reader stack: DiskSort(Translation(OTELMetricProto(file)))
func (p *MetricIngestProcessor) createReaderStack(tmpFilename, orgID, bucket, objectID string) (filereader.Reader, error) {
	// Step 1: Create proto reader for .binpb or .binpb.gz files
	reader, err := CreateMetricProtoReader(tmpFilename, filereader.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create proto reader: %w", err)
	}

	// Step 2: Add translation (adds TID and truncates timestamp)
	translator := &MetricTranslator{
		OrgID:    orgID,
		Bucket:   bucket,
		ObjectID: objectID,
	}
	reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("failed to create translating reader: %w", err)
	}

	// Step 3: Add disk-based sorting (after translation so TID is available)
	keyProvider := filereader.GetCurrentMetricSortKeyProvider()
	reader, err = filereader.NewDiskSortingReader(reader, keyProvider, 1000)
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("failed to create sorting reader: %w", err)
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

// processRowsWithTimeBinning processes rows using 60s time-based binning
func (p *MetricIngestProcessor) processRowsWithTimeBinning(ctx context.Context, reader filereader.Reader, tmpDir string, storageProfile storageprofile.StorageProfile) (map[int64]*TimeBin, error) {
	ll := logctx.FromContext(ctx)

	// Get RPF estimate for this org/instance
	rpfEstimate := p.getRPFEstimate(ctx, storageProfile.OrganizationID)

	// Create time bin manager
	binManager := &TimeBinManager{
		bins:        make(map[int64]*TimeBin),
		tmpDir:      tmpDir,
		rpfEstimate: rpfEstimate,
	}

	var totalRowsProcessed int64

	// Process all rows from the reader
	for {
		batch, readErr := reader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		if batch != nil {
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

				// Calculate 60-second aligned bin
				binStartTs := (ts / 60000) * 60000 // Truncate to 60s boundary

				// Get or create time bin
				bin, err := binManager.getOrCreateBin(ctx, binStartTs)
				if err != nil {
					ll.Error("Failed to get/create time bin", slog.Int64("binStartTs", binStartTs), slog.Any("error", err))
					continue
				}

				// Create a single-row batch for this bin
				singleRowBatch := pipeline.GetBatch()
				newRow := singleRowBatch.AddRow()
				for k, v := range row {
					newRow[k] = v
				}

				// Write to the bin's writer
				if err := bin.Writer.WriteBatch(singleRowBatch); err != nil {
					ll.Error("Failed to write row to time bin",
						slog.Int64("binStartTs", binStartTs),
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

	ll.Info("Time binning completed",
		slog.Int64("rowsProcessed", totalRowsProcessed),
		slog.Int("binsCreated", len(binManager.bins)))

	// Close all writers and collect results
	for binStartTs, bin := range binManager.bins {
		results, err := bin.Writer.Close(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to close writer for bin %d: %w", binStartTs, err)
		}

		if len(results) > 0 {
			// Should typically be one result per writer
			bin.Result = &results[0]
		}
	}

	return binManager.bins, nil
}

// getOrCreateBin gets or creates a time bin for the given start timestamp
func (manager *TimeBinManager) getOrCreateBin(_ context.Context, binStartTs int64) (*TimeBin, error) {
	if bin, exists := manager.bins[binStartTs]; exists {
		return bin, nil
	}

	// Create new writer for this time bin
	writer, err := factories.NewMetricsWriter(manager.tmpDir, manager.rpfEstimate)
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
func (p *MetricIngestProcessor) uploadAndCreateSegments(ctx context.Context, storageClient cloudstorage.Client, nowDateInt int32, timeBins map[int64]*TimeBin, storageProfile storageprofile.StorageProfile) ([]lrdb.InsertMetricSegmentParams, error) {
	ll := logctx.FromContext(ctx)

	// Get partition count once per batch for rollup topic
	rollupTopic := "lakerunner.segments.metrics.rollup"
	partitionCount, err := p.kafkaProducer.GetPartitionCount(rollupTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition count for topic %s: %w", rollupTopic, err)
	}

	var segmentParams []lrdb.InsertMetricSegmentParams
	var totalOutputRecords, totalOutputSize int64

	for binStartTs, bin := range timeBins {
		if bin.Result == nil || bin.Result.RecordCount == 0 {
			ll.Debug("Skipping empty time bin", slog.Int64("binStartTs", binStartTs))
			continue
		}

		// Extract file metadata using ExtractFileMetadata
		metadata, err := ExtractFileMetadata(ctx, *bin.Result)
		if err != nil {
			return nil, fmt.Errorf("failed to extract file metadata for bin %d: %w", binStartTs, err)
		}

		// Generate upload path using pathnames utility
		collectorName := helpers.ExtractCollectorName("otel-raw/" + storageProfile.OrganizationID.String())
		if collectorName == "" {
			collectorName = storageProfile.CollectorName
		}

		segmentID := idgen.GenerateID()

		// Generate upload path using metadata dateint and hour
		uploadPath := helpers.MakeDBObjectID(
			storageProfile.OrganizationID,
			collectorName,
			metadata.Dateint,
			metadata.Hour,
			segmentID,
			"metrics",
		)

		// Upload file to S3
		uploadErr := storageClient.UploadObject(ctx, storageProfile.Bucket, uploadPath, bin.Result.FileName)
		if uploadErr != nil {
			return nil, fmt.Errorf("failed to upload file %s to %s: %w", bin.Result.FileName, uploadPath, uploadErr)
		}

		// Compute slot_id and slot_count based on partition count
		slotID, slotCount := computeMetricSlot(storageProfile.OrganizationID, storageProfile.InstanceNum, 60000, binStartTs, int32(partitionCount))

		ll.Debug("Uploaded segment",
			slog.String("uploadPath", uploadPath),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", bin.Result.RecordCount),
			slog.Int64("fileSize", bin.Result.FileSize),
			slog.Int("slotID", int(slotID)),
			slog.Int("slotCount", int(slotCount)))

		// Create segment parameters for database insertion using extracted metadata
		params := lrdb.InsertMetricSegmentParams{
			OrganizationID: storageProfile.OrganizationID,
			Dateint:        metadata.Dateint,
			IngestDateint:  nowDateInt,
			FrequencyMs:    10000, // 10 seconds
			SegmentID:      segmentID,
			InstanceNum:    storageProfile.InstanceNum,
			SlotID:         slotID,
			SlotCount:      slotCount,
			StartTs:        metadata.StartTs,
			EndTs:          metadata.EndTs,
			RecordCount:    bin.Result.RecordCount,
			FileSize:       bin.Result.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Published:      true,
			Compacted:      false,
			Fingerprints:   metadata.Fingerprints,
			SortVersion:    lrdb.CurrentMetricSortVersion,
		}

		segmentParams = append(segmentParams, params)
		totalOutputRecords += bin.Result.RecordCount
		totalOutputSize += bin.Result.FileSize
	}

	ll.Info("Segment upload completed",
		slog.Int("totalSegments", len(segmentParams)))

	return segmentParams, nil
}

// getRPFEstimate gets the RPF estimate for a specific org using the store's estimator
func (p *MetricIngestProcessor) getRPFEstimate(ctx context.Context, orgID uuid.UUID) int64 {
	return p.store.GetMetricEstimate(ctx, orgID, 10000) // 10 second blocks
}

// computeMetricSlot determines the slot_id and slot_count for a metric segment
// based on orgID, instanceNum, frequency, and truncated 60s timestamp
func computeMetricSlot(orgID uuid.UUID, instanceNum int16, frequencyMs int32, truncatedTimestamp int64, partitionCount int32) (slotID int32, slotCount int32) {
	// Create a unique key combining all parameters
	key := fmt.Sprintf("%s_%d_%d_%d", orgID.String(), instanceNum, frequencyMs, truncatedTimestamp)

	// Hash the key to get a deterministic slot assignment
	h := sha256.Sum256([]byte(key))

	// Use the first 2 bytes of the hash to get a 16-bit number, then modulo by partition count
	slotID = int32(binary.BigEndian.Uint16(h[:])) % partitionCount
	slotCount = partitionCount

	return slotID, slotCount
}
