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
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// RollupStore defines database operations needed for rollups
type RollupStore interface {
	GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error)
	RollupMetricSegsWithKafkaOffsets(ctx context.Context, sourceParams lrdb.RollupSourceParams, targetParams lrdb.RollupTargetParams, sourceSegmentIDs []int64, newRecords []lrdb.RollupNewRecord, kafkaOffsets []lrdb.KafkaOffsetUpdate) error
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
	// Add segment_journal functionality
	InsertSegmentJournal(ctx context.Context, params lrdb.InsertSegmentJournalParams) error
}

// MetricRollupProcessor implements the Processor interface for metric rollups
type MetricRollupProcessor struct {
	store           RollupStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
	kafkaProducer   fly.Producer
}

// newMetricRollupProcessor creates a new metric rollup processor instance
func newMetricRollupProcessor(store RollupStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, cfg *config.Config, kafkaProducer fly.Producer) *MetricRollupProcessor {
	return &MetricRollupProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
		kafkaProducer:   kafkaProducer,
	}
}

// validateBundleConsistency ensures all messages in a rollup bundle have consistent fields
func (r *MetricRollupProcessor) validateBundleConsistency(bundle *messages.MetricRollupBundle) error {
	if len(bundle.Messages) == 0 {
		return &GroupValidationError{
			Field:   "message_count",
			Message: "bundle cannot be empty",
		}
	}

	// Get expected values from the first message
	firstMsg := bundle.Messages[0]
	expectedOrg := firstMsg.OrganizationID
	expectedInstance := firstMsg.InstanceNum
	expectedDateInt := firstMsg.DateInt
	expectedSourceFreq := firstMsg.SourceFrequencyMs
	expectedTargetFreq := firstMsg.TargetFrequencyMs

	// Validate each message against the expected values (including first message to maintain loop structure)
	for i, msg := range bundle.Messages {
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

		if msg.DateInt != expectedDateInt {
			return &GroupValidationError{
				Field:    "date_int",
				Expected: expectedDateInt,
				Got:      msg.DateInt,
				Message:  fmt.Sprintf("message %d has inconsistent date int", i),
			}
		}

		if msg.SourceFrequencyMs != expectedSourceFreq {
			return &GroupValidationError{
				Field:    "source_frequency_ms",
				Expected: expectedSourceFreq,
				Got:      msg.SourceFrequencyMs,
				Message:  fmt.Sprintf("message %d has inconsistent source frequency", i),
			}
		}

		if msg.TargetFrequencyMs != expectedTargetFreq {
			return &GroupValidationError{
				Field:    "target_frequency_ms",
				Expected: expectedTargetFreq,
				Got:      msg.TargetFrequencyMs,
				Message:  fmt.Sprintf("message %d has inconsistent target frequency", i),
			}
		}
	}

	return nil
}

// ProcessBundle processes a MetricRollupBundle directly (simplified interface)
func (r *MetricRollupProcessor) ProcessBundle(ctx context.Context, bundle *messages.MetricRollupBundle, partition int32, offset int64) error {
	ll := logctx.FromContext(ctx)

	if err := r.validateBundleConsistency(bundle); err != nil {
		return fmt.Errorf("bundle validation failed: %w", err)
	}

	firstMsg := bundle.Messages[0]

	targetFrequencyDuration := time.Duration(firstMsg.TargetFrequencyMs) * time.Millisecond
	truncatedTimebox := firstMsg.SegmentStartTime.Truncate(targetFrequencyDuration).Unix()
	key := messages.RollupKey{
		OrganizationID:    firstMsg.OrganizationID,
		DateInt:           firstMsg.DateInt,
		SourceFrequencyMs: firstMsg.SourceFrequencyMs,
		TargetFrequencyMs: firstMsg.TargetFrequencyMs,
		InstanceNum:       firstMsg.InstanceNum,
		TruncatedTimebox:  truncatedTimebox,
	}

	ll.Info("Starting rollup processing",
		slog.String("organizationID", firstMsg.OrganizationID.String()),
		slog.Int("dateint", int(firstMsg.DateInt)),
		slog.Int("sourceFrequencyMs", int(firstMsg.SourceFrequencyMs)),
		slog.Int("targetFrequencyMs", int(firstMsg.TargetFrequencyMs)),
		slog.Int("instanceNum", int(firstMsg.InstanceNum)),
		slog.Int64("truncatedTimebox", truncatedTimebox),
		slog.Int("messageCount", len(bundle.Messages)))

	recordCountEstimate := r.store.GetMetricEstimate(ctx, firstMsg.OrganizationID, firstMsg.TargetFrequencyMs)

	// Create temporary directory for this rollup run
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	storageProfile, err := r.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, firstMsg.OrganizationID, firstMsg.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	storageClient, err := cloudstorage.NewClient(ctx, r.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	var segments []lrdb.MetricSeg
	for _, msg := range bundle.Messages {
		segment, err := r.store.GetMetricSeg(ctx, lrdb.GetMetricSegParams{
			OrganizationID: msg.OrganizationID,
			Dateint:        msg.DateInt,
			FrequencyMs:    msg.SourceFrequencyMs, // Use source frequency for lookup
			SegmentID:      msg.SegmentID,
			InstanceNum:    msg.InstanceNum,
		})
		if err != nil {
			ll.Warn("Failed to fetch segment, skipping",
				slog.Int64("segmentID", msg.SegmentID),
				slog.String("organizationID", msg.OrganizationID.String()),
				slog.Int("dateint", int(msg.DateInt)),
				slog.Int("sourceFrequencyMs", int(msg.SourceFrequencyMs)),
				slog.Int("instanceNum", int(msg.InstanceNum)),
				slog.Any("error", err))
			continue
		}

		// Only include segments not already rolled up
		if !segment.Rolledup {
			segments = append(segments, segment)
		}
	}

	if len(segments) == 0 {
		ll.Info("No segments to roll up")
		return nil
	}

	ll.Info("Found segments to roll up",
		slog.Int("segmentCount", len(segments)))

	params := metricProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: key.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: segments,
		FrequencyMs:    key.TargetFrequencyMs,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	result, err := processMetricsWithAggregation(ctx, params)
	if err != nil {
		return err
	}

	results := result.Results

	newSegments, err := r.uploadAndCreateRollupSegments(ctx, storageClient, storageProfile, results, key, segments)
	if err != nil {
		return fmt.Errorf("upload and create rollup segments: %w", err)
	}

	// Create simplified Kafka commit data
	kafkaCommitData := &KafkaCommitData{
		Topic:         "lakerunner.segments.metrics.rollup",
		ConsumerGroup: "lakerunner.rollup.metrics",
		Offsets: map[int32]int64{
			partition: offset + 1,
		},
	}

	if err := r.atomicDatabaseUpdate(ctx, segments, newSegments, kafkaCommitData, key); err != nil {
		return fmt.Errorf("atomic database update: %w", err)
	}

	var totalRecords, totalSize int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalSize += result.FileSize
	}

	// Log the rollup operation to segment_journal for debugging (if enabled)
	if r.cfg.SegLog.Enabled {
		if err := r.logRollupOperation(ctx, storageProfile, segments, newSegments, results, key, recordCountEstimate); err != nil {
			// Don't fail the rollup if seg_log fails - this is just for debugging
			ll.Warn("Failed to log rollup operation to segment_journal", slog.Any("error", err))
		}
	}

	// Generate next-level rollup messages if the target frequency can be rolled up further
	if err := r.queueNextLevelRollups(ctx, newSegments, key); err != nil {
		// Don't fail the rollup if next-level rollup queueing fails - log and continue
		ll.Warn("Failed to queue next-level rollup messages", slog.Any("error", err))
	}

	// Queue compaction messages for the target frequency segments
	if err := r.queueCompactionMessages(ctx, newSegments, key); err != nil {
		// Don't fail the rollup if compaction queueing fails - log and continue
		ll.Warn("Failed to queue compaction messages", slog.Any("error", err))
	}

	ll.Info("Rollup completed successfully",
		slog.Int("inputSegments", len(segments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))

	return nil
}

// uploadAndCreateRollupSegments uploads the rollup files and creates new segment records at target frequency
func (r *MetricRollupProcessor) uploadAndCreateRollupSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.RollupKey, inputSegments []lrdb.MetricSeg) ([]lrdb.MetricSeg, error) {
	ll := logctx.FromContext(ctx)

	// Calculate input metrics
	var totalInputSize, totalInputRecords int64
	for _, seg := range inputSegments {
		totalInputSize += seg.FileSize
		totalInputRecords += seg.RecordCount
	}

	var segments []lrdb.MetricSeg
	var totalOutputSize, totalOutputRecords int64
	var segmentIDs []int64

	// Generate unique batch IDs for all results to avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(results))

	for i, result := range results {
		segmentID := batchSegmentIDs[i]

		stats, ok := result.Metadata.(factories.MetricsFileStats)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}

		// Upload the file
		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, r.getHourFromTimestamp(stats.FirstTS), segmentID, "metrics")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload file %s: %w", result.FileName, err)
		}

		// Clean up local file
		os.Remove(result.FileName)

		// Create new segment record at TARGET frequency
		segment := lrdb.MetricSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
			FrequencyMs:    key.TargetFrequencyMs, // Store at target frequency
			SegmentID:      segmentID,
			InstanceNum:    key.InstanceNum,
			TsRange: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Int8{Int64: stats.FirstTS, Valid: true},
				Upper:     pgtype.Int8{Int64: stats.LastTS + 1, Valid: true},
				Valid:     true,
			},
			RecordCount:  result.RecordCount,
			FileSize:     result.FileSize,
			Published:    true,
			Compacted:    false,
			Rolledup:     false,
			Fingerprints: stats.Fingerprints,
			SortVersion:  lrdb.CurrentMetricSortVersion,
			CreatedBy:    lrdb.CreateByRollup,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount
		segmentIDs = append(segmentIDs, segmentID)
	}

	// Report telemetry
	reportTelemetry(ctx, "rollup", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	// Log upload summary
	ll.Info("Rollup segment upload completed",
		slog.Int("inputFiles", len(inputSegments)),
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Any("createdSegmentIDs", segmentIDs))

	return segments, nil
}

// atomicDatabaseUpdate performs the atomic transaction for rollup completion
func (r *MetricRollupProcessor) atomicDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.MetricSeg, kafkaCommitData *KafkaCommitData, key messages.RollupKey) error {
	ll := logctx.FromContext(ctx)

	// Extract segment IDs from old segments
	sourceSegmentIDs := make([]int64, len(oldSegments))
	for i, seg := range oldSegments {
		sourceSegmentIDs[i] = seg.SegmentID
	}

	// Convert new segments to RollupNewRecord format
	newRecords := make([]lrdb.RollupNewRecord, len(newSegments))
	for i, seg := range newSegments {
		newRecords[i] = lrdb.RollupNewRecord{
			SegmentID:    seg.SegmentID,
			StartTs:      seg.TsRange.Lower.Int64,
			EndTs:        seg.TsRange.Upper.Int64,
			RecordCount:  seg.RecordCount,
			FileSize:     seg.FileSize,
			Fingerprints: seg.Fingerprints,
		}
	}

	sourceParams := lrdb.RollupSourceParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		FrequencyMs:    key.SourceFrequencyMs,
		InstanceNum:    key.InstanceNum,
	}

	targetParams := lrdb.RollupTargetParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		FrequencyMs:    key.TargetFrequencyMs,
		InstanceNum:    key.InstanceNum,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	}

	// Prepare Kafka offsets for atomic update
	var kafkaOffsets []lrdb.KafkaOffsetUpdate
	if kafkaCommitData != nil {
		for partition, offset := range kafkaCommitData.Offsets {
			kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetUpdate{
				Topic:               kafkaCommitData.Topic,
				Partition:           partition,
				ConsumerGroup:       kafkaCommitData.ConsumerGroup,
				OrganizationID:      key.OrganizationID,
				InstanceNum:         key.InstanceNum,
				LastProcessedOffset: offset,
			})

			// Log each Kafka offset update
			ll.Info("Updating Kafka consumer group offset for rollup",
				slog.String("consumerGroup", kafkaCommitData.ConsumerGroup),
				slog.String("topic", kafkaCommitData.Topic),
				slog.Int("partition", int(partition)),
				slog.Int64("newOffset", offset))
		}
	}

	// Use the atomic rollup function that handles everything in one transaction
	if err := r.store.RollupMetricSegsWithKafkaOffsets(ctx, sourceParams, targetParams, sourceSegmentIDs, newRecords, kafkaOffsets); err != nil {
		ll := logctx.FromContext(ctx)

		// Log unique keys for debugging database failures
		ll.Error("Failed RollupMetricSegsWithKafkaOffsetsWithOrg",
			slog.Any("error", err),
			slog.String("organization_id", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("source_frequency_ms", int(key.SourceFrequencyMs)),
			slog.Int("target_frequency_ms", int(key.TargetFrequencyMs)),
			slog.Int("instance_num", int(key.InstanceNum)),
			slog.Int("source_segments_count", len(sourceSegmentIDs)),
			slog.Int("new_segments_count", len(newRecords)))

		// Log segment IDs for additional context
		if len(sourceSegmentIDs) > 0 {
			ll.Error("RollupMetricSegs source segment IDs",
				slog.Any("source_segment_ids", sourceSegmentIDs))
		}

		if len(newRecords) > 0 {
			newSegmentIDs := make([]int64, len(newRecords))
			for i, record := range newRecords {
				newSegmentIDs[i] = record.SegmentID
			}
			ll.Error("RollupMetricSegs new segment IDs",
				slog.Any("new_segment_ids", newSegmentIDs))
		}

		return fmt.Errorf("failed to rollup metric segments: %w", err)
	}

	return nil
}

// logRollupOperation logs the rollup operation to segment_journal for debugging purposes
func (r *MetricRollupProcessor) logRollupOperation(ctx context.Context, storageProfile storageprofile.StorageProfile, inputSegments, outputSegments []lrdb.MetricSeg, results []parquetwriter.Result, key messages.RollupKey, recordEstimate int64) error {
	return logSegmentOperation(
		ctx,
		r.store,
		inputSegments,
		outputSegments,
		results,
		key.OrganizationID,
		storageProfile.CollectorName,
		key.DateInt,
		key.InstanceNum,
		recordEstimate,
		3,                     // 3 = rollup
		key.SourceFrequencyMs, // Source frequency from the rollup key
		key.TargetFrequencyMs, // Target frequency from the rollup key
		r.getHourFromTimestamp,
	)
}

// Helper functions

func (r *MetricRollupProcessor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

// GetTargetRecordCount returns the target record count for a rollup grouping key
func (r *MetricRollupProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.RollupKey) int64 {
	// Use target frequency for the estimate since that's what we're creating
	return r.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.TargetFrequencyMs)
}

// queueNextLevelRollups generates rollup messages for the next level if the target frequency can be rolled up further
func (r *MetricRollupProcessor) queueNextLevelRollups(ctx context.Context, newSegments []lrdb.MetricSeg, key messages.RollupKey) error {
	ll := logctx.FromContext(ctx)

	// Check if the target frequency can be rolled up to the next level
	nextTargetFrequency := r.getNextRollupFrequency(key.TargetFrequencyMs)
	if nextTargetFrequency == 0 {
		// No further rollup possible for this frequency
		ll.Debug("No further rollup needed",
			slog.Int("targetFrequencyMs", int(key.TargetFrequencyMs)))
		return nil
	}

	ll.Info("Queueing next-level rollup messages",
		slog.Int("segmentCount", len(newSegments)),
		slog.Int("sourceFrequency", int(key.TargetFrequencyMs)),
		slog.Int("nextTargetFrequency", int(nextTargetFrequency)))

	// Create rollup messages for each new segment
	rollupTopic := "lakerunner.segments.metrics.rollup"
	var queuedCount int

	for _, segment := range newSegments {
		// Extract segment start time from the timestamp range
		segmentStartTime := time.Unix(segment.TsRange.Lower.Int64/1000, 0)

		// Create rollup notification for the next level
		notification := messages.MetricRollupMessage{
			Version:           1,
			OrganizationID:    segment.OrganizationID,
			DateInt:           segment.Dateint,
			SourceFrequencyMs: key.TargetFrequencyMs, // Current target becomes next source
			TargetFrequencyMs: nextTargetFrequency,   // Next rollup target
			SegmentID:         segment.SegmentID,
			InstanceNum:       segment.InstanceNum,
			Records:           segment.RecordCount,
			FileSize:          segment.FileSize,
			SegmentStartTime:  segmentStartTime,
			QueuedAt:          time.Now(),
		}

		// Marshal the message
		msgBytes, err := notification.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal next-level rollup notification: %w", err)
		}

		// Create Kafka message with target frequency in the key (as corrected earlier)
		rollupMessage := fly.Message{
			Key:   []byte(fmt.Sprintf("%s-%d-%d-%d", segment.OrganizationID.String(), segment.Dateint, nextTargetFrequency, segment.InstanceNum)),
			Value: msgBytes,
		}

		// Send to Kafka rollup topic
		if err := r.kafkaProducer.Send(ctx, rollupTopic, rollupMessage); err != nil {
			return fmt.Errorf("failed to send next-level rollup notification to Kafka: %w", err)
		}

		queuedCount++

		ll.Debug("Sent next-level rollup notification",
			slog.String("organizationID", segment.OrganizationID.String()),
			slog.Int("dateint", int(segment.Dateint)),
			slog.Int("sourceFrequencyMs", int(key.TargetFrequencyMs)),
			slog.Int("targetFrequencyMs", int(nextTargetFrequency)),
			slog.Int64("segmentID", segment.SegmentID))
	}

	ll.Info("Successfully queued next-level rollup messages",
		slog.Int("queuedCount", queuedCount),
		slog.Int("sourceFrequency", int(key.TargetFrequencyMs)),
		slog.Int("targetFrequency", int(nextTargetFrequency)))

	return nil
}

// queueCompactionMessages generates compaction messages for the target frequency segments
func (r *MetricRollupProcessor) queueCompactionMessages(ctx context.Context, newSegments []lrdb.MetricSeg, key messages.RollupKey) error {
	ll := logctx.FromContext(ctx)

	ll.Info("Queueing compaction messages for target frequency",
		slog.Int("segmentCount", len(newSegments)),
		slog.Int("targetFrequencyMs", int(key.TargetFrequencyMs)))

	// Create compaction messages for each new segment
	compactionTopic := "lakerunner.segments.metrics.compact"
	var queuedCount int

	for _, segment := range newSegments {
		// Create compaction notification
		compactionNotification := messages.MetricCompactionMessage{
			Version:        1,
			OrganizationID: segment.OrganizationID,
			DateInt:        segment.Dateint,
			FrequencyMs:    key.TargetFrequencyMs, // Use target frequency for compaction
			SegmentID:      segment.SegmentID,
			InstanceNum:    segment.InstanceNum,
			Records:        segment.RecordCount,
			FileSize:       segment.FileSize,
			QueuedAt:       time.Now(),
		}

		// Marshal compaction message
		compactionMsgBytes, err := compactionNotification.Marshal()
		if err != nil {
			return fmt.Errorf("failed to marshal compaction notification: %w", err)
		}

		// Create Kafka message key for proper partitioning
		compactionMessage := fly.Message{
			Key:   []byte(fmt.Sprintf("%s-%d-%d-%d", segment.OrganizationID.String(), segment.Dateint, key.TargetFrequencyMs, segment.InstanceNum)),
			Value: compactionMsgBytes,
		}

		// Send to compaction topic
		if err := r.kafkaProducer.Send(ctx, compactionTopic, compactionMessage); err != nil {
			return fmt.Errorf("failed to send compaction notification to Kafka: %w", err)
		}

		queuedCount++

		ll.Debug("Sent compaction notification",
			slog.String("organizationID", segment.OrganizationID.String()),
			slog.Int("dateint", int(segment.Dateint)),
			slog.Int("frequencyMs", int(key.TargetFrequencyMs)),
			slog.Int64("segmentID", segment.SegmentID))
	}

	ll.Info("Successfully queued compaction messages",
		slog.Int("queuedCount", queuedCount),
		slog.Int("targetFrequencyMs", int(key.TargetFrequencyMs)))

	return nil
}

// getNextRollupFrequency returns the next rollup frequency for a given source frequency, or 0 if no further rollup
func (r *MetricRollupProcessor) getNextRollupFrequency(sourceFrequencyMs int32) int32 {
	switch sourceFrequencyMs {
	case 60_000:
		return 300_000 // 1m -> 5m
	case 300_000:
		return 1_200_000 // 5m -> 20m
	case 1_200_000:
		return 3_600_000 // 20m -> 1h
	default:
		return 0 // No further rollup
	}
}
