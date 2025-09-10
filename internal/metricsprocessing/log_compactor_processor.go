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
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// LogGroupValidationError represents an error when messages in a group have inconsistent fields
type LogGroupValidationError struct {
	Field    string
	Expected any
	Got      any
	Message  string
}

func (e *LogGroupValidationError) Error() string {
	return fmt.Sprintf("log group validation failed - %s: expected %v, got %v (%s)", e.Field, e.Expected, e.Got, e.Message)
}

// LogCompactionStore defines database operations needed for log compaction
type LogCompactionStore interface {
	GetLogSeg(ctx context.Context, params lrdb.GetLogSegParams) (lrdb.LogSeg, error)
	CompactLogSegsWithKafkaOffsets(ctx context.Context, params lrdb.CompactLogSegsParams, kafkaOffsets []lrdb.KafkaOffsetUpdate) error
	MarkLogSegsCompactedByKeys(ctx context.Context, params lrdb.MarkLogSegsCompactedByKeysParams) error
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
	GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64
	InsertSegmentJournal(ctx context.Context, params lrdb.InsertSegmentJournalParams) error
}

// LogCompactorProcessor implements the Processor interface for log segment compaction
type LogCompactorProcessor struct {
	store           LogCompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
}

// newLogCompactor creates a new log compactor instance
func newLogCompactor(store LogCompactionStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, cfg *config.Config) *LogCompactorProcessor {
	return &LogCompactorProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
	}
}

// validateLogGroupConsistency ensures all messages in a group have consistent org, instance, and dateint
func validateLogGroupConsistency(group *accumulationGroup[messages.LogCompactionKey]) error {
	if len(group.Messages) == 0 {
		return &LogGroupValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	expectedOrg := group.Key.OrganizationID
	expectedInstance := group.Key.InstanceNum
	expectedDateInt := group.Key.DateInt

	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.LogCompactionMessage)
		if !ok {
			return &LogGroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not a LogCompactionMessage", i),
			}
		}

		if msg.OrganizationID != expectedOrg {
			return &LogGroupValidationError{
				Field:    "organization_id",
				Expected: expectedOrg,
				Got:      msg.OrganizationID,
				Message:  fmt.Sprintf("message %d has inconsistent organization ID", i),
			}
		}

		if msg.InstanceNum != expectedInstance {
			return &LogGroupValidationError{
				Field:    "instance_num",
				Expected: expectedInstance,
				Got:      msg.InstanceNum,
				Message:  fmt.Sprintf("message %d has inconsistent instance number", i),
			}
		}

		if msg.DateInt != expectedDateInt {
			return &LogGroupValidationError{
				Field:    "date_int",
				Expected: expectedDateInt,
				Got:      msg.DateInt,
				Message:  fmt.Sprintf("message %d has inconsistent date int", i),
			}
		}
	}

	return nil
}

func (c *LogCompactorProcessor) Process(ctx context.Context, group *accumulationGroup[messages.LogCompactionKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting log compaction",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	recordCountEstimate := c.store.GetLogEstimate(ctx, group.Key.OrganizationID)

	if err := validateLogGroupConsistency(group); err != nil {
		return fmt.Errorf("log group validation failed: %w", err)
	}

	// Create temporary directory for this compaction run
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	storageProfile, err := c.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	storageClient, err := cloudstorage.NewClient(ctx, c.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	if err := c.performLogCompaction(ctx, tmpDir, storageClient, group, storageProfile, recordCountEstimate, kafkaCommitData); err != nil {
		return fmt.Errorf("perform log compaction: %w", err)
	}

	ll.Info("Log compaction completed successfully",
		slog.Int("messageCount", len(group.Messages)))

	return nil
}

// performLogCompaction handles the core log compaction logic using the new reader/writer pattern
func (c *LogCompactorProcessor) performLogCompaction(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, group *accumulationGroup[messages.LogCompactionKey], storageProfile storageprofile.StorageProfile, recordCountEstimate int64, kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	var activeSegments []lrdb.LogSeg
	var segmentsToMarkCompacted []lrdb.LogSeg
	targetSizeThreshold := constants.LogCompactionTargetSizeBytes * 80 / 100 // 80% of target file size

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.LogCompactionMessage)
		if !ok {
			ll.Debug("Skipping non-LogCompactionMessage message in compaction group")
			continue
		}

		segment, err := c.store.GetLogSeg(ctx, lrdb.GetLogSegParams{
			OrganizationID: msg.OrganizationID,
			Dateint:        msg.DateInt,
			SegmentID:      msg.SegmentID,
			InstanceNum:    msg.InstanceNum,
			SlotID:         msg.SlotID,
		})
		if err != nil {
			ll.Warn("Failed to fetch segment, skipping",
				slog.Int64("segmentID", msg.SegmentID),
				slog.String("organizationID", msg.OrganizationID.String()),
				slog.Int("dateint", int(msg.DateInt)),
				slog.Int("instanceNum", int(msg.InstanceNum)),
				slog.Any("error", err))
			continue
		}

		if segment.Compacted {
			ll.Info("Segment already marked as compacted, skipping", slog.Int64("segmentID", segment.SegmentID))
			continue
		}

		if segment.FileSize >= targetSizeThreshold {
			ll.Info("Segment already close to target size, marking as compacted",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Int64("fileSize", segment.FileSize),
				slog.Int64("targetSizeThreshold", targetSizeThreshold),
				slog.Float64("percentOfTarget", float64(segment.FileSize)/float64(constants.LogCompactionTargetSizeBytes)*100))
			segmentsToMarkCompacted = append(segmentsToMarkCompacted, segment)
			continue
		}

		activeSegments = append(activeSegments, segment)
	}

	if len(segmentsToMarkCompacted) > 0 {
		if err := c.markLogSegmentsAsCompacted(ctx, segmentsToMarkCompacted, group.Key); err != nil {
			ll.Warn("Failed to mark segments as compacted", slog.Any("error", err))
		} else {
			ll.Info("Marked segments as compacted",
				slog.Int("segmentCount", len(segmentsToMarkCompacted)))
		}
	}

	if len(activeSegments) == 0 {
		ll.Info("No active segments to compact")
		return nil
	}

	ll.Info("Found segments to compact", slog.Int("activeSegments", len(activeSegments)))

	processedSegments, results, err := c.performLogCompactionCore(ctx, tmpDir, storageClient, group.Key, storageProfile, activeSegments, recordCountEstimate)
	if err != nil {
		return fmt.Errorf("perform compaction: %w", err)
	}

	newSegments, err := c.uploadAndCreateLogSegments(ctx, storageClient, storageProfile, results, group.Key, activeSegments)
	if err != nil {
		return fmt.Errorf("upload and create segments: %w", err)
	}

	if err := c.atomicLogDatabaseUpdate(ctx, activeSegments, newSegments, kafkaCommitData, group.Key); err != nil {
		return fmt.Errorf("atomic database update: %w", err)
	}

	var totalRecords, totalSize int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalSize += result.FileSize
	}

	if c.cfg.SegLog.Enabled {
		if err := c.logLogCompactionOperation(ctx, storageProfile.CollectorName, processedSegments, newSegments, results, group.Key, recordCountEstimate); err != nil {
			ll.Warn("Failed to log compaction operation to seg_log", slog.Any("error", err))
		}
	}

	ll.Info("Log compaction completed successfully",
		slog.Int("inputSegments", len(activeSegments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))
	return nil
}

// performLogCompactionCore handles the core compaction logic: creating readers and writing
func (c *LogCompactorProcessor) performLogCompactionCore(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, compactionKey messages.LogCompactionKey, storageProfile storageprofile.StorageProfile, activeSegments []lrdb.LogSeg, recordCountEstimate int64) ([]lrdb.LogSeg, []parquetwriter.Result, error) {
	params := logProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: compactionKey.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: activeSegments,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	result, err := processLogsWithSorting(ctx, params)
	if err != nil {
		return nil, nil, err
	}

	return result.ProcessedSegments, result.Results, nil
}

// uploadAndCreateLogSegments uploads the files and creates new log segment records
func (c *LogCompactorProcessor) uploadAndCreateLogSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.LogCompactionKey, inputSegments []lrdb.LogSeg) ([]lrdb.LogSeg, error) {
	ll := logctx.FromContext(ctx)

	var totalInputSize, totalInputRecords int64
	for _, seg := range inputSegments {
		totalInputSize += seg.FileSize
		totalInputRecords += seg.RecordCount
	}

	var segments []lrdb.LogSeg
	var totalOutputSize, totalOutputRecords int64
	var segmentIDs []int64

	// Generate unique batch IDs for all results to help avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(results))

	for i, result := range results {
		segmentID := batchSegmentIDs[i]

		// Get metadata from result
		stats, ok := result.Metadata.(factories.LogsFileStats)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}

		// Upload the file
		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, c.getHourFromTimestamp(stats.FirstTS), segmentID, "logs")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload file %s: %w", result.FileName, err)
		}

		nowDateInt := helpers.CurrentDateInt()

		segment := lrdb.LogSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
			IngestDateint:  nowDateInt,
			SegmentID:      segmentID,
			InstanceNum:    key.InstanceNum,
			SlotID:         0, // not used for compacted log files
			TsRange: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Int8{Int64: stats.FirstTS, Valid: true},
				Upper:     pgtype.Int8{Int64: stats.LastTS + 1, Valid: true},
				Valid:     true,
			},
			RecordCount:  result.RecordCount,
			FileSize:     result.FileSize,
			Compacted:    true,
			Fingerprints: stats.Fingerprints,
			CreatedBy:    lrdb.CreatedByCompact,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount
		segmentIDs = append(segmentIDs, segmentID)
	}

	reportTelemetry(ctx, "compaction", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Log segment upload completed",
		slog.Int("inputFiles", len(inputSegments)),
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Any("createdSegmentIDs", segmentIDs))

	return segments, nil
}

// markLogSegmentsAsCompacted marks the given segments as compacted in the database
func (c *LogCompactorProcessor) markLogSegmentsAsCompacted(ctx context.Context, segments []lrdb.LogSeg, key messages.LogCompactionKey) error {
	if len(segments) == 0 {
		return nil
	}

	segmentIDs := make([]int64, len(segments))
	for i, seg := range segments {
		segmentIDs[i] = seg.SegmentID
	}

	return c.store.MarkLogSegsCompactedByKeys(ctx, lrdb.MarkLogSegsCompactedByKeysParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		InstanceNum:    key.InstanceNum,
		SegmentIds:     segmentIDs,
	})
}

// atomicLogDatabaseUpdate performs the atomic transaction for log compaction
func (c *LogCompactorProcessor) atomicLogDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.LogSeg, kafkaCommitData *KafkaCommitData, key messages.LogCompactionKey) error {
	ll := logctx.FromContext(ctx)

	// Prepare Kafka offsets for update
	var kafkaOffsets []lrdb.KafkaOffsetUpdate
	if kafkaCommitData != nil {
		for partition, offset := range kafkaCommitData.Offsets {
			kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetUpdate{
				Topic:          kafkaCommitData.Topic,
				Partition:      partition,
				ConsumerGroup:  kafkaCommitData.ConsumerGroup,
				OrganizationID: key.OrganizationID,
				InstanceNum:    key.InstanceNum,
				Offset:         offset,
			})

			// Log each Kafka offset update
			ll.Debug("Updating Kafka consumer group offset",
				slog.String("consumerGroup", kafkaCommitData.ConsumerGroup),
				slog.String("topic", kafkaCommitData.Topic),
				slog.Int("partition", int(partition)),
				slog.Int64("newOffset", offset))
		}
	}

	// Convert segments to appropriate types
	oldRecords := make([]lrdb.CompactLogSegsOld, len(oldSegments))
	for i, seg := range oldSegments {
		oldRecords[i] = lrdb.CompactLogSegsOld{
			SegmentID: seg.SegmentID,
		}
	}

	newRecords := make([]lrdb.CompactLogSegsNew, len(newSegments))
	for i, seg := range newSegments {
		newRecords[i] = lrdb.CompactLogSegsNew{
			SegmentID:    seg.SegmentID,
			StartTs:      seg.TsRange.Lower.Int64,
			EndTs:        seg.TsRange.Upper.Int64,
			RecordCount:  seg.RecordCount,
			FileSize:     seg.FileSize,
			Fingerprints: seg.Fingerprints,
		}
	}

	if len(newRecords) == 0 {
		return fmt.Errorf("no new segments to insert")
	}

	// Perform atomic operation
	params := lrdb.CompactLogSegsParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		IngestDateint:  newSegments[0].IngestDateint, // Use first new segment's ingest date
		InstanceNum:    key.InstanceNum,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
		CreatedBy:      lrdb.CreatedByCompact,
	}

	// Perform atomic operation with Kafka offsets
	if err := c.store.CompactLogSegsWithKafkaOffsets(ctx, params, kafkaOffsets); err != nil {
		ll.Error("Failed CompactLogSegsWithKafkaOffsets",
			slog.Any("error", err),
			slog.String("organization_id", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("instance_num", int(key.InstanceNum)),
			slog.Int("old_segments_count", len(oldSegments)),
			slog.Int("new_segments_count", len(newSegments)))

		// Log segment IDs for additional context
		if len(oldSegments) > 0 {
			oldSegmentIDs := make([]int64, len(oldSegments))
			for i, seg := range oldSegments {
				oldSegmentIDs[i] = seg.SegmentID
			}
			ll.Error("CompactLogSegs old segment IDs",
				slog.Any("old_segment_ids", oldSegmentIDs))
		}

		if len(newSegments) > 0 {
			newSegmentIDs := make([]int64, len(newSegments))
			for i, seg := range newSegments {
				newSegmentIDs[i] = seg.SegmentID
			}
			ll.Error("CompactLogSegs new segment IDs",
				slog.Any("new_segment_ids", newSegmentIDs))
		}

		return fmt.Errorf("failed to compact log segments: %w", err)
	}

	return nil
}

// getHourFromTimestamp extracts the hour component from a millisecond timestamp
func (c *LogCompactorProcessor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

// logLogCompactionOperation logs the log compaction operation to segment_journal for debugging purposes
func (c *LogCompactorProcessor) logLogCompactionOperation(ctx context.Context, collectorName string, inputSegments []lrdb.LogSeg, outputSegments []lrdb.LogSeg, results []parquetwriter.Result, key messages.LogCompactionKey, recordEstimate int64) error {
	return logLogSegmentOperation(
		ctx,
		c.store,
		inputSegments,
		outputSegments,
		results,
		key.OrganizationID,
		collectorName,
		key.DateInt,
		key.InstanceNum,
		recordEstimate,
		c.getHourFromTimestamp,
	)
}

// GetTargetRecordCount returns the target record count for a grouping key
func (c *LogCompactorProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.LogCompactionKey) int64 {
	return c.store.GetLogEstimate(ctx, groupingKey.OrganizationID)
}
