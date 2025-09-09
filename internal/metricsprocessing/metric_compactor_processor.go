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
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/lib/pq"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// GroupValidationError represents an error when messages in a group have inconsistent fields
type GroupValidationError struct {
	Field    string
	Expected any
	Got      any
	Message  string
}

func (e *GroupValidationError) Error() string {
	return fmt.Sprintf("group validation failed - %s: expected %v, got %v (%s)", e.Field, e.Expected, e.Got, e.Message)
}

// CompactionStore defines database operations needed for compaction
type CompactionStore interface {
	GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error)
	CompactMetricSegsWithKafkaOffsetsWithOrg(ctx context.Context, params lrdb.CompactMetricSegsParams, kafkaOffsets []lrdb.KafkaOffsetUpdateWithOrg) error
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
	MarkMetricSegsCompactedByKeys(ctx context.Context, params lrdb.MarkMetricSegsCompactedByKeysParams) error
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
	// Add segment_journal functionality
	InsertSegmentJournal(ctx context.Context, params lrdb.InsertSegmentJournalParams) error
}

// MetricCompactorProcessor implements the Processor interface for metric segment compaction
type MetricCompactorProcessor struct {
	store           CompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
}

// NewMetricCompactor creates a new metric compactor instance
func NewMetricCompactor(store CompactionStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, cfg *config.Config) *MetricCompactorProcessor {
	return &MetricCompactorProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
	}
}

// validateGroupConsistency ensures all messages in a group have consistent org, instance, dateint, and frequency
func validateGroupConsistency(group *AccumulationGroup[messages.CompactionKey]) error {
	if len(group.Messages) == 0 {
		return &GroupValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	// Get expected values from the group key
	expectedOrg := group.Key.OrganizationID
	expectedInstance := group.Key.InstanceNum
	expectedDateInt := group.Key.DateInt
	expectedFrequency := group.Key.FrequencyMs

	// Validate each message against the expected values
	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricCompactionMessage)
		if !ok {
			return &GroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not a MetricCompactionMessage", i),
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

		if msg.DateInt != expectedDateInt {
			return &GroupValidationError{
				Field:    "date_int",
				Expected: expectedDateInt,
				Got:      msg.DateInt,
				Message:  fmt.Sprintf("message %d has inconsistent date int", i),
			}
		}

		if msg.FrequencyMs != expectedFrequency {
			return &GroupValidationError{
				Field:    "frequency_ms",
				Expected: expectedFrequency,
				Got:      msg.FrequencyMs,
				Message:  fmt.Sprintf("message %d has inconsistent frequency", i),
			}
		}
	}

	return nil
}

func (c *MetricCompactorProcessor) Process(ctx context.Context, group *AccumulationGroup[messages.CompactionKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	// Calculate group age from Hunter timestamp
	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting compaction",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("frequencyMs", int(group.Key.FrequencyMs)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	recordCountEstimate := c.store.GetMetricEstimate(ctx, group.Key.OrganizationID, group.Key.FrequencyMs)

	if err := validateGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
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

	var activeSegments []lrdb.MetricSeg
	var segmentsToMarkCompacted []lrdb.MetricSeg
	targetSizeThreshold := config.TargetFileSize * 80 / 100 // 80% of target file size

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricCompactionMessage)
		if !ok {
			ll.Debug("Skipping non-MetricCompactionMessage message in compaction group")
			continue
		}
		segment, err := c.store.GetMetricSeg(ctx, lrdb.GetMetricSegParams{
			OrganizationID: msg.OrganizationID,
			Dateint:        msg.DateInt,
			FrequencyMs:    msg.FrequencyMs,
			SegmentID:      msg.SegmentID,
			InstanceNum:    msg.InstanceNum,
			SlotID:         msg.SlotID,
			SlotCount:      msg.SlotCount,
		})
		if err != nil {
			ll.Warn("Failed to fetch segment, skipping",
				slog.Int64("segmentID", msg.SegmentID),
				slog.String("organizationID", msg.OrganizationID.String()),
				slog.Int("dateint", int(msg.DateInt)),
				slog.Int("frequencyMs", int(msg.FrequencyMs)),
				slog.Int("instanceNum", int(msg.InstanceNum)),
				slog.Int("slotID", int(msg.SlotID)),
				slog.Int("slotCount", int(msg.SlotCount)),
				slog.Any("error", err))
			continue
		}

		if !segment.Compacted && segment.FileSize >= targetSizeThreshold {
			ll.Info("Segment already close to target size, marking as compacted",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Int64("fileSize", segment.FileSize),
				slog.Int64("targetSizeThreshold", targetSizeThreshold),
				slog.Float64("percentOfTarget", float64(segment.FileSize)/float64(config.TargetFileSize)*100))
			segmentsToMarkCompacted = append(segmentsToMarkCompacted, segment)
			continue
		}

		if segment.Compacted {
			ll.Info("Segment already marked as compacted, skipping", slog.Int64("segmentID", segment.SegmentID))
			continue
		}

		activeSegments = append(activeSegments, segment)
	}

	// Mark large segments as compacted if any
	if len(segmentsToMarkCompacted) > 0 {
		if err := c.markSegmentsAsCompacted(ctx, segmentsToMarkCompacted, group.Key); err != nil {
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

	ll.Info("Found segments to compact",
		slog.Int("activeSegments", len(activeSegments)))

	// Perform the core compaction logic
	processedSegments, results, err := c.performCompaction(ctx, tmpDir, storageClient, group.Key, storageProfile, activeSegments, recordCountEstimate)
	if err != nil {
		return fmt.Errorf("perform compaction: %w", err)
	}

	// Step 10: Upload new files and create new metric segments
	newSegments, err := c.uploadAndCreateSegments(ctx, storageClient, storageProfile, results, group.Key, activeSegments)
	if err != nil {
		return fmt.Errorf("upload and create segments: %w", err)
	}

	// Step 12: Atomic operation - mark old as compacted, insert new, update Kafka offsets
	if err := c.atomicDatabaseUpdate(ctx, activeSegments, newSegments, kafkaCommitData, group.Key); err != nil {
		return fmt.Errorf("atomic database update: %w", err)
	}

	var totalRecords, totalSize int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalSize += result.FileSize
	}

	// Log the compaction operation to seg_log for debugging (if enabled)
	if c.cfg.SegLog.Enabled {
		if err := c.logCompactionOperation(ctx, storageProfile, processedSegments, newSegments, results, group.Key, recordCountEstimate); err != nil {
			// Don't fail the compaction if seg_log fails - this is just for debugging
			ll.Warn("Failed to log compaction operation to seg_log", slog.Any("error", err))
		}
	}

	ll.Info("Compaction completed successfully",
		slog.Int("inputSegments", len(activeSegments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))

	return nil
}

// uploadAndCreateSegments uploads the files and creates new segment records
func (c *MetricCompactorProcessor) uploadAndCreateSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.CompactionKey, inputSegments []lrdb.MetricSeg) ([]lrdb.MetricSeg, error) {
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

	for _, result := range results {
		segmentID := idgen.GenerateID()

		// Get metadata from result
		stats, ok := result.Metadata.(factories.MetricsFileStats)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}

		// Upload the file
		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, c.getHourFromTimestamp(stats.FirstTS), segmentID, "metrics")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload file %s: %w", result.FileName, err)
		}

		// Clean up local file
		os.Remove(result.FileName)

		nowDateInt := int32(time.Now().UTC().Year()*10000 + int(time.Now().UTC().Month())*100 + time.Now().UTC().Day())

		// Create new segment record
		segment := lrdb.MetricSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
			IngestDateint:  nowDateInt,
			FrequencyMs:    key.FrequencyMs,
			SegmentID:      segmentID,
			InstanceNum:    key.InstanceNum,
			SlotID:         0, // Compacted segments don't need slot partitioning
			SlotCount:      1,
			TsRange: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Int8{Int64: stats.FirstTS, Valid: true},
				Upper:     pgtype.Int8{Int64: stats.LastTS, Valid: true},
				Valid:     true,
			},
			RecordCount:  result.RecordCount,
			FileSize:     result.FileSize,
			Published:    true,
			Compacted:    true,
			Fingerprints: stats.Fingerprints,
			SortVersion:  lrdb.CurrentMetricSortVersion,
			CreatedBy:    lrdb.CreatedByCompact,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount
		segmentIDs = append(segmentIDs, segmentID)
	}

	// Report telemetry
	ReportTelemetry(ctx, "compaction", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	// Log upload summary
	ll.Info("Segment upload completed",
		slog.Int("inputFiles", len(inputSegments)),
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Any("createdSegmentIDs", segmentIDs))

	return segments, nil
}

// atomicDatabaseUpdate performs the atomic transaction for step 12
func (c *MetricCompactorProcessor) atomicDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.MetricSeg, kafkaCommitData *KafkaCommitData, key messages.CompactionKey) error {
	ll := logctx.FromContext(ctx)

	// Prepare Kafka offsets for update
	var kafkaOffsets []lrdb.KafkaOffsetUpdateWithOrg
	if kafkaCommitData != nil {
		for partition, offset := range kafkaCommitData.Offsets {
			kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetUpdateWithOrg{
				Topic:          kafkaCommitData.Topic,
				Partition:      partition,
				ConsumerGroup:  kafkaCommitData.ConsumerGroup,
				OrganizationID: key.OrganizationID,
				InstanceNum:    key.InstanceNum,
				Offset:         offset,
			})

			// Log each Kafka offset update
			ll.Info("Updating Kafka consumer group offset",
				slog.String("consumerGroup", kafkaCommitData.ConsumerGroup),
				slog.String("topic", kafkaCommitData.Topic),
				slog.Int("partition", int(partition)),
				slog.Int64("newOffset", offset))
		}

		// Sort to avoid deadlocks
		sortKafkaOffsets(kafkaOffsets)
	}

	// Convert segments to appropriate types
	oldRecords := make([]lrdb.CompactMetricSegsOld, len(oldSegments))
	for i, seg := range oldSegments {
		oldRecords[i] = lrdb.CompactMetricSegsOld{
			SegmentID: seg.SegmentID,
			SlotID:    seg.SlotID,
		}
	}

	newRecords := make([]lrdb.CompactMetricSegsNew, len(newSegments))
	for i, seg := range newSegments {
		newRecords[i] = lrdb.CompactMetricSegsNew{
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
	params := lrdb.CompactMetricSegsParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		FrequencyMs:    key.FrequencyMs,
		InstanceNum:    key.InstanceNum,
		SlotID:         0, // Compacted segments don't need slot partitioning
		SlotCount:      1,
		IngestDateint:  key.DateInt,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
		CreatedBy:      lrdb.CreatedByCompact,
	}

	return c.store.CompactMetricSegsWithKafkaOffsetsWithOrg(ctx, params, kafkaOffsets)
}

// Helper functions

// sortKafkaOffsets sorts Kafka offsets to avoid deadlocks in database operations.
// The sort order is: topic, then partition, then consumer group.
func sortKafkaOffsets(offsets []lrdb.KafkaOffsetUpdateWithOrg) {
	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].Topic != offsets[j].Topic {
			return offsets[i].Topic < offsets[j].Topic
		}
		if offsets[i].Partition != offsets[j].Partition {
			return offsets[i].Partition < offsets[j].Partition
		}
		return offsets[i].ConsumerGroup < offsets[j].ConsumerGroup
	})
}

func (c *MetricCompactorProcessor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

// markSegmentsAsCompacted marks the given segments as compacted in the database
func (c *MetricCompactorProcessor) markSegmentsAsCompacted(ctx context.Context, segments []lrdb.MetricSeg, key messages.CompactionKey) error {
	if len(segments) == 0 {
		return nil
	}

	// Extract segment IDs
	segmentIDs := make([]int64, len(segments))
	for i, seg := range segments {
		segmentIDs[i] = seg.SegmentID
	}

	// Mark segments as compacted
	return c.store.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		FrequencyMs:    key.FrequencyMs,
		InstanceNum:    key.InstanceNum,
		SegmentIds:     segmentIDs,
	})
}

// GetTargetRecordCount returns the target record count for a grouping key
func (c *MetricCompactorProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return c.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.FrequencyMs)
}

// logCompactionOperation logs the compaction operation to seg_log for debugging purposes
func (c *MetricCompactorProcessor) logCompactionOperation(ctx context.Context, storageProfile storageprofile.StorageProfile, inputSegments, outputSegments []lrdb.MetricSeg, results []parquetwriter.Result, key messages.CompactionKey, recordEstimate int64) error {
	// Extract source object keys from input segments
	sourceObjectKeys := make([]string, len(inputSegments))
	var sourceTotalRecords, sourceTotalSize int64
	for i, seg := range inputSegments {
		sourceObjectKeys[i] = helpers.MakeDBObjectID(key.OrganizationID, storageProfile.CollectorName, key.DateInt, c.getHourFromTimestamp(seg.TsRange.Lower.Int64), seg.SegmentID, "metrics")
		sourceTotalRecords += seg.RecordCount
		sourceTotalSize += seg.FileSize
	}

	// Extract destination object keys from results/output segments
	destObjectKeys := make([]string, len(results))
	var destTotalRecords, destTotalSize int64
	for i, result := range results {
		stats, ok := result.Metadata.(factories.MetricsFileStats)
		if !ok {
			return fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}
		// Use the segment ID from the corresponding output segment
		if i < len(outputSegments) {
			destObjectKeys[i] = helpers.MakeDBObjectID(key.OrganizationID, storageProfile.CollectorName, key.DateInt, c.getHourFromTimestamp(stats.FirstTS), outputSegments[i].SegmentID, "metrics")
		}
		destTotalRecords += result.RecordCount
		destTotalSize += result.FileSize
	}

	// Create segment_journal entry
	logParams := lrdb.InsertSegmentJournalParams{
		Signal:             2, // 2 = metrics (based on enum pattern)
		Action:             2, // 2 = compact (based on enum pattern)
		OrganizationID:     key.OrganizationID,
		InstanceNum:        key.InstanceNum,
		Dateint:            key.DateInt,
		FrequencyMs:        key.FrequencyMs,
		SourceCount:        int32(len(inputSegments)),
		SourceObjectKeys:   pq.StringArray(sourceObjectKeys),
		SourceTotalRecords: sourceTotalRecords,
		SourceTotalSize:    sourceTotalSize,
		DestCount:          int32(len(results)),
		DestObjectKeys:     pq.StringArray(destObjectKeys),
		DestTotalRecords:   destTotalRecords,
		DestTotalSize:      destTotalSize,
		RecordEstimate:     recordEstimate,
		Metadata:           make(map[string]any), // Empty metadata for now
	}

	return c.store.InsertSegmentJournal(ctx, logParams)
}

// performCompaction handles the core compaction logic: creating readers, aggregating, and writing
func (c *MetricCompactorProcessor) performCompaction(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, compactionKey messages.CompactionKey, storageProfile storageprofile.StorageProfile, activeSegments []lrdb.MetricSeg, recordCountEstimate int64) ([]lrdb.MetricSeg, []parquetwriter.Result, error) {
	params := MetricProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: compactionKey.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: activeSegments,
		FrequencyMs:    compactionKey.FrequencyMs,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	result, err := ProcessMetricsWithAggregation(ctx, params)
	if err != nil {
		return nil, nil, err
	}

	return result.ProcessedSegments, result.Results, nil
}
