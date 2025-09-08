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

package accumulation

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// GroupValidationError represents an error when messages in a group have inconsistent fields
type GroupValidationError struct {
	Field    string
	Expected interface{}
	Got      interface{}
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
}

// MetricCompactor implements the Processor interface for metric segment compaction
type MetricCompactor struct {
	store                CompactionStore
	storageProvider      storageprofile.StorageProfileProvider
	cmgr                 cloudstorage.ClientProvider
	targetRecordMultiple int // multiply estimate by this for safety net (e.g., 2)
}

// NewMetricCompactor creates a new metric compactor instance
func NewMetricCompactor(store CompactionStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider) *MetricCompactor {
	return &MetricCompactor{
		store:                store,
		storageProvider:      storageProvider,
		cmgr:                 cmgr,
		targetRecordMultiple: 2,
	}
}

// validateGroupConsistency ensures all messages in a group have consistent org, instance, dateint, and frequency
func validateGroupConsistency(group *AccumulationGroup[CompactionKey]) error {
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
		msg := accMsg.Message

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

// Process implements the Processor interface and performs the 12-step compaction flow
func (c *MetricCompactor) Process(ctx context.Context, group *AccumulationGroup[CompactionKey], kafkaCommitData *KafkaCommitData, recordCountEstimate int64) error {
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

	// Step 0: Validate that all messages in the group have consistent fields
	if err := validateGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Create temporary directory for this compaction run
	tmpDir, err := os.MkdirTemp("", "compaction-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	// Step 1: Get the storage profile for the given org/instance
	storageProfile, err := c.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	// Step 2: Make a storage client from that profile
	storageClient, err := cloudstorage.NewClient(ctx, c.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	// Step 3: Fetch the segments from the DB by iterating over messages
	var activeSegments []lrdb.MetricSeg
	var segmentsToMarkCompacted []lrdb.MetricSeg
	targetSizeThreshold := config.TargetFileSize * 80 / 100 // 80% of target file size

	for _, accMsg := range group.Messages {
		msg := accMsg.Message
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

		// Step 4: Check if segment is already close to target size and mark as compacted
		if !segment.Compacted && segment.FileSize >= targetSizeThreshold {
			ll.Info("Segment already close to target size, marking as compacted",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Int64("fileSize", segment.FileSize),
				slog.Int64("targetSizeThreshold", targetSizeThreshold),
				slog.Float64("percentOfTarget", float64(segment.FileSize)/float64(config.TargetFileSize)*100))
			segmentsToMarkCompacted = append(segmentsToMarkCompacted, segment)
			continue
		}

		// Step 5: Only include segments not marked as compacted and not being marked now
		if !segment.Compacted {
			activeSegments = append(activeSegments, segment)
		}
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

	// Step 5-8: Use metricsprocessing.CreateReaderStack to handle download and reader creation
	readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpDir, storageClient, group.Key.OrganizationID, storageProfile, activeSegments)
	if err != nil {
		return fmt.Errorf("create reader stack: %w", err)
	}
	defer metricsprocessing.CloseReaderStack(ctx, readerStack)

	// Step 8: Create aggregation reader from the merge reader with frequency_ms as window
	var aggReader filereader.Reader
	if len(readerStack.Readers) == 1 {
		// Single reader case
		aggReader, err = filereader.NewAggregatingMetricsReader(readerStack.Readers[0], int64(group.Key.FrequencyMs), 1000)
	} else if len(readerStack.Readers) > 1 {
		// Multiple readers - need merge sort first
		mergeReader, mergeErr := filereader.NewMergesortReader(ctx, readerStack.Readers, &filereader.MetricSortKeyProvider{}, 1000)
		if mergeErr != nil {
			return fmt.Errorf("create merge sort reader: %w", mergeErr)
		}
		defer mergeReader.Close()
		aggReader, err = filereader.NewAggregatingMetricsReader(mergeReader, int64(group.Key.FrequencyMs), 1000)
	} else {
		return fmt.Errorf("no readers available for compaction")
	}
	if err != nil {
		return fmt.Errorf("create aggregating reader: %w", err)
	}
	defer aggReader.Close()

	// Step 9: Create parquet writer with estimated max records
	maxRecords := recordCountEstimate * int64(c.targetRecordMultiple)
	writer, err := factories.NewMetricsWriter(tmpDir, maxRecords)
	if err != nil {
		return fmt.Errorf("create parquet writer: %w", err)
	}

	// Write from aggregation reader to writer
	if err := c.writeFromReader(ctx, aggReader, writer); err != nil {
		writer.Abort()
		return fmt.Errorf("write from reader: %w", err)
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		return fmt.Errorf("close writer: %w", err)
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

	ll.Info("Compaction completed successfully",
		slog.Int("inputSegments", len(activeSegments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))

	return nil
}

// writeFromReader writes data from reader to writer
func (c *MetricCompactor) writeFromReader(ctx context.Context, reader filereader.Reader, writer *parquetwriter.UnifiedWriter) error {
	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("read batch: %w", err)
		}

		if err := writer.WriteBatch(batch); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
	}

	return nil
}

// uploadAndCreateSegments uploads the files and creates new segment records
func (c *MetricCompactor) uploadAndCreateSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key CompactionKey, inputSegments []lrdb.MetricSeg) ([]lrdb.MetricSeg, error) {
	ll := logctx.FromContext(ctx)

	// Calculate input metrics
	var totalInputSize int64
	for _, seg := range inputSegments {
		totalInputSize += seg.FileSize
	}

	var segments []lrdb.MetricSeg
	var totalOutputSize int64
	var segmentIDs []int64

	for _, result := range results {
		// Generate new segment ID
		segmentID := c.generateSegmentID()

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

		// Create new segment record
		segment := lrdb.MetricSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
			IngestDateint:  key.DateInt, // Use same as dateint for compacted segments
			FrequencyMs:    key.FrequencyMs,
			SegmentID:      segmentID,
			InstanceNum:    key.InstanceNum,
			SlotID:         0, // Compacted segments don't need slot partitioning
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
			Compacted:    true,
			Fingerprints: stats.Fingerprints,
			SortVersion:  lrdb.CurrentMetricSortVersion,
			SlotCount:    1,
			CreatedBy:    lrdb.CreatedByCompact,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		segmentIDs = append(segmentIDs, segmentID)
	}

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
func (c *MetricCompactor) atomicDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.MetricSeg, kafkaCommitData *KafkaCommitData, key CompactionKey) error {
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
		sort.Slice(kafkaOffsets, func(i, j int) bool {
			if kafkaOffsets[i].Topic != kafkaOffsets[j].Topic {
				return kafkaOffsets[i].Topic < kafkaOffsets[j].Topic
			}
			if kafkaOffsets[i].Partition != kafkaOffsets[j].Partition {
				return kafkaOffsets[i].Partition < kafkaOffsets[j].Partition
			}
			return kafkaOffsets[i].ConsumerGroup < kafkaOffsets[j].ConsumerGroup
		})
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

func (c *MetricCompactor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

func (c *MetricCompactor) generateSegmentID() int64 {
	// This would typically use a proper ID generator
	// For now, use timestamp-based generation
	return int64(uuid.New().ID())
}

// markSegmentsAsCompacted marks the given segments as compacted in the database
func (c *MetricCompactor) markSegmentsAsCompacted(ctx context.Context, segments []lrdb.MetricSeg, key CompactionKey) error {
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
