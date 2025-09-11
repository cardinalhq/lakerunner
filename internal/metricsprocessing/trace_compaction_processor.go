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

	"github.com/jackc/pgx/v5/pgtype"

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

// TraceCompactionProcessor implements compaction processing for traces using the base framework
type TraceCompactionProcessor struct {
	*CommonProcessorBase[*messages.TraceCompactionMessage, messages.TraceCompactionKey]
	store TraceCompactionStore
}

// NewTraceCompactionProcessor creates a new trace compaction processor
func NewTraceCompactionProcessor(
	store TraceCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg *config.Config,
) *TraceCompactionProcessor {
	return &TraceCompactionProcessor{
		CommonProcessorBase: NewCommonProcessorBase[*messages.TraceCompactionMessage, messages.TraceCompactionKey](
			storageProvider,
			cmgr,
			cfg,
		),
		store: store,
	}
}

// Process implements the processor interface for trace compaction
func (p *TraceCompactionProcessor) Process(ctx context.Context, group *accumulationGroup[messages.TraceCompactionKey], kafkaCommitData *KafkaCommitData) error {
	// Log compaction start with trace-specific fields
	p.LogCompactionStart(ctx, group, "traces",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
	)

	// Use the base framework for common processing
	return p.ProcessCore(ctx, group, kafkaCommitData, p)
}

// ProcessWork handles the trace-specific work logic
func (p *TraceCompactionProcessor) ProcessWork(
	ctx context.Context,
	tmpDir string,
	storageClient cloudstorage.Client,
	storageProfile storageprofile.StorageProfile,
	group *accumulationGroup[messages.TraceCompactionKey],
	kafkaCommitData *KafkaCommitData,
) error {
	ll := logctx.FromContext(ctx)

	recordCountEstimate := p.store.GetTraceEstimate(ctx, group.Key.OrganizationID)

	// Process segments
	var activeSegments []lrdb.TraceSeg
	var segmentsToMarkCompacted []lrdb.TraceSeg
	targetSizeThreshold := config.TargetFileSize * 80 / 100 // 80% of target file size

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.TraceCompactionMessage)
		if !ok {
			ll.Debug("Skipping non-TraceCompactionMessage message in compaction group")
			continue
		}

		segment, err := p.store.GetTraceSeg(ctx, lrdb.GetTraceSegParams{
			OrganizationID: msg.OrganizationID,
			Dateint:        msg.DateInt,
			SegmentID:      msg.SegmentID,
			InstanceNum:    msg.InstanceNum,
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
				slog.Float64("percentOfTarget", float64(segment.FileSize)/float64(config.TargetFileSize)*100))
			segmentsToMarkCompacted = append(segmentsToMarkCompacted, segment)
			continue
		}

		activeSegments = append(activeSegments, segment)
	}

	if len(segmentsToMarkCompacted) > 0 {
		if err := p.markTraceSegmentsAsCompacted(ctx, segmentsToMarkCompacted, group.Key); err != nil {
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

	_, results, err := p.performTraceCompactionCore(ctx, tmpDir, storageClient, group.Key, storageProfile, activeSegments, recordCountEstimate)
	if err != nil {
		return fmt.Errorf("perform compaction: %w", err)
	}

	newSegments, err := p.uploadAndCreateTraceSegments(ctx, storageClient, storageProfile, results, group.Key, activeSegments)
	if err != nil {
		return fmt.Errorf("upload and create segments: %w", err)
	}

	if err := p.atomicTraceDatabaseUpdate(ctx, activeSegments, newSegments, kafkaCommitData, group.Key); err != nil {
		return fmt.Errorf("atomic database update: %w", err)
	}

	var totalRecords, totalSize int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalSize += result.FileSize
	}

	ll.Info("Trace compaction completed successfully",
		slog.Int("inputSegments", len(activeSegments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))
	return nil
}

// GetTargetRecordCount returns the target record count for a grouping key
func (p *TraceCompactionProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.TraceCompactionKey) int64 {
	return p.store.GetTraceEstimate(ctx, groupingKey.OrganizationID)
}

// Helper methods similar to logs but for traces
func (p *TraceCompactionProcessor) performTraceCompactionCore(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, compactionKey messages.TraceCompactionKey, storageProfile storageprofile.StorageProfile, activeSegments []lrdb.TraceSeg, recordCountEstimate int64) ([]lrdb.TraceSeg, []parquetwriter.Result, error) {
	params := traceProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: compactionKey.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: activeSegments,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	result, err := processTracesWithSorting(ctx, params)
	if err != nil {
		return nil, nil, err
	}

	return result.ProcessedSegments, result.Results, nil
}

func (p *TraceCompactionProcessor) uploadAndCreateTraceSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.TraceCompactionKey, inputSegments []lrdb.TraceSeg) ([]lrdb.TraceSeg, error) {
	ll := logctx.FromContext(ctx)

	var totalInputSize, totalInputRecords int64
	for _, seg := range inputSegments {
		totalInputSize += seg.FileSize
		totalInputRecords += seg.RecordCount
	}

	var segments []lrdb.TraceSeg
	var totalOutputSize, totalOutputRecords int64
	var segmentIDs []int64

	// Generate unique batch IDs for all results to help avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(results))

	for i, result := range results {
		segmentID := batchSegmentIDs[i]

		// Get metadata from result
		stats, ok := result.Metadata.(factories.TracesFileStats)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}

		// Upload the file
		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, p.getHourFromTimestamp(stats.FirstTS), segmentID, "traces")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload file %s: %w", result.FileName, err)
		}

		segment := lrdb.TraceSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
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
			Compacted:    true,
			Published:    true,
			Fingerprints: stats.Fingerprints,
			CreatedBy:    lrdb.CreatedByCompact,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount
		segmentIDs = append(segmentIDs, segmentID)
	}

	reportTelemetry(ctx, "compaction", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Trace segment upload completed",
		slog.Int("inputFiles", len(inputSegments)),
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Any("createdSegmentIDs", segmentIDs))

	return segments, nil
}

func (p *TraceCompactionProcessor) atomicTraceDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.TraceSeg, kafkaCommitData *KafkaCommitData, key messages.TraceCompactionKey) error {
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
	oldRecords := make([]lrdb.CompactTraceSegsOld, len(oldSegments))
	for i, seg := range oldSegments {
		oldRecords[i] = lrdb.CompactTraceSegsOld{
			SegmentID: seg.SegmentID,
		}
	}

	newRecords := make([]lrdb.CompactTraceSegsNew, len(newSegments))
	for i, seg := range newSegments {
		newRecords[i] = lrdb.CompactTraceSegsNew{
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
	params := lrdb.CompactTraceSegsParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		InstanceNum:    key.InstanceNum,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
		CreatedBy:      lrdb.CreatedByCompact,
	}

	// Perform atomic operation with Kafka offsets
	if err := p.store.CompactTraceSegsWithKafkaOffsets(ctx, params, kafkaOffsets); err != nil {
		ll.Error("Failed CompactTraceSegsWithKafkaOffsets",
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
			ll.Error("CompactTraceSegs old segment IDs",
				slog.Any("old_segment_ids", oldSegmentIDs))
		}

		if len(newSegments) > 0 {
			newSegmentIDs := make([]int64, len(newSegments))
			for i, seg := range newSegments {
				newSegmentIDs[i] = seg.SegmentID
			}
			ll.Error("CompactTraceSegs new segment IDs",
				slog.Any("new_segment_ids", newSegmentIDs))
		}

		return fmt.Errorf("failed to compact trace segments: %w", err)
	}

	return nil
}

func (p *TraceCompactionProcessor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

func (p *TraceCompactionProcessor) markTraceSegmentsAsCompacted(ctx context.Context, segments []lrdb.TraceSeg, key messages.TraceCompactionKey) error {
	if len(segments) == 0 {
		return nil
	}

	segmentIDs := make([]int64, len(segments))
	for i, seg := range segments {
		segmentIDs[i] = seg.SegmentID
	}

	return p.store.MarkTraceSegsCompactedByKeys(ctx, lrdb.MarkTraceSegsCompactedByKeysParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		InstanceNum:    key.InstanceNum,
		SegmentIds:     segmentIDs,
	})
}
