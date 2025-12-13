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
	"log/slog"
	"os"
	"runtime"

	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// LogCompactionProcessor implements compaction processing for logs
type LogCompactionProcessor struct {
	store           LogCompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	config          *config.Config
}

// NewLogCompactionProcessor creates a new log compaction processor
func NewLogCompactionProcessor(
	store LogCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg *config.Config,
) *LogCompactionProcessor {
	return &LogCompactionProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		config:          cfg,
	}
}

// ProcessBundleFromQueue processes a log compaction bundle from the work queue
func (p *LogCompactionProcessor) ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error {
	ll := logctx.FromContext(ctx)

	// Extract bundle from work item spec
	var bundle messages.LogCompactionBundle
	specBytes, err := json.Marshal(workItem.Spec())
	if err != nil {
		return fmt.Errorf("failed to marshal work item spec: %w", err)
	}

	if err := json.Unmarshal(specBytes, &bundle); err != nil {
		return fmt.Errorf("failed to unmarshal log compaction bundle: %w", err)
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Skipping empty bundle")
		return nil
	}

	// Extract key from first message
	firstMsg := bundle.Messages[0]
	key := firstMsg.GroupingKey().(messages.LogCompactionKey)

	// Call the existing ProcessBundle with 0 for partition and offset (not needed anymore)
	return p.ProcessBundle(ctx, key, bundle.Messages, 0, 0)
}

// ProcessBundle processes a compaction bundle directly (simplified interface)
func (p *LogCompactionProcessor) ProcessBundle(ctx context.Context, key messages.LogCompactionKey, msgs []*messages.LogCompactionMessage, partition int32, offset int64) error {
	ll := logctx.FromContext(ctx)

	defer runtime.GC() // TODO find a way to not need this

	if len(msgs) == 0 {
		return nil
	}

	ll.Info("Starting compaction processing",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("dateint", int(key.DateInt)),
		slog.Int("instanceNum", int(key.InstanceNum)),
		slog.Int("messageCount", len(msgs)))

	recordCountEstimate := p.store.GetLogEstimate(ctx, key.OrganizationID)

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

	ctx, storageSpan := boxerTracer.Start(ctx, "logs.compact.setup_storage", trace.WithAttributes(
		attribute.String("organization_id", key.OrganizationID.String()),
		attribute.Int("dateint", int(key.DateInt)),
		attribute.Int("instance_num", int(key.InstanceNum)),
	))

	storageProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, key.OrganizationID, key.InstanceNum)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to get storage profile")
		storageSpan.End()
		return fmt.Errorf("get storage profile: %w", err)
	}

	storageClient, err := cloudstorage.NewClient(ctx, p.cmgr, storageProfile)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to create storage client")
		storageSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}
	storageSpan.End()

	ctx, fetchSpan := boxerTracer.Start(ctx, "logs.compact.fetch_segments", trace.WithAttributes(
		attribute.Int("message_count", len(msgs)),
	))

	// Process segments
	var activeSegments []lrdb.LogSeg
	var segmentsToMarkCompacted []lrdb.LogSeg
	targetSizeThreshold := config.TargetFileSize * 80 / 100 // 80% of target file size

	for _, msg := range msgs {
		segment, err := p.store.GetLogSeg(ctx, lrdb.GetLogSegParams{
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

	fetchSpan.SetAttributes(
		attribute.Int("active_segments", len(activeSegments)),
		attribute.Int("segments_to_mark_compacted", len(segmentsToMarkCompacted)),
	)
	fetchSpan.End()

	if len(segmentsToMarkCompacted) > 0 {
		if err := p.markLogSegmentsAsCompacted(ctx, segmentsToMarkCompacted, key); err != nil {
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

	ctx, processSpan := boxerTracer.Start(ctx, "logs.compact.process_core", trace.WithAttributes(
		attribute.Int("active_segments", len(activeSegments)),
	))

	results, err := p.performLogCompactionCore(ctx, tmpDir, storageClient, key, storageProfile, activeSegments, recordCountEstimate)
	if err != nil {
		processSpan.RecordError(err)
		processSpan.SetStatus(codes.Error, "failed to perform compaction")
		processSpan.End()
		ll.Error("Failed to perform log compaction core processing, skipping bundle",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("activeSegments", len(activeSegments)),
			slog.Any("error", err))
		return nil
	}
	processSpan.SetAttributes(attribute.Int("output_files", len(results)))
	processSpan.End()

	ctx, uploadSpan := boxerTracer.Start(ctx, "logs.compact.upload_segments", trace.WithAttributes(
		attribute.Int("result_count", len(results)),
	))

	newSegments, err := p.uploadAndCreateLogSegments(ctx, storageClient, storageProfile, results, key, activeSegments)
	if err != nil {
		uploadSpan.RecordError(err)
		uploadSpan.SetStatus(codes.Error, "failed to upload segments")
		uploadSpan.End()
		ll.Error("Failed to upload and create log segments, skipping bundle",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("resultsCount", len(results)),
			slog.Any("error", err))
		return nil
	}
	uploadSpan.SetAttributes(attribute.Int("new_segment_count", len(newSegments)))
	uploadSpan.End()

	_, dbSpan := boxerTracer.Start(ctx, "logs.compact.update_db", trace.WithAttributes(
		attribute.Int("old_segment_count", len(activeSegments)),
		attribute.Int("new_segment_count", len(newSegments)),
	))

	if err := p.atomicLogDatabaseUpdate(ctx, activeSegments, newSegments, key); err != nil {
		dbSpan.RecordError(err)
		dbSpan.SetStatus(codes.Error, "failed to update database")
		dbSpan.End()
		ll.Error("Failed to perform atomic database update, skipping bundle",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("activeSegments", len(activeSegments)),
			slog.Int("newSegments", len(newSegments)),
			slog.Any("error", err))
		return nil
	}
	dbSpan.End()

	var totalRecords, totalSize int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalSize += result.FileSize
	}

	ll.Info("Log compaction completed successfully",
		slog.Int("inputSegments", len(activeSegments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))

	return nil
}

// GetTargetRecordCount returns the target record count for a grouping key
func (p *LogCompactionProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.LogCompactionKey) int64 {
	return p.store.GetLogEstimate(ctx, groupingKey.OrganizationID)
}

// ShouldEmitImmediately returns false - log compaction always uses normal grouping.
func (p *LogCompactionProcessor) ShouldEmitImmediately(msg *messages.LogCompactionMessage) bool {
	return false
}

func (p *LogCompactionProcessor) performLogCompactionCore(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, compactionKey messages.LogCompactionKey, storageProfile storageprofile.StorageProfile, activeSegments []lrdb.LogSeg, recordCountEstimate int64) ([]parquetwriter.Result, error) {
	params := logProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: compactionKey.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: activeSegments,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	results, err := processLogsWithSorting(ctx, params)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func (p *LogCompactionProcessor) uploadAndCreateLogSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.LogCompactionKey, inputSegments []lrdb.LogSeg) ([]lrdb.LogSeg, error) {
	ll := logctx.FromContext(ctx)

	var totalInputSize, totalInputRecords int64
	for _, seg := range inputSegments {
		totalInputSize += seg.FileSize
		totalInputRecords += seg.RecordCount
	}

	// Merge label name maps from input segments for legacy API support
	mergedLabelMap := mergeLabelNameMaps(inputSegments)

	var segments []lrdb.LogSeg
	var totalOutputSize, totalOutputRecords int64
	var segmentIDs []int64

	// Generate unique batch IDs for all results to help avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(results))

	for i, result := range results {
		segmentID := batchSegmentIDs[i]

		stats, ok := result.Metadata.(factories.LogsFileStats)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type: %T", result.Metadata)
		}

		// Emit metric for rows missing stream identification field
		if stats.MissingStreamFieldCount > 0 {
			factories.StreamFieldMissingCounter.Add(ctx, stats.MissingStreamFieldCount)
		}

		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, p.getHourFromTimestamp(stats.FirstTS), segmentID, "logs")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload file %s: %w", result.FileName, err)
		}

		// Clean up local file
		_ = os.Remove(result.FileName)

		segment := lrdb.LogSeg{
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
			RecordCount:   result.RecordCount,
			FileSize:      result.FileSize,
			Published:     true,
			Compacted:     true,
			Fingerprints:  stats.Fingerprints,
			CreatedBy:     lrdb.CreatedByCompact,
			LabelNameMap:  mergedLabelMap,
			StreamIds:     stats.StreamValues,
			StreamIDField: stats.StreamIdField,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount
		segmentIDs = append(segmentIDs, segmentID)
	}

	reportTelemetry(ctx, "logs", "compaction", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Log segment upload completed",
		slog.Int("inputFiles", len(inputSegments)),
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Any("createdSegmentIDs", segmentIDs))

	return segments, nil
}

func (p *LogCompactionProcessor) atomicLogDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.LogSeg, key messages.LogCompactionKey) error {
	ll := logctx.FromContext(ctx)

	oldRecords := make([]lrdb.CompactLogSegsOld, len(oldSegments))
	for i, seg := range oldSegments {
		oldRecords[i] = lrdb.CompactLogSegsOld{
			SegmentID: seg.SegmentID,
		}
	}

	newRecords := make([]lrdb.CompactLogSegsNew, len(newSegments))
	for i, seg := range newSegments {
		newRecords[i] = lrdb.CompactLogSegsNew{
			SegmentID:     seg.SegmentID,
			StartTs:       seg.TsRange.Lower.Int64,
			EndTs:         seg.TsRange.Upper.Int64,
			RecordCount:   seg.RecordCount,
			FileSize:      seg.FileSize,
			Fingerprints:  seg.Fingerprints,
			LabelNameMap:  seg.LabelNameMap,
			StreamIds:     seg.StreamIds,
			StreamIdField: seg.StreamIDField,
			SortVersion:   lrdb.CurrentLogSortVersion,
		}
	}

	if len(newRecords) == 0 {
		return fmt.Errorf("no new segments to insert")
	}

	params := lrdb.CompactLogSegsParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		InstanceNum:    key.InstanceNum,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
		CreatedBy:      lrdb.CreatedByCompact,
	}

	if err := p.store.CompactLogSegments(ctx, params); err != nil {
		ll.Error("Failed CompactLogSegments",
			slog.Any("error", err),
			slog.String("organization_id", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("instance_num", int(key.InstanceNum)),
			slog.Int("old_segments_count", len(oldSegments)),
			slog.Int("new_segments_count", len(newSegments)))

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

func (p *LogCompactionProcessor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

func (p *LogCompactionProcessor) markLogSegmentsAsCompacted(ctx context.Context, segments []lrdb.LogSeg, key messages.LogCompactionKey) error {
	if len(segments) == 0 {
		return nil
	}

	segmentIDs := make([]int64, len(segments))
	for i, seg := range segments {
		segmentIDs[i] = seg.SegmentID
	}

	return p.store.MarkLogSegsCompactedByKeys(ctx, lrdb.MarkLogSegsCompactedByKeysParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		InstanceNum:    key.InstanceNum,
		SegmentIds:     segmentIDs,
	})
}

// mergeLabelNameMaps merges label name maps from multiple input segments.
// Returns nil if no segments have label maps, otherwise returns a merged JSONB map.
func mergeLabelNameMaps(inputSegments []lrdb.LogSeg) []byte {
	merged := make(map[string]string)

	for _, seg := range inputSegments {
		if len(seg.LabelNameMap) == 0 {
			continue
		}

		// Parse existing JSON
		var segMap map[string]string
		if err := json.Unmarshal(seg.LabelNameMap, &segMap); err != nil {
			// Skip segments with invalid label maps
			continue
		}

		// Merge into result - later segments override earlier ones
		for k, v := range segMap {
			merged[k] = v
		}
	}

	if len(merged) == 0 {
		return nil
	}

	// Serialize back to JSON
	result, err := json.Marshal(merged)
	if err != nil {
		return nil // Return nil on error
	}

	return result
}
