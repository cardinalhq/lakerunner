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

// MetricCompactionProcessor implements compaction processing for metrics
type MetricCompactionProcessor struct {
	store           MetricCompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	config          *config.Config
}

// NewMetricCompactionProcessor creates a new metric compaction processor
func NewMetricCompactionProcessor(
	store MetricCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg *config.Config,
) *MetricCompactionProcessor {
	return &MetricCompactionProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		config:          cfg,
	}
}

// GetTargetRecordCount returns the target record count for a grouping key
func (p *MetricCompactionProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return p.store.GetMetricEstimate(ctx, groupingKey.OrganizationID, groupingKey.FrequencyMs)
}

// ShouldEmitImmediately returns false - metric compaction always uses normal grouping.
func (p *MetricCompactionProcessor) ShouldEmitImmediately(msg *messages.MetricCompactionMessage) bool {
	return false
}

// Helper methods from original processor
func (p *MetricCompactionProcessor) performCompaction(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, compactionKey messages.CompactionKey, storageProfile storageprofile.StorageProfile, activeSegments []lrdb.MetricSeg, recordCountEstimate int64) (*metricProcessingResult, error) {
	params := metricProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: compactionKey.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: activeSegments,
		FrequencyMs:    compactionKey.FrequencyMs,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	return processMetricsWithDuckDB(ctx, params)
}

func (p *MetricCompactionProcessor) uploadAndCreateSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.CompactionKey, inputSegments []lrdb.MetricSeg) ([]lrdb.MetricSeg, error) {
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

		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, p.getHourFromTimestamp(stats.FirstTS), segmentID, "metrics")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload file %s: %w", result.FileName, err)
		}

		// Create new segment record
		segment := lrdb.MetricSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
			FrequencyMs:    key.FrequencyMs,
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
			Compacted:    true,
			Fingerprints: stats.Fingerprints,
			SortVersion:  lrdb.CurrentMetricSortVersion,
			CreatedBy:    lrdb.CreatedByCompact,
			MetricNames:  stats.MetricNames,
			MetricTypes:  stats.MetricTypes,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount
		segmentIDs = append(segmentIDs, segmentID)
	}

	reportTelemetry(ctx, "metrics", "compaction", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Segment upload completed",
		slog.Int("inputFiles", len(inputSegments)),
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int("outputSegments", len(segments)),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Any("createdSegmentIDs", segmentIDs))

	return segments, nil
}

func (p *MetricCompactionProcessor) atomicDatabaseUpdate(ctx context.Context, oldSegments, newSegments []lrdb.MetricSeg, key messages.CompactionKey) error {
	oldRecords := make([]lrdb.CompactMetricSegsOld, len(oldSegments))
	for i, seg := range oldSegments {
		oldRecords[i] = lrdb.CompactMetricSegsOld{
			SegmentID: seg.SegmentID,
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
			MetricNames:  seg.MetricNames,
			MetricTypes:  seg.MetricTypes,
		}
	}

	if len(newRecords) == 0 {
		return fmt.Errorf("no new segments to insert")
	}

	params := lrdb.CompactMetricSegsParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		FrequencyMs:    key.FrequencyMs,
		InstanceNum:    key.InstanceNum,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
		CreatedBy:      lrdb.CreatedByCompact,
	}

	if err := p.store.CompactMetricSegments(ctx, params); err != nil {
		ll := logctx.FromContext(ctx)

		// Log unique keys for debugging database failures
		ll.Error("Failed CompactMetricSegments",
			slog.Any("error", err),
			slog.String("organization_id", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("frequency_ms", int(key.FrequencyMs)),
			slog.Int("instance_num", int(key.InstanceNum)),
			slog.Int("old_segments_count", len(oldSegments)),
			slog.Int("new_segments_count", len(newSegments)))

		if len(oldSegments) > 0 {
			oldSegmentIDs := make([]int64, len(oldSegments))
			for i, seg := range oldSegments {
				oldSegmentIDs[i] = seg.SegmentID
			}
			ll.Error("CompactMetricSegs old segment IDs",
				slog.Any("old_segment_ids", oldSegmentIDs))
		}

		if len(newSegments) > 0 {
			newSegmentIDs := make([]int64, len(newSegments))
			for i, seg := range newSegments {
				newSegmentIDs[i] = seg.SegmentID
			}
			ll.Error("CompactMetricSegs new segment IDs",
				slog.Any("new_segment_ids", newSegmentIDs))
		}

		return fmt.Errorf("failed to compact metric segments: %w", err)
	}

	return nil
}

func (p *MetricCompactionProcessor) getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}

func (p *MetricCompactionProcessor) markSegmentsAsCompacted(ctx context.Context, segments []lrdb.MetricSeg, key messages.CompactionKey) error {
	if len(segments) == 0 {
		return nil
	}

	segmentIDs := make([]int64, len(segments))
	for i, seg := range segments {
		segmentIDs[i] = seg.SegmentID
	}

	return p.store.MarkMetricSegsCompactedByKeys(ctx, lrdb.MarkMetricSegsCompactedByKeysParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		FrequencyMs:    key.FrequencyMs,
		InstanceNum:    key.InstanceNum,
		SegmentIds:     segmentIDs,
	})
}

// ProcessBundleFromQueue processes a metric compaction bundle from the work queue
func (p *MetricCompactionProcessor) ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error {
	ll := logctx.FromContext(ctx)

	// Extract bundle from work item spec
	var bundle messages.MetricCompactionBundle
	specBytes, err := json.Marshal(workItem.Spec())
	if err != nil {
		return fmt.Errorf("failed to marshal work item spec: %w", err)
	}

	if err := json.Unmarshal(specBytes, &bundle); err != nil {
		return fmt.Errorf("failed to unmarshal metric compaction bundle: %w", err)
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Skipping empty bundle")
		return nil
	}

	// Extract key from first message
	firstMsg := bundle.Messages[0]
	key := firstMsg.GroupingKey().(messages.CompactionKey)

	// Call the existing ProcessBundle with 0 for partition and offset (not needed anymore)
	return p.ProcessBundle(ctx, key, bundle.Messages, 0, 0)
}

// ProcessBundle processes a compaction bundle directly (simplified interface)
func (p *MetricCompactionProcessor) ProcessBundle(ctx context.Context, key messages.CompactionKey, msgs []*messages.MetricCompactionMessage, partition int32, offset int64) error {
	ll := logctx.FromContext(ctx)

	if len(msgs) == 0 {
		return nil
	}

	ll.Info("Starting compaction processing",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("dateint", int(key.DateInt)),
		slog.Int("frequencyMs", int(key.FrequencyMs)),
		slog.Int("instanceNum", int(key.InstanceNum)),
		slog.Int("messageCount", len(msgs)))

	recordCountEstimate := p.store.GetMetricEstimate(ctx, key.OrganizationID, key.FrequencyMs)

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

	ctx, storageSpan := boxerTracer.Start(ctx, "metrics.compact.setup_storage", trace.WithAttributes(
		attribute.String("organization_id", key.OrganizationID.String()),
		attribute.Int("dateint", int(key.DateInt)),
		attribute.Int("frequency_ms", int(key.FrequencyMs)),
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

	ctx, fetchSpan := boxerTracer.Start(ctx, "metrics.compact.fetch_segments", trace.WithAttributes(
		attribute.Int("message_count", len(msgs)),
	))

	var activeSegments []lrdb.MetricSeg
	var segmentsToMarkCompacted []lrdb.MetricSeg
	targetSizeThreshold := config.TargetFileSize * 80 / 100 // 80% of target file size

	for _, msg := range msgs {
		segment, err := p.store.GetMetricSeg(ctx, lrdb.GetMetricSegParams{
			OrganizationID: msg.OrganizationID,
			Dateint:        msg.DateInt,
			FrequencyMs:    msg.FrequencyMs,
			SegmentID:      msg.SegmentID,
			InstanceNum:    msg.InstanceNum,
		})
		if err != nil {
			ll.Warn("Failed to fetch segment, skipping",
				slog.Int64("segmentID", msg.SegmentID),
				slog.String("organizationID", msg.OrganizationID.String()),
				slog.Int("dateint", int(msg.DateInt)),
				slog.Int("frequencyMs", int(msg.FrequencyMs)),
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
		if err := p.markSegmentsAsCompacted(ctx, segmentsToMarkCompacted, key); err != nil {
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

	ctx, processSpan := boxerTracer.Start(ctx, "metrics.compact.process_core", trace.WithAttributes(
		attribute.Int("active_segments", len(activeSegments)),
	))

	processedResult, err := p.performCompaction(ctx, tmpDir, storageClient, key, storageProfile, activeSegments, recordCountEstimate)
	if err != nil {
		processSpan.RecordError(err)
		processSpan.SetStatus(codes.Error, "failed to perform compaction")
		processSpan.End()
		ll.Error("Failed to perform metric compaction core processing, skipping bundle",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("frequencyMs", int(key.FrequencyMs)),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("activeSegments", len(activeSegments)),
			slog.Any("error", err))
		return nil
	}

	// Use only successfully downloaded segments for database updates
	processedSegments := processedResult.ProcessedSegments
	results := processedResult.Results

	processSpan.SetAttributes(
		attribute.Int("output_files", len(results)),
		attribute.Int("processed_segments", len(processedSegments)),
		attribute.Int("skipped_segments", len(activeSegments)-len(processedSegments)),
	)
	processSpan.End()

	ctx, uploadSpan := boxerTracer.Start(ctx, "metrics.compact.upload_segments", trace.WithAttributes(
		attribute.Int("result_count", len(results)),
	))

	newSegments, err := p.uploadAndCreateSegments(ctx, storageClient, storageProfile, results, key, processedSegments)
	if err != nil {
		uploadSpan.RecordError(err)
		uploadSpan.SetStatus(codes.Error, "failed to upload segments")
		uploadSpan.End()
		ll.Error("Failed to upload and create metric segments, skipping bundle",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("frequencyMs", int(key.FrequencyMs)),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("resultsCount", len(results)),
			slog.Any("error", err))
		return nil
	}
	uploadSpan.SetAttributes(attribute.Int("new_segment_count", len(newSegments)))
	uploadSpan.End()

	ctx, dbSpan := boxerTracer.Start(ctx, "metrics.compact.update_db", trace.WithAttributes(
		attribute.Int("old_segment_count", len(processedSegments)),
		attribute.Int("new_segment_count", len(newSegments)),
	))

	if err := p.atomicDatabaseUpdate(ctx, processedSegments, newSegments, key); err != nil {
		dbSpan.RecordError(err)
		dbSpan.SetStatus(codes.Error, "failed to update database")
		dbSpan.End()
		ll.Error("Failed to perform atomic database update for metric compaction, skipping bundle",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.DateInt)),
			slog.Int("frequencyMs", int(key.FrequencyMs)),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("processedSegments", len(processedSegments)),
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

	ll.Info("Compaction completed successfully",
		slog.Int("inputSegments", len(activeSegments)),
		slog.Int("processedSegments", len(processedSegments)),
		slog.Int("outputFiles", len(results)),
		slog.Int64("outputRecords", totalRecords),
		slog.Int64("outputFileSize", totalSize))

	return nil
}
