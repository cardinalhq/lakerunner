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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// TraceGroupValidationError represents an error when messages in a group have inconsistent fields
type TraceGroupValidationError struct {
	Field    string
	Expected any
	Got      any
	Message  string
}

func (e *TraceGroupValidationError) Error() string {
	return fmt.Sprintf("trace group validation failed - %s: expected %v, got %v (%s)", e.Field, e.Expected, e.Got, e.Message)
}

// TraceCompactionStore defines database operations needed for trace compaction
type TraceCompactionStore interface {
	GetTraceSeg(ctx context.Context, params lrdb.GetTraceSegParams) (lrdb.TraceSeg, error)
	CompactTraceSegsWithKafkaOffsets(ctx context.Context, params lrdb.CompactTraceSegsParams, kafkaOffsets []lrdb.KafkaOffsetUpdate) error
	MarkTraceSegsCompactedByKeys(ctx context.Context, params lrdb.MarkTraceSegsCompactedByKeysParams) error
	KafkaGetLastProcessed(ctx context.Context, params lrdb.KafkaGetLastProcessedParams) (int64, error)
	GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64
	InsertSegmentJournal(ctx context.Context, params lrdb.InsertSegmentJournalParams) error
}

// TraceCompactionProcessor implements the Processor interface for trace segment compaction
type TraceCompactionProcessor struct {
	store           TraceCompactionStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
}

// newTraceCompactor creates a new trace compactor instance
func newTraceCompactor(store TraceCompactionStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, cfg *config.Config) *TraceCompactionProcessor {
	return &TraceCompactionProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
	}
}

// validateTraceGroupConsistency ensures all messages in a group have consistent org, instance, and dateint
func validateTraceGroupConsistency(group *accumulationGroup[messages.TraceCompactionKey]) error {
	if len(group.Messages) == 0 {
		return &TraceGroupValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	expectedOrg := group.Key.OrganizationID
	expectedInstance := group.Key.InstanceNum
	expectedDateInt := group.Key.DateInt

	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.TraceCompactionMessage)
		if !ok {
			return &TraceGroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not a TraceCompactionMessage", i),
			}
		}

		if msg.OrganizationID != expectedOrg {
			return &TraceGroupValidationError{
				Field:    "organization_id",
				Expected: expectedOrg,
				Got:      msg.OrganizationID,
				Message:  fmt.Sprintf("message %d has inconsistent organization ID", i),
			}
		}

		if msg.InstanceNum != expectedInstance {
			return &TraceGroupValidationError{
				Field:    "instance_num",
				Expected: expectedInstance,
				Got:      msg.InstanceNum,
				Message:  fmt.Sprintf("message %d has inconsistent instance number", i),
			}
		}

		if msg.DateInt != expectedDateInt {
			return &TraceGroupValidationError{
				Field:    "dateint",
				Expected: expectedDateInt,
				Got:      msg.DateInt,
				Message:  fmt.Sprintf("message %d has inconsistent dateint", i),
			}
		}

	}

	return nil
}

// Process implements the Processor interface and performs trace segment compaction
func (p *TraceCompactionProcessor) Process(ctx context.Context, group *accumulationGroup[messages.TraceCompactionKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	groupAge := time.Since(group.CreatedAt)

	ll.Info("Processing trace compaction notification group",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	if err := validateTraceGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Extract all segment IDs from the compaction messages
	var segmentIDs []int64
	segmentRecords := int64(0)
	segmentSize := int64(0)

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.TraceCompactionMessage)
		if !ok {
			continue
		}
		segmentIDs = append(segmentIDs, msg.SegmentID)
		segmentRecords += msg.Records
		segmentSize += msg.FileSize
	}

	ll.Info("Received trace compaction notifications",
		slog.Int("segmentCount", len(segmentIDs)),
		slog.Int64("totalRecords", segmentRecords),
		slog.Int64("totalSize", segmentSize),
		slog.Any("segmentIDs", segmentIDs))

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

	storageProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	storageClient, err := cloudstorage.NewClient(ctx, p.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	if err := p.performTraceCompaction(ctx, tmpDir, storageClient, group, storageProfile, kafkaCommitData); err != nil {
		return fmt.Errorf("perform trace compaction: %w", err)
	}

	ll.Info("Trace compaction notification processing completed",
		slog.Int("segmentCount", len(segmentIDs)))

	return nil
}

// GetTargetRecordCount returns the target record count for accumulation (similar to logs)
func (p *TraceCompactionProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.TraceCompactionKey) int64 {
	return p.store.GetTraceEstimate(ctx, groupingKey.OrganizationID)
}

// performTraceCompaction handles the core trace compaction logic
func (p *TraceCompactionProcessor) performTraceCompaction(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, group *accumulationGroup[messages.TraceCompactionKey], storageProfile storageprofile.StorageProfile, kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

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
			ll.Warn("Failed to fetch trace segment, skipping",
				slog.Int64("segmentID", msg.SegmentID),
				slog.String("organizationID", msg.OrganizationID.String()),
				slog.Int("dateint", int(msg.DateInt)),
				slog.Int("instanceNum", int(msg.InstanceNum)),
				slog.Any("error", err))
			continue
		}

		if segment.Compacted {
			ll.Info("Trace segment already marked as compacted, skipping", slog.Int64("segmentID", segment.SegmentID))
			continue
		}

		if segment.FileSize >= targetSizeThreshold {
			ll.Info("Trace segment already close to target size, marking as compacted",
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
			ll.Warn("Failed to mark trace segments as compacted", slog.Any("error", err))
		} else {
			ll.Info("Marked trace segments as compacted",
				slog.Int("segmentCount", len(segmentsToMarkCompacted)))
		}
	}

	if len(activeSegments) == 0 {
		ll.Info("No active trace segments to compact")
		return nil
	}

	ll.Info("Found trace segments to compact", slog.Int("activeSegments", len(activeSegments)))

	processedSegments, results, err := p.performTraceCompactionCore(ctx, tmpDir, storageClient, group.Key, storageProfile, activeSegments)
	if err != nil {
		return fmt.Errorf("perform trace compaction core: %w", err)
	}

	newSegments, err := p.uploadAndCreateTraceSegments(ctx, storageClient, storageProfile, results, group.Key, processedSegments)
	if err != nil {
		return fmt.Errorf("upload and create trace segments: %w", err)
	}

	// Convert KafkaCommitData to KafkaOffsetUpdate slice for database storage
	var kafkaOffsets []lrdb.KafkaOffsetUpdate
	if kafkaCommitData != nil && len(kafkaCommitData.Offsets) > 0 {
		for partition, offset := range kafkaCommitData.Offsets {
			kafkaOffsets = append(kafkaOffsets, lrdb.KafkaOffsetUpdate{
				ConsumerGroup:  kafkaCommitData.ConsumerGroup,
				Topic:          kafkaCommitData.Topic,
				Partition:      partition,
				Offset:         offset,
				OrganizationID: group.Key.OrganizationID,
				InstanceNum:    group.Key.InstanceNum,
			})
		}
	}

	// Perform the database transaction to replace old segments with new ones
	compactTraceSegsParams := lrdb.CompactTraceSegsParams{
		OrganizationID: group.Key.OrganizationID,
		Dateint:        group.Key.DateInt,
		InstanceNum:    group.Key.InstanceNum,
		CreatedBy:      lrdb.CreatedByCompact,
		NewRecords:     make([]lrdb.CompactTraceSegsNew, len(newSegments)),
		OldRecords:     make([]lrdb.CompactTraceSegsOld, len(processedSegments)),
	}

	for i, seg := range newSegments {
		compactTraceSegsParams.NewRecords[i] = lrdb.CompactTraceSegsNew{
			SegmentID:    seg.SegmentID,
			RecordCount:  seg.RecordCount,
			FileSize:     seg.FileSize,
			StartTs:      0, // TODO: Extract from segment metadata
			EndTs:        0, // TODO: Extract from segment metadata
			Fingerprints: seg.Fingerprints,
		}
	}

	for i, seg := range processedSegments {
		compactTraceSegsParams.OldRecords[i] = lrdb.CompactTraceSegsOld{
			SegmentID: seg.SegmentID,
		}
	}

	if err := p.store.CompactTraceSegsWithKafkaOffsets(ctx, compactTraceSegsParams, kafkaOffsets); err != nil {
		return fmt.Errorf("compact trace segments with kafka offsets: %w", err)
	}

	ll.Info("Trace compaction completed successfully",
		slog.Int("inputSegments", len(processedSegments)),
		slog.Int("outputSegments", len(newSegments)))

	return nil
}

// performTraceCompactionCore handles the core compaction logic: creating readers and writing
func (p *TraceCompactionProcessor) performTraceCompactionCore(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, compactionKey messages.TraceCompactionKey, storageProfile storageprofile.StorageProfile, activeSegments []lrdb.TraceSeg) ([]lrdb.TraceSeg, []parquetwriter.Result, error) {
	// For traces, we use trace processing with trace-specific sorting
	params := traceProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: compactionKey.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: activeSegments,
		MaxRecords:     p.store.GetTraceEstimate(ctx, compactionKey.OrganizationID) * 2, // safety net
	}

	result, err := processTracesWithSorting(ctx, params)
	if err != nil {
		return nil, nil, err
	}

	return result.ProcessedSegments, result.Results, nil
}

// uploadAndCreateTraceSegments uploads the files and creates new trace segment records
func (p *TraceCompactionProcessor) uploadAndCreateTraceSegments(ctx context.Context, client cloudstorage.Client, profile storageprofile.StorageProfile, results []parquetwriter.Result, key messages.TraceCompactionKey, inputSegments []lrdb.TraceSeg) ([]lrdb.TraceSeg, error) {
	ll := logctx.FromContext(ctx)

	var totalInputSize, totalInputRecords int64
	for _, seg := range inputSegments {
		totalInputSize += seg.FileSize
		totalInputRecords += seg.RecordCount
	}

	var segments []lrdb.TraceSeg
	var totalOutputSize, totalOutputRecords int64

	// Generate unique batch IDs for all results to help avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(results))

	for i, result := range results {
		segmentID := batchSegmentIDs[i]

		// Get metadata from result
		stats, ok := result.Metadata.(factories.TracesFileStats)
		if !ok {
			return nil, fmt.Errorf("unexpected metadata type for traces: %T", result.Metadata)
		}

		// Upload the file
		objectPath := helpers.MakeDBObjectID(key.OrganizationID, profile.CollectorName, key.DateInt, int16(p.getHourFromTimestamp(stats.FirstTS)), segmentID, "traces")
		if err := client.UploadObject(ctx, profile.Bucket, objectPath, result.FileName); err != nil {
			return nil, fmt.Errorf("upload trace object: %w", err)
		}

		// Create segment record
		segment := lrdb.TraceSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.DateInt,
			SegmentID:      segmentID,
			InstanceNum:    key.InstanceNum,
			Fingerprints:   stats.Fingerprints,
			RecordCount:    result.RecordCount,
			FileSize:       result.FileSize,
			CreatedBy:      lrdb.CreatedByCompact,
			Compacted:      true,
			Published:      true,
		}

		segments = append(segments, segment)
		totalOutputSize += result.FileSize
		totalOutputRecords += result.RecordCount

		ll.Info("Uploaded compacted trace segment",
			slog.Int64("segmentID", segmentID),
			slog.String("objectPath", objectPath),
			slog.Int64("fileSize", result.FileSize),
			slog.Int64("recordCount", result.RecordCount))
	}

	ll.Info("Trace compaction upload summary",
		slog.Int64("totalInputSize", totalInputSize),
		slog.Int64("totalInputRecords", totalInputRecords),
		slog.Int64("totalOutputSize", totalOutputSize),
		slog.Int64("totalOutputRecords", totalOutputRecords),
		slog.Float64("compressionRatio", float64(totalInputSize)/float64(totalOutputSize)),
		slog.Int("inputSegments", len(inputSegments)),
		slog.Int("outputSegments", len(segments)))

	return segments, nil
}

// markTraceSegmentsAsCompacted marks segments as compacted without performing actual compaction
func (p *TraceCompactionProcessor) markTraceSegmentsAsCompacted(ctx context.Context, segments []lrdb.TraceSeg, key messages.TraceCompactionKey) error {
	var segmentIDs []int64
	for _, seg := range segments {
		segmentIDs = append(segmentIDs, seg.SegmentID)
	}

	return p.store.MarkTraceSegsCompactedByKeys(ctx, lrdb.MarkTraceSegsCompactedByKeysParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.DateInt,
		InstanceNum:    key.InstanceNum,
		SegmentIds:     segmentIDs,
	})
}

// getHourFromTimestamp extracts the hour from a timestamp (same logic as logs)
func (p *TraceCompactionProcessor) getHourFromTimestamp(timestamp int64) int {
	return int((timestamp / 1000 / 3600) % 24)
}

// traceProcessingParams contains the parameters needed for trace processing
type traceProcessingParams struct {
	TmpDir         string
	StorageClient  cloudstorage.Client
	OrganizationID uuid.UUID
	StorageProfile storageprofile.StorageProfile
	ActiveSegments []lrdb.TraceSeg
	MaxRecords     int64
}

// traceProcessingResult contains the results of trace processing
type traceProcessingResult struct {
	ProcessedSegments []lrdb.TraceSeg
	Results           []parquetwriter.Result
}

// processTracesWithSorting performs the common pattern of:
// 1. Create reader stack from trace segments
// 2. Create traces writer (traces need trace-specific sorting)
// 3. Write from reader to writer
// 4. Close writer and return results
func processTracesWithSorting(ctx context.Context, params traceProcessingParams) (*traceProcessingResult, error) {
	// Create reader stack
	readerStack, err := createTraceReaderStack(
		ctx,
		params.TmpDir,
		params.StorageClient,
		params.StorageProfile,
		params.ActiveSegments,
	)
	if err != nil {
		return nil, fmt.Errorf("create trace reader stack: %w", err)
	}
	defer readerStack.Close()

	// Create traces writer
	writer, err := factories.NewTracesWriter(params.TmpDir, params.MaxRecords)
	if err != nil {
		return nil, fmt.Errorf("create trace writer: %w", err)
	}

	// Process all data from reader to writer
	for {
		batch, err := readerStack.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("read batch from trace reader stack: %w", err)
		}
		if batch == nil {
			break
		}

		if err := writer.WriteBatch(batch); err != nil {
			return nil, fmt.Errorf("write batch to trace writer: %w", err)
		}
	}

	// Close writer and get results
	results, err := writer.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("close trace writer: %w", err)
	}

	return &traceProcessingResult{
		ProcessedSegments: params.ActiveSegments,
		Results:           results,
	}, nil
}

// createTraceReaderStack creates a reader stack for trace segments
func createTraceReaderStack(ctx context.Context, tmpDir string, storageClient cloudstorage.Client, storageProfile storageprofile.StorageProfile, segments []lrdb.TraceSeg) (filereader.Reader, error) {
	// This function would need to be implemented similar to createLogReaderStack
	// but for trace segments. For now, return an error to indicate it needs implementation.
	return nil, fmt.Errorf("createTraceReaderStack not implemented yet")
}
