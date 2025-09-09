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

// RollupStore defines database operations needed for rollups
type RollupStore interface {
	GetMetricSeg(ctx context.Context, params lrdb.GetMetricSegParams) (lrdb.MetricSeg, error)
	RollupMetricSegsWithKafkaOffsetsWithOrg(ctx context.Context, sourceParams lrdb.RollupSourceParams, targetParams lrdb.RollupTargetParams, sourceSegmentIDs []int64, newRecords []lrdb.RollupNewRecord, kafkaOffsets []lrdb.KafkaOffsetUpdateWithOrg) error
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
	GetMetricEstimate(ctx context.Context, orgID uuid.UUID, frequencyMs int32) int64
}

// MetricRollupProcessor implements the Processor interface for metric rollups
type MetricRollupProcessor struct {
	store           RollupStore
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
}

// NewMetricRollupProcessor creates a new metric rollup processor instance
func NewMetricRollupProcessor(store RollupStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider) *MetricRollupProcessor {
	return &MetricRollupProcessor{
		store:           store,
		storageProvider: storageProvider,
		cmgr:            cmgr,
	}
}

// validateRollupGroupConsistency ensures all messages in a rollup group have consistent fields
func validateRollupGroupConsistency(group *AccumulationGroup[messages.RollupKey]) error {
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
	expectedSourceFreq := group.Key.SourceFrequencyMs
	expectedTargetFreq := group.Key.TargetFrequencyMs
	expectedSlotID := group.Key.SlotID
	expectedSlotCount := group.Key.SlotCount

	// Validate each message against the expected values
	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricRollupMessage)
		if !ok {
			return &GroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not a MetricRollupMessage", i),
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

		if msg.SlotID != expectedSlotID {
			return &GroupValidationError{
				Field:    "slot_id",
				Expected: expectedSlotID,
				Got:      msg.SlotID,
				Message:  fmt.Sprintf("message %d has inconsistent slot ID", i),
			}
		}

		if msg.SlotCount != expectedSlotCount {
			return &GroupValidationError{
				Field:    "slot_count",
				Expected: expectedSlotCount,
				Got:      msg.SlotCount,
				Message:  fmt.Sprintf("message %d has inconsistent slot count", i),
			}
		}
	}

	return nil
}

// Process implements the Processor interface and performs rollup aggregation
func (r *MetricRollupProcessor) Process(ctx context.Context, group *AccumulationGroup[messages.RollupKey], kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	// Calculate group age from Hunter timestamp
	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting rollup processing",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("dateint", int(group.Key.DateInt)),
		slog.Int("sourceFrequencyMs", int(group.Key.SourceFrequencyMs)),
		slog.Int("targetFrequencyMs", int(group.Key.TargetFrequencyMs)),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("slotID", int(group.Key.SlotID)),
		slog.Int("slotCount", int(group.Key.SlotCount)),
		slog.Int64("truncatedTimebox", group.Key.TruncatedTimebox),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	recordCountEstimate := r.store.GetMetricEstimate(ctx, group.Key.OrganizationID, group.Key.TargetFrequencyMs)

	// Step 0: Validate that all messages in the group have consistent fields
	if err := validateRollupGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Create temporary directory for this rollup run
	tmpDir, err := os.MkdirTemp("", "rollup-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	// Step 1: Get the storage profile for the given org/instance
	storageProfile, err := r.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	// Step 2: Make a storage client from that profile
	storageClient, err := cloudstorage.NewClient(ctx, r.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	// Step 3: Fetch the segments from the DB by iterating over messages
	var segments []lrdb.MetricSeg
	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.MetricRollupMessage)
		if !ok {
			continue // Skip non-MetricRollupMessage messages
		}

		segment, err := r.store.GetMetricSeg(ctx, lrdb.GetMetricSegParams{
			OrganizationID: msg.OrganizationID,
			Dateint:        msg.DateInt,
			FrequencyMs:    msg.SourceFrequencyMs, // Use source frequency for lookup
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
				slog.Int("sourceFrequencyMs", int(msg.SourceFrequencyMs)),
				slog.Int("instanceNum", int(msg.InstanceNum)),
				slog.Int("slotID", int(msg.SlotID)),
				slog.Int("slotCount", int(msg.SlotCount)),
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

	params := MetricProcessingParams{
		TmpDir:         tmpDir,
		StorageClient:  storageClient,
		OrganizationID: group.Key.OrganizationID,
		StorageProfile: storageProfile,
		ActiveSegments: segments,
		FrequencyMs:    group.Key.TargetFrequencyMs,
		MaxRecords:     recordCountEstimate * 2, // safety net
	}

	result, err := ProcessMetricsWithAggregation(ctx, params)
	if err != nil {
		return err
	}

	results := result.Results

	newSegments, err := r.uploadAndCreateRollupSegments(ctx, storageClient, storageProfile, results, group.Key, segments)
	if err != nil {
		return fmt.Errorf("upload and create rollup segments: %w", err)
	}

	if err := r.atomicDatabaseUpdate(ctx, segments, newSegments, kafkaCommitData, group.Key); err != nil {
		return fmt.Errorf("atomic database update: %w", err)
	}

	var totalRecords, totalSize int64
	for _, result := range results {
		totalRecords += result.RecordCount
		totalSize += result.FileSize
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

	for _, result := range results {
		segmentID := idgen.GenerateID()

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
			IngestDateint:  key.DateInt,           // Use same as dateint for rolled up segments
			FrequencyMs:    key.TargetFrequencyMs, // Store at target frequency
			SegmentID:      segmentID,
			InstanceNum:    key.InstanceNum,
			SlotID:         key.SlotID,    // Preserve slot information
			SlotCount:      key.SlotCount, // Preserve slot information
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
	ReportTelemetry(ctx, "rollup", int64(len(inputSegments)), int64(len(segments)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

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
		SlotID:         key.SlotID,
		SlotCount:      key.SlotCount,
		IngestDateint:  key.DateInt,
		SortVersion:    lrdb.CurrentMetricSortVersion,
	}

	// Prepare Kafka offsets for atomic update
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
			ll.Info("Updating Kafka consumer group offset for rollup",
				slog.String("consumerGroup", kafkaCommitData.ConsumerGroup),
				slog.String("topic", kafkaCommitData.Topic),
				slog.Int("partition", int(partition)),
				slog.Int64("newOffset", offset))
		}
	}

	// Use the atomic rollup function that handles everything in one transaction
	return r.store.RollupMetricSegsWithKafkaOffsetsWithOrg(ctx, sourceParams, targetParams, sourceSegmentIDs, newRecords, kafkaOffsets)
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
