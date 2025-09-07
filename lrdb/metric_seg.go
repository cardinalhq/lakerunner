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

package lrdb

import (
	"context"
	"fmt"
	"sort"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
)

// Sort version constants for metric segments
const (
	// SortVersionUnknown indicates the file's sort order is unknown or unsorted (legacy files)
	SortVersionUnknown = 0
	// SortVersionNameTidTimestamp indicates the file is sorted by [metric_name, tid, timestamp] (old TID calculation)
	SortVersionNameTidTimestamp = 1
	// SortVersionNameTidTimestampV2 indicates the file is sorted by [metric_name, tid, timestamp] (new TID calculation)
	SortVersionNameTidTimestampV2 = 2
)

// Current metric sort configuration - single source of truth for all metric sorting
const (
	// CurrentMetricSortVersion is the sort version used for all newly created metric segments
	CurrentMetricSortVersion = SortVersionNameTidTimestampV2
)

func (q *Store) InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error {
	if err := q.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.InsertMetricSegmentDirect(ctx, params)
}

type CompactMetricSegsOld struct {
	SegmentID int64
	SlotID    int32
}

type CompactMetricSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

type RollupSourceParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
}

type RollupTargetParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
	SlotID         int32
	SlotCount      int32
	IngestDateint  int32
	SortVersion    int16
}

type RollupNewRecord struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

type CompactMetricSegsParams struct {
	// OrganizationID is the ID of the organization to which the metric segments belong.
	OrganizationID uuid.UUID
	// Dateint is the date in YYYYMMDD format for which the metric segments are being replaced.
	Dateint int32
	// InstanceNum is the instance number for which the segments are being replaced.
	InstanceNum int16
	// SlotID is the slot identifier for partitioning.
	SlotID int32
	// SlotCount is the number of slots.
	SlotCount int32
	// IngestDateint is the date in YYYYMMDD format when the segments were ingested.
	IngestDateint int32
	// FrequencyMs is the frequency in milliseconds at which the metrics are collected.
	FrequencyMs int32
	// OldRecords contains the segments to be deleted.
	OldRecords []CompactMetricSegsOld
	// NewRecords contains the segments to be inserted.
	NewRecords []CompactMetricSegsNew
	CreatedBy  CreatedBy
	// SortVersion indicates the sort order of the data in the new segments.
	// 0: Unknown/unsorted, 1: Sorted by [name, tid, timestamp]
	SortVersion int16
}

// RollupMetricSegs marks source segments as rolled up and atomically replaces target segments.
func (q *Store) RollupMetricSegs(ctx context.Context, sourceParams RollupSourceParams, targetParams RollupTargetParams, sourceSegmentIDs []int64, newRecords []RollupNewRecord) error {
	// Ensure partitions exist
	if err := q.ensureMetricSegmentPartition(ctx, targetParams.OrganizationID, targetParams.Dateint); err != nil {
		return fmt.Errorf("ensure partition: %w", err)
	}

	newItems := make([]BatchInsertMetricSegsParams, len(newRecords))
	for i, newRec := range newRecords {
		newItems[i] = BatchInsertMetricSegsParams{
			CreatedBy:      CreateByRollup,
			Dateint:        targetParams.Dateint,
			EndTs:          newRec.EndTs,
			FileSize:       newRec.FileSize,
			Fingerprints:   newRec.Fingerprints,
			FrequencyMs:    targetParams.FrequencyMs,
			IngestDateint:  targetParams.IngestDateint,
			InstanceNum:    targetParams.InstanceNum,
			OrganizationID: targetParams.OrganizationID,
			RecordCount:    newRec.RecordCount,
			Published:      true,
			Compacted:      true,
			Rolledup:       false,
			SegmentID:      newRec.SegmentID,
			SlotCount:      targetParams.SlotCount,
			SlotID:         targetParams.SlotID,
			SortVersion:    targetParams.SortVersion,
			StartTs:        newRec.StartTs,
		}
	}

	var errs *multierror.Error
	return q.execTx(ctx, func(s *Store) error {
		if len(sourceSegmentIDs) > 0 {
			if err := s.MarkMetricSegsRolledupByKeys(ctx, MarkMetricSegsRolledupByKeysParams{
				OrganizationID: sourceParams.OrganizationID,
				Dateint:        sourceParams.Dateint,
				FrequencyMs:    sourceParams.FrequencyMs,
				InstanceNum:    sourceParams.InstanceNum,
				SegmentIds:     sourceSegmentIDs,
			}); err != nil {
				return fmt.Errorf("mark source segments as rolled up: %w", err)
			}
		}

		if len(newItems) > 0 {
			result := s.BatchInsertMetricSegs(ctx, newItems)
			result.Exec(func(i int, err error) {
				if err != nil {
					err = fmt.Errorf("error inserting new target segment %d, keys %v: %w", i, newItems[i], err)
					errs = multierror.Append(errs, err)
				}
			})
		}

		return errs.ErrorOrNil()
	})
}

// CompactMetricSegsWithKafkaOffsets marks old segments as compacted, inserts new compacted segments, and updates Kafka offsets in a single transaction
func (q *Store) CompactMetricSegsWithKafkaOffsets(ctx context.Context, params CompactMetricSegsParams, kafkaOffsets []KafkaOffsetUpdate) error {
	return q.execTx(ctx, func(s *Store) error {
		// Mark old segments as compacted if any
		if len(params.OldRecords) > 0 {
			segIDs := make([]int64, len(params.OldRecords))
			for i, oldRec := range params.OldRecords {
				segIDs[i] = oldRec.SegmentID
			}

			if err := s.MarkMetricSegsCompactedByKeys(ctx, MarkMetricSegsCompactedByKeysParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				FrequencyMs:    params.FrequencyMs,
				InstanceNum:    params.InstanceNum,
				SegmentIds:     segIDs,
			}); err != nil {
				return fmt.Errorf("mark old segments compacted: %w", err)
			}
		}

		// Insert new compacted segments if any
		if len(params.NewRecords) > 0 {
			// Ensure partition exists
			if err := s.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition: %w", err)
			}

			newItems := make([]InsertCompactedMetricSegParams, len(params.NewRecords))
			for i, r := range params.NewRecords {
				newItems[i] = InsertCompactedMetricSegParams{
					OrganizationID: params.OrganizationID,
					Dateint:        params.Dateint,
					FrequencyMs:    params.FrequencyMs,
					SegmentID:      r.SegmentID,
					InstanceNum:    params.InstanceNum,
					StartTs:        r.StartTs,
					EndTs:          r.EndTs,
					RecordCount:    r.RecordCount,
					FileSize:       r.FileSize,
					EIngestDateint: params.IngestDateint,
					Published:      true,
					Rolledup:       false,
					CreatedBy:      params.CreatedBy,
					SlotID:         params.SlotID,
					Fingerprints:   r.Fingerprints,
					SortVersion:    CurrentMetricSortVersion,
					SlotCount:      params.SlotCount,
				}
			}

			res := s.InsertCompactedMetricSeg(ctx, newItems)
			var insertErr error
			res.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert compacted segment %d: %w", i, err)
				}
			})
			if insertErr != nil {
				return insertErr
			}
		}

		// Update Kafka offsets
		if len(kafkaOffsets) > 0 {
			// Sort offsets to prevent deadlocks
			sort.Slice(kafkaOffsets, func(i, j int) bool {
				a, b := kafkaOffsets[i], kafkaOffsets[j]
				if a.ConsumerGroup != b.ConsumerGroup {
					return a.ConsumerGroup < b.ConsumerGroup
				}
				if a.Topic != b.Topic {
					return a.Topic < b.Topic
				}
				return a.Partition < b.Partition
			})

			// Convert to batch parameters
			batchOffsetParams := make([]KafkaJournalBatchUpsertParams, len(kafkaOffsets))
			for i, offset := range kafkaOffsets {
				batchOffsetParams[i] = KafkaJournalBatchUpsertParams{
					ConsumerGroup:       offset.ConsumerGroup,
					Topic:               offset.Topic,
					Partition:           offset.Partition,
					LastProcessedOffset: offset.Offset,
				}
			}

			// Execute batch upsert
			result := s.KafkaJournalBatchUpsert(ctx, batchOffsetParams)
			var offsetErr error
			result.Exec(func(i int, err error) {
				if err != nil && offsetErr == nil {
					offsetErr = fmt.Errorf("update kafka offset %d: %w", i, err)
				}
			})
			if offsetErr != nil {
				return offsetErr
			}
		}

		return nil
	})
}

// InsertMetricSegmentBatchWithKafkaOffsets inserts multiple metric segments and updates multiple Kafka offsets in a single transaction
func (q *Store) InsertMetricSegmentBatchWithKafkaOffsets(ctx context.Context, batch MetricSegmentBatch) error {
	return q.execTx(ctx, func(s *Store) error {
		// Ensure partitions exist for all segments
		for _, params := range batch.Segments {
			if err := s.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
				return err
			}
		}

		// Convert to batch parameters
		batchParams := make([]BatchInsertMetricSegsParams, len(batch.Segments))
		for i, params := range batch.Segments {
			batchParams[i] = BatchInsertMetricSegsParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				IngestDateint:  params.IngestDateint,
				FrequencyMs:    params.FrequencyMs,
				SegmentID:      params.SegmentID,
				InstanceNum:    params.InstanceNum,
				SlotID:         params.SlotID,
				StartTs:        params.StartTs,
				EndTs:          params.EndTs,
				RecordCount:    params.RecordCount,
				FileSize:       params.FileSize,
				CreatedBy:      params.CreatedBy,
				Published:      params.Published,
				Compacted:      params.Compacted,
				Rolledup:       false,
				Fingerprints:   params.Fingerprints,
				SortVersion:    params.SortVersion,
				SlotCount:      params.SlotCount,
			}
		}

		// Insert all segments using batch
		result := s.BatchInsertMetricSegs(ctx, batchParams)
		var insertErr error
		result.Exec(func(i int, err error) {
			if err != nil && insertErr == nil {
				insertErr = err
			}
		})
		if insertErr != nil {
			return insertErr
		}

		// Update all Kafka offsets using batch operation
		if len(batch.KafkaOffsets) > 0 {
			// Sort offsets to prevent deadlocks - consistent lock acquisition order
			sort.Slice(batch.KafkaOffsets, func(i, j int) bool {
				a, b := batch.KafkaOffsets[i], batch.KafkaOffsets[j]
				if a.ConsumerGroup != b.ConsumerGroup {
					return a.ConsumerGroup < b.ConsumerGroup
				}
				if a.Topic != b.Topic {
					return a.Topic < b.Topic
				}
				return a.Partition < b.Partition
			})

			// Convert to batch parameters
			batchOffsetParams := make([]KafkaJournalBatchUpsertParams, len(batch.KafkaOffsets))
			for i, offset := range batch.KafkaOffsets {
				batchOffsetParams[i] = KafkaJournalBatchUpsertParams{
					ConsumerGroup:       offset.ConsumerGroup,
					Topic:               offset.Topic,
					Partition:           offset.Partition,
					LastProcessedOffset: offset.Offset,
				}
			}

			// Execute batch upsert
			result := s.KafkaJournalBatchUpsert(ctx, batchOffsetParams)
			var offsetErr error
			result.Exec(func(i int, err error) {
				if err != nil && offsetErr == nil {
					offsetErr = err
				}
			})
			if offsetErr != nil {
				return offsetErr
			}
		}

		return nil
	})
}
