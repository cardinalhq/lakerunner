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

	"github.com/google/uuid"
)

// Sort version constants for metric segments
const (
	// MetricSortVersionUnknown indicates the file's sort order is unknown or unsorted (legacy files)
	MetricSortVersionUnknown = 0
	// MetricSortVersionNameTidTimestamp indicates the file is sorted by [metric_name, tid, timestamp] (old TID calculation)
	MetricSortVersionNameTidTimestamp = 1
	// MetricSortVersionNameTidTimestampV2 indicates the file is sorted by [metric_name, tid, timestamp] (new TID calculation)
	MetricSortVersionNameTidTimestampV2 = 2
	// due to a bug, we will move everyone to 3, same key though...
	MetricSortVersionNameTidTimestampV3 = 3
)

// Current metric sort configuration - single source of truth for all metric sorting
const (
	// CurrentMetricSortVersion is the sort version used for all newly created metric segments
	CurrentMetricSortVersion = MetricSortVersionNameTidTimestampV3
)

func (q *Store) InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error {
	if err := q.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.insertMetricSegDirect(ctx, params)
}

type CompactMetricSegsOld struct {
	SegmentID int64
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
	// IngestDateint is the date in YYYYMMDD format when the segments were ingested.
	IngestDateint int32
	// FrequencyMs is the frequency in milliseconds at which the metrics are collected.
	FrequencyMs int32
	// OldRecords contains the segments to be deleted.
	OldRecords []CompactMetricSegsOld
	// NewRecords contains the segments to be inserted.
	NewRecords []CompactMetricSegsNew
	CreatedBy  CreatedBy
}

// InsertMetricSegmentBatchWithKafkaOffsets inserts multiple metric segments and updates multiple Kafka offsets in a single transaction
func (q *Store) InsertMetricSegmentBatchWithKafkaOffsets(ctx context.Context, batch MetricSegmentBatch) error {
	return q.execTx(ctx, func(s *Store) error {
		for _, params := range batch.Segments {
			if err := s.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
				return err
			}
		}

		batchParams := make([]InsertMetricSegsParams, len(batch.Segments))
		for i, params := range batch.Segments {
			batchParams[i] = InsertMetricSegsParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				FrequencyMs:    params.FrequencyMs,
				SegmentID:      params.SegmentID,
				InstanceNum:    params.InstanceNum,
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
			}
		}

		result := s.insertMetricSegsDirect(ctx, batchParams)
		var insertErr error
		result.Exec(func(i int, err error) {
			if err != nil && insertErr == nil {
				insertErr = err
			}
		})
		if insertErr != nil {
			return insertErr
		}

		if len(batch.KafkaOffsets) > 0 {
			SortKafkaOffsets(batch.KafkaOffsets)

			batchOffsetParams := make([]KafkaJournalBatchUpsertParams, len(batch.KafkaOffsets))
			for i, offset := range batch.KafkaOffsets {
				batchOffsetParams[i] = KafkaJournalBatchUpsertParams{
					ConsumerGroup:       offset.ConsumerGroup,
					Topic:               offset.Topic,
					Partition:           offset.Partition,
					LastProcessedOffset: offset.LastProcessedOffset,
					OrganizationID:      offset.OrganizationID,
					InstanceNum:         offset.InstanceNum,
				}
			}

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

// CompactMetricSegsWithKafkaOffsets marks old segments as compacted, inserts new compacted segments,
// and updates Kafka offsets in a single transaction.
// Deadlocks are avoided by sorting Kafka offsets before processing.
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
			// Prepare batch insert parameters
			newItems := make([]InsertMetricSegsParams, len(params.NewRecords))
			for i, r := range params.NewRecords {
				newItems[i] = InsertMetricSegsParams{
					OrganizationID: params.OrganizationID,
					Dateint:        params.Dateint,
					FrequencyMs:    params.FrequencyMs,
					SegmentID:      r.SegmentID,
					InstanceNum:    params.InstanceNum,
					StartTs:        r.StartTs,
					EndTs:          r.EndTs,
					RecordCount:    r.RecordCount,
					FileSize:       r.FileSize,
					Published:      true,
					CreatedBy:      params.CreatedBy,
					Rolledup:       false,
					Fingerprints:   r.Fingerprints,
					SortVersion:    CurrentMetricSortVersion,
					Compacted:      true,
				}
			}

			res := s.insertMetricSegsDirect(ctx, newItems)
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

		// Update Kafka offsets with organization and instance tracking
		if len(kafkaOffsets) > 0 {
			// Sort offsets to prevent deadlocks
			SortKafkaOffsets(kafkaOffsets)

			// Convert to batch parameters - use KafkaJournalUpsertWithOrgInstance for individual org/instance tracking
			for _, offset := range kafkaOffsets {
				if err := s.KafkaJournalUpsertWithOrgInstance(ctx, KafkaJournalUpsertWithOrgInstanceParams{
					Topic:               offset.Topic,
					Partition:           offset.Partition,
					ConsumerGroup:       offset.ConsumerGroup,
					OrganizationID:      offset.OrganizationID,
					InstanceNum:         offset.InstanceNum,
					LastProcessedOffset: offset.LastProcessedOffset,
				}); err != nil {
					return fmt.Errorf("update kafka offset for org %s instance %d: %w", offset.OrganizationID, offset.InstanceNum, err)
				}
			}
		}

		return nil
	})
}

// RollupMetricSegsWithKafkaOffsets marks source segments as rolled up, inserts new rollup segments,
// and updates Kafka offsets in a single transaction
func (q *Store) RollupMetricSegsWithKafkaOffsets(ctx context.Context, sourceParams RollupSourceParams, targetParams RollupTargetParams, sourceSegmentIDs []int64, newRecords []RollupNewRecord, kafkaOffsets []KafkaOffsetUpdate) error {
	return q.execTx(ctx, func(s *Store) error {
		// Mark source segments as rolled up if any
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

		// Insert new rollup segments if any
		if len(newRecords) > 0 {
			// Ensure partition exists
			if err := s.ensureMetricSegmentPartition(ctx, targetParams.OrganizationID, targetParams.Dateint); err != nil {
				return fmt.Errorf("ensure partition: %w", err)
			}

			newItems := make([]InsertMetricSegsParams, len(newRecords))
			for i, newRec := range newRecords {
				newItems[i] = InsertMetricSegsParams{
					CreatedBy:      CreateByRollup,
					Dateint:        targetParams.Dateint,
					EndTs:          newRec.EndTs,
					FileSize:       newRec.FileSize,
					Fingerprints:   newRec.Fingerprints,
					FrequencyMs:    targetParams.FrequencyMs,
					InstanceNum:    targetParams.InstanceNum,
					OrganizationID: targetParams.OrganizationID,
					RecordCount:    newRec.RecordCount,
					Published:      true,
					Compacted:      false,
					Rolledup:       false,
					SegmentID:      newRec.SegmentID,
					SortVersion:    targetParams.SortVersion,
					StartTs:        newRec.StartTs,
				}
			}

			result := s.insertMetricSegsDirect(ctx, newItems)
			var insertErr error
			result.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert rollup segment %d: %w", i, err)
				}
			})
			if insertErr != nil {
				return insertErr
			}
		}

		// Update Kafka offsets with organization and instance tracking
		if len(kafkaOffsets) > 0 {
			// Sort offsets to prevent deadlocks
			SortKafkaOffsets(kafkaOffsets)

			// Update Kafka offsets individually
			for _, offset := range kafkaOffsets {
				if err := s.KafkaJournalUpsertWithOrgInstance(ctx, KafkaJournalUpsertWithOrgInstanceParams{
					Topic:               offset.Topic,
					Partition:           offset.Partition,
					ConsumerGroup:       offset.ConsumerGroup,
					OrganizationID:      offset.OrganizationID,
					InstanceNum:         offset.InstanceNum,
					LastProcessedOffset: offset.LastProcessedOffset,
				}); err != nil {
					return fmt.Errorf("update kafka offset for org %s instance %d: %w", offset.OrganizationID, offset.InstanceNum, err)
				}
			}
		}

		return nil
	})
}
