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
)

// KafkaOffsetInfo represents simplified kafka offset tracking information
type KafkaOffsetInfo struct {
	ConsumerGroup string
	Topic         string
	PartitionID   int32
	BinID         int64
	Offsets       []int64
}

// InsertMetricSegmentsBatch inserts multiple metric segments and tracks kafka offsets
// using the new kafka_offset_tracker table
func (q *Store) InsertMetricSegmentsBatch(
	ctx context.Context,
	segments []InsertMetricSegmentParams,
	kafkaOffsets []KafkaOffsetInfo,
) error {
	return q.execTx(ctx, func(s *Store) error {
		// Group segments by org/dateint to ensure partitions efficiently
		partitions := make(map[string]struct{})
		for _, params := range segments {
			key := fmt.Sprintf("%s:%d", params.OrganizationID, params.Dateint)
			if _, exists := partitions[key]; !exists {
				if err := s.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
					return fmt.Errorf("ensure partition for org %s date %d: %w",
						params.OrganizationID, params.Dateint, err)
				}
				partitions[key] = struct{}{}
			}
		}

		// Insert metric segments using existing batch insert
		if len(segments) > 0 {
			batchParams := make([]InsertMetricSegsParams, len(segments))
			for i, params := range segments {
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
					Rolledup:       params.Rolledup,
					Fingerprints:   params.Fingerprints,
					SortVersion:    params.SortVersion,
				}
			}

			result := s.insertMetricSegsDirect(ctx, batchParams)
			var insertErr error
			result.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert segment %d: %w", i, err)
				}
			})
			if insertErr != nil {
				return insertErr
			}
		}

		// Insert kafka offsets into the new tracker table
		for _, offset := range kafkaOffsets {
			if len(offset.Offsets) == 0 {
				continue // Skip empty offset arrays
			}

			err := s.InsertKafkaOffsets(ctx, InsertKafkaOffsetsParams{
				ConsumerGroup: offset.ConsumerGroup,
				Topic:         offset.Topic,
				PartitionID:   offset.PartitionID,
				BinID:         offset.BinID,
				Offsets:       offset.Offsets,
				CreatedAt:     nil, // Use default (now())
			})
			if err != nil {
				return fmt.Errorf("insert kafka offsets for %s/%s/%d: %w",
					offset.ConsumerGroup, offset.Topic, offset.PartitionID, err)
			}
		}

		return nil
	})
}

// CompactMetricSegments marks old segments as compacted, inserts new compacted segments,
// and tracks kafka offsets using the new kafka_offset_tracker table
func (q *Store) CompactMetricSegments(
	ctx context.Context,
	params CompactMetricSegsParams,
	kafkaOffsets []KafkaOffsetInfo,
) error {
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
			// Ensure partition exists once for all new segments (they all share the same org/dateint)
			if err := s.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					params.OrganizationID, params.Dateint, err)
			}

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

		// Insert kafka offsets into the new tracker table
		for _, offset := range kafkaOffsets {
			if len(offset.Offsets) == 0 {
				continue // Skip empty offset arrays
			}

			err := s.InsertKafkaOffsets(ctx, InsertKafkaOffsetsParams{
				ConsumerGroup: offset.ConsumerGroup,
				Topic:         offset.Topic,
				PartitionID:   offset.PartitionID,
				BinID:         offset.BinID,
				Offsets:       offset.Offsets,
				CreatedAt:     nil, // Use default (now())
			})
			if err != nil {
				return fmt.Errorf("insert kafka offsets for %s/%s/%d: %w",
					offset.ConsumerGroup, offset.Topic, offset.PartitionID, err)
			}
		}

		return nil
	})
}

// RollupMetricSegments marks source segments as rolled up, inserts new rollup segments,
// and tracks kafka offsets using the new kafka_offset_tracker table
func (q *Store) RollupMetricSegments(
	ctx context.Context,
	sourceParams RollupSourceParams,
	targetParams RollupTargetParams,
	sourceSegmentIDs []int64,
	newRecords []RollupNewRecord,
	kafkaOffsets []KafkaOffsetInfo,
) error {
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
			// Ensure partition exists once for all new rollup segments (they all share the same org/dateint)
			if err := s.ensureMetricSegmentPartition(ctx, targetParams.OrganizationID, targetParams.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					targetParams.OrganizationID, targetParams.Dateint, err)
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

		// Insert kafka offsets into the new tracker table
		for _, offset := range kafkaOffsets {
			if len(offset.Offsets) == 0 {
				continue // Skip empty offset arrays
			}

			err := s.InsertKafkaOffsets(ctx, InsertKafkaOffsetsParams{
				ConsumerGroup: offset.ConsumerGroup,
				Topic:         offset.Topic,
				PartitionID:   offset.PartitionID,
				BinID:         offset.BinID,
				Offsets:       offset.Offsets,
				CreatedAt:     nil, // Use default (now())
			})
			if err != nil {
				return fmt.Errorf("insert kafka offsets for %s/%s/%d: %w",
					offset.ConsumerGroup, offset.Topic, offset.PartitionID, err)
			}
		}

		return nil
	})
}
