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

// InsertLogSegmentsBatch inserts multiple log segments and tracks kafka offsets
// using the new kafka_offset_tracker table
func (q *Store) InsertLogSegmentsBatch(
	ctx context.Context,
	segments []InsertLogSegmentParams,
	kafkaOffsets []KafkaOffsetInfo,
) error {
	return q.execTx(ctx, func(s *Store) error {
		// Ensure partitions exist for all segments
		for _, params := range segments {
			if err := s.ensureLogFPPartition(ctx, "log_seg", params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					params.OrganizationID, params.Dateint, err)
			}
		}

		// Insert log segments using existing batch insert
		if len(segments) > 0 {
			batchParams := make([]batchInsertLogSegsDirectParams, len(segments))
			for i, params := range segments {
				batchParams[i] = batchInsertLogSegsDirectParams(params)
			}

			result := s.batchInsertLogSegsDirect(ctx, batchParams)
			var insertErr error
			result.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert log segment %d: %w", i, err)
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

// CompactLogSegments marks old log segments as compacted, inserts new compacted segments,
// and tracks kafka offsets using the new kafka_offset_tracker table
func (q *Store) CompactLogSegments(
	ctx context.Context,
	params CompactLogSegsParams,
	kafkaOffsets []KafkaOffsetInfo,
) error {
	return q.execTx(ctx, func(s *Store) error {
		// Mark old segments as compacted if any
		if len(params.OldRecords) > 0 {
			segIDs := make([]int64, len(params.OldRecords))
			for i, oldRec := range params.OldRecords {
				segIDs[i] = oldRec.SegmentID
			}

			if err := s.MarkLogSegsCompactedByKeys(ctx, MarkLogSegsCompactedByKeysParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				InstanceNum:    params.InstanceNum,
				SegmentIds:     segIDs,
			}); err != nil {
				return fmt.Errorf("mark old log segments compacted: %w", err)
			}
		}

		// Insert new compacted segments if any
		if len(params.NewRecords) > 0 {
			// Ensure partition exists
			if err := s.ensureLogFPPartition(ctx, "log_seg", params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					params.OrganizationID, params.Dateint, err)
			}

			newItems := make([]batchInsertLogSegsDirectParams, len(params.NewRecords))
			for i, r := range params.NewRecords {
				newItems[i] = batchInsertLogSegsDirectParams{
					OrganizationID: params.OrganizationID,
					Dateint:        params.Dateint,
					SegmentID:      r.SegmentID,
					InstanceNum:    params.InstanceNum,
					StartTs:        r.StartTs,
					EndTs:          r.EndTs,
					RecordCount:    r.RecordCount,
					FileSize:       r.FileSize,
					CreatedBy:      params.CreatedBy,
					Fingerprints:   r.Fingerprints,
					Published:      true,
					Compacted:      true,
				}
			}

			res := s.batchInsertLogSegsDirect(ctx, newItems)
			var insertErr error
			res.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert compacted log segment %d: %w", i, err)
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
