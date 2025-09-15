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

// InsertMetricSegmentBatch inserts multiple metric segments and tracks kafka offsets
// using the new kafka_offset_tracker table
func (q *Store) InsertMetricSegmentBatch(
	ctx context.Context,
	segments []InsertMetricSegmentParams,
	kafkaOffsets []KafkaOffsetInfo,
) error {
	return q.execTx(ctx, func(s *Store) error {
		// Ensure partitions exist for all segments
		for _, params := range segments {
			if err := s.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					params.OrganizationID, params.Dateint, err)
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