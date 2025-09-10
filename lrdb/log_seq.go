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
)

func (q *Store) InsertLogSegment(ctx context.Context, params InsertLogSegmentParams) error {
	if err := q.ensureLogFPPartition(ctx, "log_seg", params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.insertLogSegmentDirect(ctx, params)
}

// InsertLogSegmentBatchWithKafkaOffsets inserts multiple log segments and updates multiple Kafka offsets in a single transaction
func (q *Store) InsertLogSegmentBatchWithKafkaOffsets(ctx context.Context, batch LogSegmentBatch) error {
	return q.execTx(ctx, func(s *Store) error {
		for _, params := range batch.Segments {
			if err := s.ensureLogFPPartition(ctx, "log_seg", params.OrganizationID, params.Dateint); err != nil {
				return err
			}
		}

		batchParams := make([]batchInsertLogSegsDirectParams, len(batch.Segments))
		for i, params := range batch.Segments {
			batchParams[i] = batchInsertLogSegsDirectParams(params)
		}

		result := s.batchInsertLogSegsDirect(ctx, batchParams)
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
			SortKafkaOffsets(batch.KafkaOffsets)

			batchOffsetParams := make([]KafkaJournalBatchUpsertParams, len(batch.KafkaOffsets))
			for i, offset := range batch.KafkaOffsets {
				batchOffsetParams[i] = KafkaJournalBatchUpsertParams{
					ConsumerGroup:       offset.ConsumerGroup,
					Topic:               offset.Topic,
					Partition:           offset.Partition,
					LastProcessedOffset: offset.Offset,
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
