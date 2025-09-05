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
	return q.InsertLogSegmentDirect(ctx, params)
}

// InsertLogSegmentBatchWithKafkaOffset inserts multiple log segments and updates Kafka offset in a single transaction
func (q *Store) InsertLogSegmentBatchWithKafkaOffset(ctx context.Context, batch LogSegmentBatch) error {
	return q.execTx(ctx, func(s *Store) error {
		// Ensure partitions exist for all segments
		for _, params := range batch.Segments {
			if err := s.ensureLogFPPartition(ctx, "log_seg", params.OrganizationID, params.Dateint); err != nil {
				return err
			}
		}

		// Convert to batch parameters
		batchParams := make([]BatchInsertLogSegsParams, len(batch.Segments))
		for i, params := range batch.Segments {
			batchParams[i] = BatchInsertLogSegsParams(params)
		}

		// Insert all segments using batch
		result := s.BatchInsertLogSegs(ctx, batchParams)
		var insertErr error
		result.Exec(func(i int, err error) {
			if err != nil && insertErr == nil {
				insertErr = err
			}
		})
		if insertErr != nil {
			return insertErr
		}

		// Update Kafka offset
		return s.KafkaJournalUpsert(ctx, KafkaJournalUpsertParams{
			ConsumerGroup:       batch.KafkaOffset.ConsumerGroup,
			Topic:               batch.KafkaOffset.Topic,
			Partition:           batch.KafkaOffset.Partition,
			LastProcessedOffset: batch.KafkaOffset.Offset,
		})
	})
}
