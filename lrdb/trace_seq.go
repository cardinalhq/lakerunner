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
	"sort"
)

func (q *Store) InsertTraceSegment(ctx context.Context, params InsertTraceSegmentDirectParams) error {
	if err := q.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.InsertTraceSegmentDirect(ctx, params)
}

// InsertTraceSegmentBatchWithKafkaOffsets inserts multiple trace segments and updates multiple Kafka offsets in a single transaction
func (q *Store) InsertTraceSegmentBatchWithKafkaOffsets(ctx context.Context, batch TraceSegmentBatch) error {
	return q.execTx(ctx, func(s *Store) error {
		// Ensure partitions exist for all segments
		for _, params := range batch.Segments {
			if err := s.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
				return err
			}
		}

		// Convert to batch parameters
		batchParams := make([]BatchInsertTraceSegsParams, len(batch.Segments))
		for i, params := range batch.Segments {
			batchParams[i] = BatchInsertTraceSegsParams(params)
		}

		// Insert all segments using batch
		result := s.BatchInsertTraceSegs(ctx, batchParams)
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
