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

// CompactTraceSegsParams defines the parameters for trace segment compaction with Kafka offsets
type CompactTraceSegsParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	IngestDateint  int32
	InstanceNum    int16
	NewRecords     []CompactTraceSegsNew
	OldRecords     []CompactTraceSegsOld
	CreatedBy      CreatedBy
}

// CompactTraceSegsOld represents an old trace segment to be marked as compacted
type CompactTraceSegsOld struct {
	SegmentID int64
}

// CompactTraceSegsNew represents a new compacted trace segment to be inserted
type CompactTraceSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

// CompactTraceSegsWithKafkaOffsets marks old trace segments as compacted, inserts new compacted segments,
// and updates Kafka offsets in a single transaction
func (q *Store) CompactTraceSegsWithKafkaOffsets(ctx context.Context, params CompactTraceSegsParams, kafkaOffsets []KafkaOffsetUpdate) error {
	return q.execTx(ctx, func(s *Store) error {
		if len(params.OldRecords) > 0 {
			segIDs := make([]int64, len(params.OldRecords))
			for i, oldRec := range params.OldRecords {
				segIDs[i] = oldRec.SegmentID
			}

			if err := s.MarkTraceSegsCompactedByKeys(ctx, MarkTraceSegsCompactedByKeysParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				InstanceNum:    params.InstanceNum,
				SegmentIds:     segIDs,
			}); err != nil {
				return fmt.Errorf("mark old trace segments compacted: %w", err)
			}
		}

		if len(params.NewRecords) > 0 {
			newItems := make([]batchInsertTraceSegsDirectParams, len(params.NewRecords))
			for i, r := range params.NewRecords {
				newItems[i] = batchInsertTraceSegsDirectParams{
					OrganizationID: params.OrganizationID,
					Dateint:        params.Dateint,
					IngestDateint:  params.IngestDateint,
					SegmentID:      r.SegmentID,
					InstanceNum:    params.InstanceNum,
					StartTs:        r.StartTs,
					EndTs:          r.EndTs,
					RecordCount:    r.RecordCount,
					FileSize:       r.FileSize,
					CreatedBy:      params.CreatedBy,
					Fingerprints:   r.Fingerprints,
				}
			}

			res := s.batchInsertTraceSegsDirect(ctx, newItems)
			var insertErr error
			res.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert compacted trace segment %d: %w", i, err)
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
					LastProcessedOffset: offset.Offset,
				}); err != nil {
					return fmt.Errorf("update kafka offset for org %s instance %d: %w", offset.OrganizationID, offset.InstanceNum, err)
				}
			}
		}

		return nil
	})
}

func (q *Store) InsertTraceSegment(ctx context.Context, params InsertTraceSegmentParams) error {
	if err := q.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.insertTraceSegmentDirect(ctx, params)
}

// InsertTraceSegmentBatchWithKafkaOffsets inserts multiple trace segments and updates multiple Kafka offsets in a single transaction
func (q *Store) InsertTraceSegmentBatchWithKafkaOffsets(ctx context.Context, batch TraceSegmentBatch) error {
	return q.execTx(ctx, func(s *Store) error {
		for _, params := range batch.Segments {
			if err := s.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
				return err
			}
		}

		batchParams := make([]batchInsertTraceSegsDirectParams, len(batch.Segments))
		for i, params := range batch.Segments {
			batchParams[i] = batchInsertTraceSegsDirectParams(params)
		}

		result := s.batchInsertTraceSegsDirect(ctx, batchParams)
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
