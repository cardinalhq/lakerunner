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

// CompactLogSegsParams defines the parameters for log segment compaction with Kafka offsets
type CompactLogSegsParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	IngestDateint  int32
	InstanceNum    int16
	NewRecords     []CompactLogSegsNew
	OldRecords     []CompactLogSegsOld
	CreatedBy      CreatedBy
}

// CompactLogSegsOld represents an old log segment to be marked as compacted
type CompactLogSegsOld struct {
	SegmentID int64
}

// CompactLogSegsNew represents a new compacted log segment to be inserted
type CompactLogSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

// CompactLogSegsWithKafkaOffsets marks old log segments as compacted, inserts new compacted segments,
// and updates Kafka offsets in a single transaction
func (q *Store) CompactLogSegsWithKafkaOffsets(ctx context.Context, params CompactLogSegsParams, kafkaOffsets []KafkaOffsetUpdate) error {
	return q.execTx(ctx, func(s *Store) error {
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

		if len(params.NewRecords) > 0 {
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
