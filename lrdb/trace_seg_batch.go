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

// InsertTraceSegmentsBatch inserts multiple trace segments
func (q *Store) InsertTraceSegmentsBatch(
	ctx context.Context,
	segments []InsertTraceSegmentParams,
) error {
	return q.execTx(ctx, func(s *Store) error {
		// Ensure partitions exist for all segments
		for _, params := range segments {
			if err := s.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					params.OrganizationID, params.Dateint, err)
			}
		}

		// Insert trace segments using existing batch insert
		if len(segments) > 0 {
			batchParams := make([]batchInsertTraceSegsDirectParams, len(segments))
			for i, params := range segments {
				batchParams[i] = batchInsertTraceSegsDirectParams(params)
			}

			result := s.batchInsertTraceSegsDirect(ctx, batchParams)
			var insertErr error
			result.Exec(func(i int, err error) {
				if err != nil && insertErr == nil {
					insertErr = fmt.Errorf("insert trace segment %d: %w", i, err)
				}
			})
			if insertErr != nil {
				return insertErr
			}
		}

		return nil
	})
}

// CompactTraceSegments marks old trace segments as compacted and inserts new compacted segments
func (q *Store) CompactTraceSegments(
	ctx context.Context,
	params CompactTraceSegsParams,
) error {
	return q.execTx(ctx, func(s *Store) error {
		// Mark old segments as compacted if any
		if len(params.OldRecords) > 0 {
			segIDs := make([]int64, len(params.OldRecords))
			for i, oldRec := range params.OldRecords {
				segIDs[i] = oldRec.SegmentID
			}

			if err := s.markTraceSegsCompactedUnpublishedByKeys(ctx, markTraceSegsCompactedUnpublishedByKeysParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				InstanceNum:    params.InstanceNum,
				SegmentIds:     segIDs,
			}); err != nil {
				return fmt.Errorf("mark old trace segments compacted: %w", err)
			}
		}

		// Insert new compacted segments if any
		if len(params.NewRecords) > 0 {
			// Ensure partition exists
			if err := s.ensureTraceFPPartition(ctx, "trace_seg", params.OrganizationID, params.Dateint); err != nil {
				return fmt.Errorf("ensure partition for org %s date %d: %w",
					params.OrganizationID, params.Dateint, err)
			}

			newItems := make([]batchInsertTraceSegsDirectParams, len(params.NewRecords))
			for i, r := range params.NewRecords {
				newItems[i] = batchInsertTraceSegsDirectParams{
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

		return nil
	})
}
