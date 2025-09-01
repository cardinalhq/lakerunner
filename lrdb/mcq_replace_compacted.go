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

	"github.com/hashicorp/go-multierror"
)

func (q *Store) CompactMetricSegs(ctx context.Context, args CompactMetricSegsParams) error {
	// Ensure partition exists before any operations
	if err := q.ensureMetricSegmentPartition(ctx, args.OrganizationID, args.Dateint); err != nil {
		return fmt.Errorf("ensure partition: %w", err)
	}

	segIDs := make([]int64, len(args.OldRecords))
	for i, oldRec := range args.OldRecords {
		segIDs[i] = oldRec.SegmentID
	}

	newItems := make([]InsertCompactedMetricSegParams, len(args.NewRecords))
	for i, r := range args.NewRecords {
		newItems[i] = InsertCompactedMetricSegParams{
			OrganizationID: args.OrganizationID,
			Dateint:        args.Dateint,
			FrequencyMs:    args.FrequencyMs,
			SegmentID:      r.SegmentID,
			InstanceNum:    args.InstanceNum,
			StartTs:        r.StartTs,
			EndTs:          r.EndTs,
			RecordCount:    r.RecordCount,
			FileSize:       r.FileSize,
			EIngestDateint: args.IngestDateint,
			Published:      args.Published,
			Rolledup:       args.Rolledup,
			CreatedBy:      args.CreatedBy,
			SlotID:         args.SlotID,
			Fingerprints:   r.Fingerprints,
			SortVersion:    args.SortVersion,
			SlotCount:      args.SlotCount,
		}
	}

	return q.execTx(ctx, func(s *Store) error {
		if len(segIDs) > 0 {
			if err := s.MarkMetricSegsCompactedByKeys(ctx, MarkMetricSegsCompactedByKeysParams{
				OrganizationID: args.OrganizationID,
				Dateint:        args.Dateint,
				FrequencyMs:    args.FrequencyMs,
				InstanceNum:    args.InstanceNum,
				SegmentIds:     segIDs,
			}); err != nil {
				return fmt.Errorf("mark compacted: %w", err)
			}
		}

		if len(newItems) > 0 {
			res := s.InsertCompactedMetricSeg(ctx, newItems)
			var errs *multierror.Error
			res.Exec(func(i int, err error) {
				if err != nil {
					errs = multierror.Append(errs, fmt.Errorf("insert new seg %d (%v): %w", i, newItems[i], err))
				}
			})
			if err := errs.ErrorOrNil(); err != nil {
				return err
			}
		}
		return nil
	})
}
