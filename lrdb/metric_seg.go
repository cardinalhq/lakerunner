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
	"github.com/hashicorp/go-multierror"
)

// Sort version constants for metric segments
const (
	// SortVersionUnknown indicates the file's sort order is unknown or unsorted (legacy files)
	SortVersionUnknown = 0
	// SortVersionNameTidTimestamp indicates the file is sorted by [metric_name, tid, timestamp]
	SortVersionNameTidTimestamp = 1
)

// Current metric sort configuration - single source of truth for all metric sorting
const (
	// CurrentMetricSortVersion is the sort version used for all newly created metric segments
	CurrentMetricSortVersion = SortVersionNameTidTimestamp
)

func (q *Store) InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error {
	if err := q.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.InsertMetricSegmentDirect(ctx, params)
}

type ReplaceMetricSegsOld struct {
	SegmentID int64
	SlotID    int32
}

type ReplaceMetricSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

type ReplaceMetricSegsParams struct {
	// OrganizationID is the ID of the organization to which the metric segments belong.
	OrganizationID uuid.UUID
	// Dateint is the date in YYYYMMDD format for which the metric segments are being replaced.
	Dateint int32
	// InstanceNum is the instance number for which the segments are being replaced.
	InstanceNum int16
	// SlotID is the slot identifier for partitioning.
	SlotID int32
	// SlotCount is the number of slots.
	SlotCount int32
	// IngestDateint is the date in YYYYMMDD format when the segments were ingested.
	IngestDateint int32
	// FrequencyMs is the frequency in milliseconds at which the metrics are collected.
	FrequencyMs int32
	// Published indicates whether the new segments are marked as published.
	Published bool
	// Rolledup indicates whether the new segments are marked as rolledup.
	Rolledup bool
	// OldRecords contains the segments to be deleted.
	OldRecords []ReplaceMetricSegsOld
	// NewRecords contains the segments to be inserted.
	NewRecords []ReplaceMetricSegsNew
	CreatedBy  CreatedBy
	// SortVersion indicates the sort order of the data in the new segments.
	// 0: Unknown/unsorted, 1: Sorted by [name, tid, timestamp]
	SortVersion int16
}

// ReplaceMetricSegs replaces old metric segments with new ones for a given organization, date, and instance.
// The change is made atomically.
func (q *Store) ReplaceMetricSegs(ctx context.Context, args ReplaceMetricSegsParams) error {
	oldItems := make([]BatchDeleteMetricSegsParams, len(args.OldRecords))
	for i, oldRec := range args.OldRecords {
		oldItems[i] = BatchDeleteMetricSegsParams{
			OrganizationID: args.OrganizationID,
			Dateint:        args.Dateint,
			FrequencyMs:    args.FrequencyMs,
			SegmentID:      oldRec.SegmentID,
			InstanceNum:    args.InstanceNum,
			SlotID:         oldRec.SlotID,
		}
	}

	newItems := make([]BatchInsertMetricSegsParams, len(args.NewRecords))
	for i, newRec := range args.NewRecords {
		newItems[i] = BatchInsertMetricSegsParams{
			OrganizationID: args.OrganizationID,
			Dateint:        args.Dateint,
			IngestDateint:  args.IngestDateint,
			FrequencyMs:    args.FrequencyMs,
			SegmentID:      newRec.SegmentID,
			InstanceNum:    args.InstanceNum,
			SlotID:         args.SlotID,
			StartTs:        newRec.StartTs,
			EndTs:          newRec.EndTs,
			RecordCount:    newRec.RecordCount,
			FileSize:       newRec.FileSize,
			Published:      args.Published,
			Rolledup:       args.Rolledup,
			CreatedBy:      args.CreatedBy,
			Fingerprints:   newRec.Fingerprints,
			SortVersion:    args.SortVersion,
		}
	}

	var errs *multierror.Error
	return q.execTx(ctx, func(s *Store) error {
		if len(oldItems) > 0 {
			result := s.BatchDeleteMetricSegs(ctx, oldItems)
			result.Exec(func(i int, err error) {
				if err != nil {
					err = fmt.Errorf("error deleting old metric segment %d, keys %v: %w", i, oldItems[i], err)
					errs = multierror.Append(errs, err)
				}
			})
		}

		if errs.ErrorOrNil() == nil && len(newItems) > 0 {
			result := s.BatchInsertMetricSegs(ctx, newItems)
			result.Exec(func(i int, err error) {
				if err != nil {
					err = fmt.Errorf("error inserting new metric segment %d, keys %v: %w", i, newItems[i], err)
					errs = multierror.Append(errs, err)
				}
			})
		}

		return errs.ErrorOrNil()
	})
}
