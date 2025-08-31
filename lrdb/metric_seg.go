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

type CompactMetricSegsOld struct {
	SegmentID int64
	SlotID    int32
}

type CompactMetricSegsNew struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

type RollupSourceParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
}

type RollupTargetParams struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
	SlotID         int32
	SlotCount      int32
	IngestDateint  int32
	SortVersion    int16
}

type RollupNewRecord struct {
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	Fingerprints []int64
}

type CompactMetricSegsParams struct {
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
	OldRecords []CompactMetricSegsOld
	// NewRecords contains the segments to be inserted.
	NewRecords []CompactMetricSegsNew
	CreatedBy  CreatedBy
	// SortVersion indicates the sort order of the data in the new segments.
	// 0: Unknown/unsorted, 1: Sorted by [name, tid, timestamp]
	SortVersion int16
}

// RollupMetricSegs marks source segments as rolled up and atomically replaces target segments.
func (q *Store) RollupMetricSegs(ctx context.Context, sourceParams RollupSourceParams, targetParams RollupTargetParams, sourceSegmentIDs []int64, newRecords []RollupNewRecord) error {
	// Ensure partitions exist
	if err := q.ensureMetricSegmentPartition(ctx, targetParams.OrganizationID, targetParams.Dateint); err != nil {
		return fmt.Errorf("ensure partition: %w", err)
	}

	newItems := make([]BatchInsertMetricSegsParams, len(newRecords))
	for i, newRec := range newRecords {
		newItems[i] = BatchInsertMetricSegsParams{
			OrganizationID: targetParams.OrganizationID,
			Dateint:        targetParams.Dateint,
			IngestDateint:  targetParams.IngestDateint,
			FrequencyMs:    targetParams.FrequencyMs,
			SegmentID:      newRec.SegmentID,
			InstanceNum:    targetParams.InstanceNum,
			SlotID:         targetParams.SlotID,
			StartTs:        newRec.StartTs,
			EndTs:          newRec.EndTs,
			RecordCount:    newRec.RecordCount,
			FileSize:       newRec.FileSize,
			Published:      true,
			Rolledup:       false,
			CreatedBy:      CreateByRollup,
			Fingerprints:   newRec.Fingerprints,
			SortVersion:    targetParams.SortVersion,
			SlotCount:      targetParams.SlotCount,
		}
	}

	var errs *multierror.Error
	return q.execTx(ctx, func(s *Store) error {
		// Mark source segments as rolled up (don't delete them)
		if len(sourceSegmentIDs) > 0 {
			if err := s.MarkMetricSegsRolledupByKeys(ctx, MarkMetricSegsRolledupByKeysParams{
				OrganizationID: sourceParams.OrganizationID,
				Dateint:        sourceParams.Dateint,
				FrequencyMs:    sourceParams.FrequencyMs,
				InstanceNum:    sourceParams.InstanceNum,
				SegmentIds:     sourceSegmentIDs,
			}); err != nil {
				return fmt.Errorf("mark source segments as rolled up: %w", err)
			}
		}

		// Insert new target segments
		if len(newItems) > 0 {
			result := s.BatchInsertMetricSegs(ctx, newItems)
			result.Exec(func(i int, err error) {
				if err != nil {
					err = fmt.Errorf("error inserting new target segment %d, keys %v: %w", i, newItems[i], err)
					errs = multierror.Append(errs, err)
				}
			})
		}

		return errs.ErrorOrNil()
	})
}
