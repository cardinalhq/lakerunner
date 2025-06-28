// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lrdb

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
)

func (q *Store) InsertMetricSegment(ctx context.Context, params InsertMetricSegmentParams) error {
	if err := q.ensureMetricSegmentPartition(ctx, params.OrganizationID, params.Dateint); err != nil {
		return err
	}
	return q.InsertMetricSegmentDirect(ctx, params)
}

type ReplaceMetricSegsOld struct {
	TidPartition int16
	SegmentID    int64
}

type ReplaceMetricSegsNew struct {
	TidPartition int16
	SegmentID    int64
	StartTs      int64
	EndTs        int64
	RecordCount  int64
	FileSize     int64
	TidCount     int32
}

type ReplaceMetricSegsParams struct {
	// OrganizationID is the ID of the organization to which the metric segments belong.
	OrganizationID uuid.UUID
	// Dateint is the date in YYYYMMDD format for which the metric segments are being replaced.
	Dateint int32
	// IngestDateint is the date in YYYYMMDD format when the segments were ingested.
	IngestDateint int32
	// InstanceNum is the collector instance number, gotta keep it separated.
	InstanceNum int16
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
			TidPartition:   oldRec.TidPartition,
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
			TidPartition:   newRec.TidPartition,
			StartTs:        newRec.StartTs,
			EndTs:          newRec.EndTs,
			RecordCount:    newRec.RecordCount,
			FileSize:       newRec.FileSize,
			TidCount:       newRec.TidCount,
			Published:      args.Published,
			Rolledup:       args.Rolledup,
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
