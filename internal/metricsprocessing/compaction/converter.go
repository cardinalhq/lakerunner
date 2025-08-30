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

package compaction

import (
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func ConvertToMetricSegs(rows []lrdb.ClaimMetricCompactionWorkRow) []lrdb.MetricSeg {
	segments := make([]lrdb.MetricSeg, 0, len(rows))

	for _, row := range rows {
		if !row.TsRange.Valid {
			continue
		}

		startMs := row.TsRange.Lower.Time.UnixMilli()
		endMs := row.TsRange.Upper.Time.UnixMilli()
		dateint, _ := helpers.MSToDateintHour(startMs)

		segments = append(segments, lrdb.MetricSeg{
			OrganizationID: row.OrganizationID,
			Dateint:        dateint,
			FrequencyMs:    int32(row.FrequencyMs),
			SegmentID:      row.SegmentID,
			InstanceNum:    row.InstanceNum,
			RecordCount:    row.RecordCount,
			FileSize:       row.RecordCount * 100, // Estimate based on record count
			CreatedAt:      row.QueueTs,
			IngestDateint:  dateint,
			TsRange: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: startMs, Valid: true},
				Upper:     pgtype.Int8{Int64: endMs, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
			Published:    true,  // Assume published if in compaction queue
			Rolledup:     false, // Not yet rolled up
			TidPartition: 0,     // Default
			TidCount:     0,     // Default
			CreatedBy:    lrdb.CreatedByIngest,
			SlotID:       0,         // Default
			Fingerprints: []int64{}, // Empty
			SortVersion:  lrdb.CurrentMetricSortVersion,
		})
	}

	return segments
}
