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

package cmd

import (
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

func TestShouldCompactMetrics(t *testing.T) {
	tests := []struct {
		name     string
		rows     []lrdb.MetricSeg
		expected bool
	}{
		{
			name:     "less than 2 rows",
			rows:     []lrdb.MetricSeg{{FileSize: 500}},
			expected: false,
		},
		{
			name: "file much larger than 2x target",
			rows: []lrdb.MetricSeg{
				{FileSize: targetFileSize * 3},
				{FileSize: targetFileSize},
			},
			expected: true,
		},
		{
			name: "file much smaller than 30% of target",
			rows: []lrdb.MetricSeg{
				{FileSize: int64(float64(targetFileSize) * 0.2)},
				{FileSize: targetFileSize},
			},
			expected: true,
		},
		{
			name: "estimated file count less than input rows",
			rows: []lrdb.MetricSeg{
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
			},
			expected: true,
		},
		{
			name: "no compaction needed, files are optimal",
			rows: []lrdb.MetricSeg{
				{FileSize: targetFileSize},
				{FileSize: targetFileSize},
			},
			expected: false,
		},
		{
			name: "no compaction, estimated file count equals input rows",
			rows: []lrdb.MetricSeg{
				{FileSize: targetFileSize / 2},
				{FileSize: targetFileSize / 2},
			},
			expected: false,
		},
		{
			name: "all files just above 30% threshold, no compaction",
			rows: []lrdb.MetricSeg{
				{FileSize: int64(float64(targetFileSize) * 0.31)},
				{FileSize: int64(float64(targetFileSize) * 0.32)},
			},
			expected: false,
		},
		{
			name: "all files just below 30% threshold, compaction",
			rows: []lrdb.MetricSeg{
				{FileSize: int64(float64(targetFileSize) * 0.29)},
				{FileSize: int64(float64(targetFileSize) * 0.29)},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldCompactMetrics(tt.rows)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetStartEndTimes(t *testing.T) {
	tests := []struct {
		name      string
		rows      []lrdb.MetricSeg
		wantStart int64
		wantEnd   int64
	}{
		{
			name:      "empty slice",
			rows:      []lrdb.MetricSeg{},
			wantStart: int64(math.MaxInt64),
			wantEnd:   int64(math.MinInt64),
		},
		{
			name: "single row",
			rows: []lrdb.MetricSeg{
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 100, Valid: true}, Upper: pgtype.Int8{Int64: 200, Valid: true}}},
			},
			wantStart: 100,
			wantEnd:   200,
		},
		{
			name: "multiple rows, increasing order",
			rows: []lrdb.MetricSeg{
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 100, Valid: true}, Upper: pgtype.Int8{Int64: 200, Valid: true}}},
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 150, Valid: true}, Upper: pgtype.Int8{Int64: 250, Valid: true}}},
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 120, Valid: true}, Upper: pgtype.Int8{Int64: 220, Valid: true}}},
			},
			wantStart: 100,
			wantEnd:   250,
		},
		{
			name: "multiple rows, decreasing order",
			rows: []lrdb.MetricSeg{
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 300, Valid: true}, Upper: pgtype.Int8{Int64: 400, Valid: true}}},
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 200, Valid: true}, Upper: pgtype.Int8{Int64: 350, Valid: true}}},
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 100, Valid: true}, Upper: pgtype.Int8{Int64: 150, Valid: true}}},
			},
			wantStart: 100,
			wantEnd:   400,
		},
		{
			name: "rows with same start and end",
			rows: []lrdb.MetricSeg{
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 50, Valid: true}, Upper: pgtype.Int8{Int64: 50, Valid: true}}},
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: 50, Valid: true}, Upper: pgtype.Int8{Int64: 50, Valid: true}}},
			},
			wantStart: 50,
			wantEnd:   50,
		},
		{
			name: "rows with negative timestamps",
			rows: []lrdb.MetricSeg{
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: -100, Valid: true}, Upper: pgtype.Int8{Int64: -50, Valid: true}}},
				{TsRange: pgtype.Range[pgtype.Int8]{Lower: pgtype.Int8{Int64: -200, Valid: true}, Upper: pgtype.Int8{Int64: -10, Valid: true}}},
			},
			wantStart: -200,
			wantEnd:   -10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd := getStartEndTimes(tt.rows)
			assert.Equal(t, tt.wantStart, gotStart)
			assert.Equal(t, tt.wantEnd, gotEnd)
		})
	}
}
