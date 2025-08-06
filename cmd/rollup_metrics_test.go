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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoxesForRange(t *testing.T) {
	tests := []struct {
		name        string
		startTs     int64
		endTs       int64
		frequencyMs int32
		want        []int64
	}{
		{
			name:        "single box, exact match",
			startTs:     0,
			endTs:       999,
			frequencyMs: 1000,
			want:        []int64{0},
		},
		{
			name:        "single box, endTs at boundary",
			startTs:     0,
			endTs:       1000,
			frequencyMs: 1000,
			want:        []int64{0, 1},
		},
		{
			name:        "multiple boxes",
			startTs:     0,
			endTs:       2500,
			frequencyMs: 1000,
			want:        []int64{0, 1, 2},
		},
		{
			name:        "start and end in same box",
			startTs:     1000,
			endTs:       1999,
			frequencyMs: 1000,
			want:        []int64{1},
		},
		{
			name:        "start and end in adjacent boxes",
			startTs:     1000,
			endTs:       2000,
			frequencyMs: 1000,
			want:        []int64{1, 2},
		},
		{
			name:        "start and end in far boxes",
			startTs:     500,
			endTs:       3500,
			frequencyMs: 1000,
			want:        []int64{0, 1, 2, 3},
		},
		{
			name:        "zero frequency",
			startTs:     0,
			endTs:       1000,
			frequencyMs: 0,
			want:        []int64{},
		},
		{
			name:        "startTs == endTs",
			startTs:     1000,
			endTs:       1000,
			frequencyMs: 1000,
			want:        []int64{1},
		},
		{
			name:        "endTs < startTs",
			startTs:     2000,
			endTs:       1000,
			frequencyMs: 1000,
			want:        []int64{},
		},
		{
			name:        "real world example",
			startTs:     1749029139999, // Example end timestamp
			endTs:       1749029150000, // Example start timestamp
			frequencyMs: 60_000,        // 1 minute frequency
			want:        []int64{29150485},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := boxesForRange(tt.startTs, tt.endTs, tt.frequencyMs)
			assert.ElementsMatch(t, got, tt.want)
		})
	}
}

func TestCalculateTargetRecordsPerFile(t *testing.T) {
	tests := []struct {
		name                    string
		recordCount             int
		estimatedBytesPerRecord int
		targetFileSize          int64
		want                    int64
	}{
		{
			name:                    "exact fit",
			recordCount:             1000,
			estimatedBytesPerRecord: 100,
			targetFileSize:          100000,
			want:                    1000,
		},
		{
			name:                    "multiple files",
			recordCount:             2000,
			estimatedBytesPerRecord: 100,
			targetFileSize:          100000,
			want:                    1000,
		},
		{
			name:                    "less than one file",
			recordCount:             500,
			estimatedBytesPerRecord: 100,
			targetFileSize:          100000,
			want:                    500,
		},
		{
			name:                    "targetFileSize larger than total size",
			recordCount:             10,
			estimatedBytesPerRecord: 100,
			targetFileSize:          100000,
			want:                    10,
		},
		{
			name:                    "realworld example 1",
			recordCount:             12489*2 + 1,
			estimatedBytesPerRecord: 100,
			targetFileSize:          1000000,
			want:                    8327,
		},
		{
			name:                    "zero records",
			recordCount:             0,
			estimatedBytesPerRecord: 100,
			targetFileSize:          100000,
			want:                    0,
		},
		{
			name:                    "zero estimatedBytesPerRecord",
			recordCount:             1000,
			estimatedBytesPerRecord: 0,
			targetFileSize:          100000,
			want:                    0,
		},
		{
			name:                    "zero targetFileSize (should avoid div by zero)",
			recordCount:             1000,
			estimatedBytesPerRecord: 100,
			targetFileSize:          0,
			want:                    0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calculateTargetRecordsPerFile(tt.recordCount, tt.estimatedBytesPerRecord, tt.targetFileSize)
			assert.Equal(t, tt.want, got)
		})
	}
}
