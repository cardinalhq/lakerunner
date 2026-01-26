// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package helpers

import (
	"testing"

	"github.com/google/uuid"
)

func TestMakeDBObjectID(t *testing.T) {
	tests := []struct {
		name          string
		orgID         uuid.UUID
		collectorName string
		dateint       int32
		hour          int16
		segmentID     int64
		ttype         string
		expected      string
	}{
		{
			name:          "standard case",
			orgID:         uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			collectorName: "default",
			dateint:       20240607,
			hour:          15,
			segmentID:     42,
			ttype:         "events",
			expected:      "db/123e4567-e89b-12d3-a456-426614174000/default/20240607/events/15/tbl_42.parquet",
		},
		{
			name:          "single digit hour",
			orgID:         uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			collectorName: "default",
			dateint:       20240607,
			hour:          7,
			segmentID:     1,
			ttype:         "metrics",
			expected:      "db/123e4567-e89b-12d3-a456-426614174000/default/20240607/metrics/07/tbl_1.parquet",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := MakeDBObjectID(tc.orgID, tc.collectorName, tc.dateint, tc.hour, tc.segmentID, tc.ttype)
			if result != tc.expected {
				t.Errorf("MakeDBObjectID() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestMakeAggDBObjectID(t *testing.T) {
	tests := []struct {
		name          string
		orgID         uuid.UUID
		collectorName string
		dateint       int32
		hour          int16
		segmentID     int64
		ttype         string
		expected      string
	}{
		{
			name:          "standard case",
			orgID:         uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			collectorName: "default",
			dateint:       20240607,
			hour:          15,
			segmentID:     42,
			ttype:         "logs",
			expected:      "db/123e4567-e89b-12d3-a456-426614174000/default/20240607/logs/15/agg_42.parquet",
		},
		{
			name:          "single digit hour with zero padding",
			orgID:         uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			collectorName: "default",
			dateint:       20240607,
			hour:          7,
			segmentID:     1,
			ttype:         "logs",
			expected:      "db/123e4567-e89b-12d3-a456-426614174000/default/20240607/logs/07/agg_1.parquet",
		},
		{
			name:          "different collector name",
			orgID:         uuid.MustParse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
			collectorName: "my-collector",
			dateint:       20250101,
			hour:          0,
			segmentID:     999,
			ttype:         "logs",
			expected:      "db/aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee/my-collector/20250101/logs/00/agg_999.parquet",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := MakeAggDBObjectID(tc.orgID, tc.collectorName, tc.dateint, tc.hour, tc.segmentID, tc.ttype)
			if result != tc.expected {
				t.Errorf("MakeAggDBObjectID() = %q, want %q", result, tc.expected)
			}
		})
	}
}
