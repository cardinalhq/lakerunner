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
			collectorName: "testCollector",
			dateint:       20240607,
			hour:          15,
			segmentID:     42,
			ttype:         "events",
			expected:      "db/123e4567-e89b-12d3-a456-426614174000/testCollector/20240607/events/15/tbl_42.parquet",
		},
		{
			name:          "single digit hour",
			orgID:         uuid.MustParse("123e4567-e89b-12d3-a456-426614174000"),
			collectorName: "collector",
			dateint:       20240607,
			hour:          7,
			segmentID:     1,
			ttype:         "metrics",
			expected:      "db/123e4567-e89b-12d3-a456-426614174000/collector/20240607/metrics/07/tbl_1.parquet",
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
