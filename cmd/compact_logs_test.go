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

package cmd

import (
	"testing"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

func TestFirstFromGroup(t *testing.T) {
	tests := []struct {
		name     string
		group    []lrdb.GetLogSegmentsForCompactionRow
		expected int64
	}{
		{
			name:     "EmptyGroup",
			group:    nil,
			expected: 0,
		},
		{
			name: "SingleElement",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{StartTs: 12345},
			},
			expected: 12345,
		},
		{
			name: "MultipleElements",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{StartTs: 300},
				{StartTs: 100},
				{StartTs: 200},
			},
			expected: 100,
		},
		{
			name: "DuplicateMin",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{StartTs: 50},
				{StartTs: 50},
				{StartTs: 100},
			},
			expected: 50,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := firstFromGroup(tt.group)
			if got != tt.expected {
				t.Errorf("firstFromGroup(%s) = %d; want %d", tt.name, got, tt.expected)
			}
		})
	}
}

func TestLastFromGroup(t *testing.T) {
	tests := []struct {
		name     string
		group    []lrdb.GetLogSegmentsForCompactionRow
		expected int64
	}{
		{
			name:     "EmptyGroup",
			group:    nil,
			expected: 1, // last = 0, so returns 0+1
		},
		{
			name: "SingleElement",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{EndTs: 12345},
			},
			expected: 12346,
		},
		{
			name: "MultipleElements",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{EndTs: 300},
				{EndTs: 100},
				{EndTs: 200},
			},
			expected: 301,
		},
		{
			name: "DuplicateMax",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{EndTs: 50},
				{EndTs: 50},
				{EndTs: 10},
			},
			expected: 51,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := lastFromGroup(tt.group)
			if got != tt.expected {
				t.Errorf("lastFromGroup(%s) = %d; want %d", tt.name, got, tt.expected)
			}
		})
	}
}

func TestIngestDateintFromGroup(t *testing.T) {
	tests := []struct {
		name     string
		group    []lrdb.GetLogSegmentsForCompactionRow
		expected int32
	}{
		{
			name:     "EmptyGroup",
			group:    nil,
			expected: 0,
		},
		{
			name: "SingleElement",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{IngestDateint: 20240601},
			},
			expected: 20240601,
		},
		{
			name: "MultipleElements",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{IngestDateint: 20240601},
				{IngestDateint: 20240603},
				{IngestDateint: 20240602},
			},
			expected: 20240603,
		},
		{
			name: "DuplicateMax",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{IngestDateint: 20240605},
				{IngestDateint: 20240605},
				{IngestDateint: 20240601},
			},
			expected: 20240605,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ingestDateintFromGroup(tt.group)
			if got != tt.expected {
				t.Errorf("ingestDateintFromGroup(%s) = %d; want %d", tt.name, got, tt.expected)
			}
		})
	}
}

func TestSegmentIDsFrom(t *testing.T) {
	tests := []struct {
		name     string
		segments []lrdb.GetLogSegmentsForCompactionRow
		expected []int64
	}{
		{
			name:     "EmptySlice",
			segments: nil,
			expected: []int64{},
		},
		{
			name: "SingleElement",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 42},
			},
			expected: []int64{42},
		},
		{
			name: "MultipleElements",
			segments: []lrdb.GetLogSegmentsForCompactionRow{
				{SegmentID: 1},
				{SegmentID: 2},
				{SegmentID: 3},
			},
			expected: []int64{1, 2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := segmentIDsFrom(tt.segments)
			if len(got) != len(tt.expected) {
				t.Errorf("segmentIDsFrom(%s) length = %d; want %d", tt.name, len(got), len(tt.expected))
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("segmentIDsFrom(%s)[%d] = %d; want %d", tt.name, i, got[i], tt.expected[i])
				}
			}
		})
	}
}
