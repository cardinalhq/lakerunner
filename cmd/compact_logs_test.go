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

	"github.com/cardinalhq/lakerunner/lrdb"
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
			expected: 0,
		},
		{
			name: "SingleElement",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{EndTs: 12345},
			},
			expected: 12345,
		},
		{
			name: "MultipleElements",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{EndTs: 300},
				{EndTs: 100},
				{EndTs: 200},
			},
			expected: 300,
		},
		{
			name: "DuplicateMax",
			group: []lrdb.GetLogSegmentsForCompactionRow{
				{EndTs: 50},
				{EndTs: 50},
				{EndTs: 10},
			},
			expected: 50,
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
