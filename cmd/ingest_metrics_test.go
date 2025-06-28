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

	"github.com/stretchr/testify/require"
)

func TestHandleHistogram(t *testing.T) {
	type testCase struct {
		name         string
		bucketCounts []float64
		bucketBounds []float64
		wantCounts   []float64
		wantValues   []float64
	}

	tests := []testCase{
		{
			name:         "simple one bucket",
			bucketCounts: []float64{2},
			bucketBounds: []float64{10},
			wantCounts:   []float64{2},
			wantValues:   []float64{5}, // (1e-10 + 10)/2
		},
		{
			name:         "two buckets, only second has count",
			bucketCounts: []float64{0, 3},
			bucketBounds: []float64{10, 20},
			wantCounts:   []float64{3},
			wantValues:   []float64{15}, // (10 + 20)/2
		},
		{
			name:         "three buckets, last is overflow",
			bucketCounts: []float64{0, 0, 5},
			bucketBounds: []float64{10, 20},
			wantCounts:   []float64{5},
			wantValues:   []float64{21}, // min(20+1, maxTrackableValue)
		},
		{
			name:         "all buckets have counts",
			bucketCounts: []float64{1, 2, 3},
			bucketBounds: []float64{10, 20},
			wantCounts:   []float64{1, 2, 3},
			wantValues:   []float64{5, 15, 21},
		},
		{
			name:         "empty input",
			bucketCounts: []float64{},
			bucketBounds: []float64{},
			wantCounts:   []float64{},
			wantValues:   []float64{},
		},
		{
			name:         "overflow bucket with huge bound",
			bucketCounts: []float64{0, 0, 1},
			bucketBounds: []float64{10, 1e10},
			wantCounts:   []float64{1},
			wantValues:   []float64{1e9}, // capped at maxTrackableValue
		},
		{
			name:         "live data #1",
			bucketCounts: []float64{0, 409, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			bucketBounds: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
			wantCounts:   []float64{409},
			wantValues:   []float64{2.5},
		},
		{
			name:         "live data #2",
			bucketCounts: []float64{0, 299, 60, 7, 1, 2, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0},
			bucketBounds: []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
			wantCounts:   []float64{299, 60, 7, 1, 2, 1, 1},
			wantValues: []float64{
				(0.0 + 5.0) / 2,
				(5.0 + 10.0) / 2,
				(10.0 + 25.0) / 2,
				(25.0 + 50.0) / 2,
				(50.0 + 75.0) / 2,
				(75.0 + 100.0) / 2,
				(100.0 + 250.0) / 2,
			},
		},
	}

	const delta = 1e-8

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotCounts, gotValues := handleHistogram(tc.bucketCounts, tc.bucketBounds)
			require.Equal(t, len(gotCounts), len(gotValues), "counts and values length mismatch")
			require.Equal(t, tc.wantCounts, gotCounts, "counts mismatch")
			require.Equal(t, len(tc.wantValues), len(gotValues), "values length mismatch", gotValues)
			for i := range tc.wantCounts {
				require.InDelta(t, tc.wantValues[i], gotValues[i], delta, "values[%d]", i)
			}
		})
	}
}
