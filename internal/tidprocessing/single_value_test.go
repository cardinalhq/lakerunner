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

package tidprocessing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMakekey_EmptySketch(t *testing.T) {
	tests := []struct {
		name         string
		rec          map[string]any
		interval     int32
		wantErr      bool
		expectSketch bool
	}{
		{
			name: "EmptySketchWithSingleValue",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1234567890),
				"_cardinalhq.name":      "test_metric",
				"sketch":                []byte{},
				"rollup_count":          float64(1),
				"rollup_sum":            float64(42.5),
			},
			interval:     60,
			wantErr:      false,
			expectSketch: true,
		},
		{
			name: "EmptySketchWithoutRollupData",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1234567890),
				"_cardinalhq.name":      "test_metric",
				"sketch":                []byte{},
			},
			interval:     60,
			wantErr:      false,
			expectSketch: false,
		},
		{
			name: "EmptyStringSketchWithSingleValue",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1234567890),
				"_cardinalhq.name":      "test_metric",
				"sketch":                "",
				"rollup_count":          float64(1),
				"rollup_sum":            float64(42.5),
			},
			interval:     60,
			wantErr:      false,
			expectSketch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, sketchBytes, err := makekey(tc.rec, tc.interval)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, int64(123), key.tid)
				assert.Equal(t, "test_metric", key.name)

				if tc.expectSketch {
					assert.NotEmpty(t, sketchBytes, "Expected non-empty sketch bytes")
					// Try to decode the sketch to make sure it's valid
					sketch, err := DecodeSketch(sketchBytes)
					require.NoError(t, err)
					assert.Equal(t, float64(1), sketch.GetCount())
					assert.InDelta(t, float64(42.5), sketch.GetSum(), 0.1, "Sum should be approximately correct")
				} else {
					assert.Empty(t, sketchBytes, "Expected empty sketch bytes")
				}
			}
		})
	}
}

func TestReconstructSketchFromRollup(t *testing.T) {
	tests := []struct {
		name          string
		rec           map[string]any
		expectSuccess bool
		expectCount   float64
		expectSum     float64
	}{
		{
			name: "SingleValueReconstruction",
			rec: map[string]any{
				"rollup_count": float64(1),
				"rollup_sum":   float64(42.5),
			},
			expectSuccess: true,
			expectCount:   1,
			expectSum:     42.5,
		},
		{
			name: "MultipleValueSkip",
			rec: map[string]any{
				"rollup_count": float64(5),
				"rollup_sum":   float64(42.5),
			},
			expectSuccess: false,
		},
		{
			name: "MissingCount",
			rec: map[string]any{
				"rollup_sum": float64(42.5),
			},
			expectSuccess: false,
		},
		{
			name: "MissingSum",
			rec: map[string]any{
				"rollup_count": float64(1),
			},
			expectSuccess: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sketchBytes, err := reconstructSketchFromRollup(tc.rec)

			if tc.expectSuccess {
				require.NoError(t, err)
				assert.NotEmpty(t, sketchBytes)

				// Decode and verify the reconstructed sketch
				sketch, err := DecodeSketch(sketchBytes)
				require.NoError(t, err)
				assert.Equal(t, tc.expectCount, sketch.GetCount())
				assert.InDelta(t, tc.expectSum, sketch.GetSum(), 0.1, "Sum should be approximately correct")
			} else {
				if err == nil {
					// Should return empty bytes for cases we can't handle
					assert.Empty(t, sketchBytes)
				}
			}
		})
	}
}
