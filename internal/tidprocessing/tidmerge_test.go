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

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateFromSketch_NilSketch(t *testing.T) {
	// Test that updateFromSketch handles nil sketch gracefully
	acc := &mergeaccumulator{
		row: map[string]any{
			"_cardinalhq.tid":  int64(123),
			"_cardinalhq.name": "test_metric",
		},
		contributions: 2, // Multiple contributions but nil sketch
		sketch:        nil,
	}

	err := updateFromSketch(acc)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "attempting to update from nil sketch")
	assert.Contains(t, err.Error(), "2 contributions")
}

func TestUpdateFromSketch_AllFieldsSet(t *testing.T) {
	// Build a source sketch and compute expected values off it
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	require.NoError(t, sketch.Add(2))
	require.NoError(t, sketch.Add(2))
	require.NoError(t, sketch.Add(2))

	expCount := sketch.GetCount()
	expSum := sketch.GetSum()
	expMax, err := sketch.GetMaxValue()
	require.NoError(t, err)
	expMin, err := sketch.GetMinValue()
	require.NoError(t, err)
	expQs, err := sketch.GetValuesAtQuantiles([]float64{0.25, 0.50, 0.75, 0.90, 0.95, 0.99})
	require.NoError(t, err)

	// Encode and feed bytes only to exercise lazy decoding with Dense store
	enc := EncodeSketch(sketch)

	row := make(map[string]any)
	acc := &mergeaccumulator{
		row: row,
		holder: sketchHolder{
			bytes: enc, // no decoded sketch yet; ensureDecoded() will run inside updateFromSketch
		},
		contributions: 2, // simulate merged path (updateFromSketch typically called when >1)
	}

	err = updateFromSketch(acc)
	require.NoError(t, err)

	// sketch field present and non-empty
	b, ok := row["sketch"].([]byte)
	require.True(t, ok, "sketch field should be of type []byte")
	assert.NotEmpty(t, b, "sketch field should not be empty")

	// Verify rollup fields (allow small deltas for approximate DDSketch math)
	const eps = 1e-6

	assert.Equal(t, expCount, row["rollup_count"])
	assert.InDelta(t, expSum, row["rollup_sum"], eps)
	assert.InDelta(t, expSum/expCount, row["rollup_avg"], eps)
	assert.InDelta(t, expMax, row["rollup_max"], eps)
	assert.InDelta(t, expMin, row["rollup_min"], eps)

	// Quantiles
	assert.InDelta(t, expQs[0], row["rollup_p25"], eps)
	assert.InDelta(t, expQs[1], row["rollup_p50"], eps)
	assert.InDelta(t, expQs[2], row["rollup_p75"], eps)
	assert.InDelta(t, expQs[3], row["rollup_p90"], eps)
	assert.InDelta(t, expQs[4], row["rollup_p95"], eps)
	assert.InDelta(t, expQs[5], row["rollup_p99"], eps)
}

func TestMakekey(t *testing.T) {
	tests := []struct {
		name        string
		rec         map[string]any
		interval    int32
		wantKey     *mergekey
		wantSketch  []byte
		wantErr     bool
		errContains string
	}{
		{
			name: "Success",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1, 2, 3},
			},
			interval:   60,
			wantKey:    &mergekey{tid: 123, timestamp: 1690000020, name: "foo"},
			wantSketch: []byte{1, 2, 3},
			wantErr:    false,
		},
		{
			name: "Success with sketch as string",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      "foo",
				"sketch":                "sketch-as-string",
			},
			interval:   60,
			wantKey:    &mergekey{tid: 123, timestamp: 1690000020, name: "foo"},
			wantSketch: []byte("sketch-as-string"),
			wantErr:    false,
		},
		{
			name: "MissingTID",
			rec: map[string]any{
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1, 2, 3},
			},
			interval:    60,
			wantErr:     true,
			errContains: "record does not contain a valid int64 _cardinalhq.tid",
		},
		{
			name: "InvalidTIDType",
			rec: map[string]any{
				"_cardinalhq.tid":       "not-an-int",
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1, 2, 3},
			},
			interval:    60,
			wantErr:     true,
			errContains: "record does not contain a valid int64 _cardinalhq.tid",
		},
		{
			name: "MissingTimestamp",
			rec: map[string]any{
				"_cardinalhq.tid":  int64(123),
				"_cardinalhq.name": "foo",
				"sketch":           []byte{1, 2, 3},
			},
			interval:    60,
			wantErr:     true,
			errContains: "record does not contain a valid int64 _cardinalhq.timestamp",
		},
		{
			name: "InvalidTimestampType",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": "not-an-int",
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1, 2, 3},
			},
			interval:    60,
			wantErr:     true,
			errContains: "record does not contain a valid int64 _cardinalhq.timestamp",
		},
		{
			name: "MissingName",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1690000061),
				"sketch":                []byte{1, 2, 3},
			},
			interval:    60,
			wantErr:     true,
			errContains: "record does not contain a valid string _cardinalhq.name",
		},
		{
			name: "InvalidNameType",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      123,
				"sketch":                []byte{1, 2, 3},
			},
			interval:    60,
			wantErr:     true,
			errContains: "record does not contain a valid string _cardinalhq.name",
		},
		{
			name: "MissingSketch",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      "foo",
			},
			interval:    60,
			wantErr:     true,
			errContains: ErrorInvalidSketchType.Error(),
		},
		{
			name: "InvalidSketchType",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(123),
				"_cardinalhq.timestamp": int64(1690000061),
				"_cardinalhq.name":      "foo",
				"sketch":                1,
			},
			interval:    60,
			wantErr:     true,
			errContains: ErrorInvalidSketchType.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, sketchBytes, err := makekey(tt.rec, tt.interval)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantKey.tid, key.tid)
				assert.Equal(t, tt.wantKey.timestamp, key.timestamp)
				assert.Equal(t, tt.wantKey.name, key.name)
				assert.Equal(t, tt.wantSketch, sketchBytes)
			}
		})
	}
}

func TestTsInRange(t *testing.T) {
	type args struct {
		ts      int64
		startTS int64
		endTS   int64
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "ts equal to startTS",
			args: args{ts: 100, startTS: 100, endTS: 200},
			want: true,
		},
		{
			name: "ts just below startTS",
			args: args{ts: 99, startTS: 100, endTS: 200},
			want: false,
		},
		{
			name: "ts just above startTS",
			args: args{ts: 101, startTS: 100, endTS: 200},
			want: true,
		},
		{
			name: "ts equal to endTS",
			args: args{ts: 200, startTS: 100, endTS: 200},
			want: false,
		},
		{
			name: "ts just below endTS",
			args: args{ts: 199, startTS: 100, endTS: 200},
			want: true,
		},
		{
			name: "ts between startTS and endTS",
			args: args{ts: 150, startTS: 100, endTS: 200},
			want: true,
		},
		{
			name: "ts less than both startTS and endTS",
			args: args{ts: 50, startTS: 100, endTS: 200},
			want: false,
		},
		{
			name: "ts greater than both startTS and endTS",
			args: args{ts: 250, startTS: 100, endTS: 200},
			want: false,
		},
		{
			name: "startTS equals endTS, ts equals startTS",
			args: args{ts: 100, startTS: 100, endTS: 100},
			want: false,
		},
		{
			name: "startTS equals endTS, ts less than startTS",
			args: args{ts: 99, startTS: 100, endTS: 100},
			want: false,
		},
		{
			name: "startTS equals endTS, ts greater than startTS",
			args: args{ts: 101, startTS: 100, endTS: 100},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tsInRange(tt.args.ts, tt.args.startTS, tt.args.endTS)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGroupTIDGroupFunc(t *testing.T) {
	tests := []struct {
		name    string
		prev    map[string]any
		current map[string]any
		want    bool
	}{
		{
			name: "Equal int64 TIDs",
			prev: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			current: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			want: true,
		},
		{
			name: "Different int64 TIDs",
			prev: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			current: map[string]any{
				"_cardinalhq.tid": int64(43),
			},
			want: false,
		},
		{
			name: "Missing TID in prev",
			prev: map[string]any{
				"foo": int64(1),
			},
			current: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			want: false,
		},
		{
			name: "Missing TID in current",
			prev: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			current: map[string]any{
				"bar": int64(2),
			},
			want: false,
		},
		{
			name: "TID wrong type in prev",
			prev: map[string]any{
				"_cardinalhq.tid": "not-an-int",
			},
			current: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			want: false,
		},
		{
			name: "TID wrong type in current",
			prev: map[string]any{
				"_cardinalhq.tid": int64(42),
			},
			current: map[string]any{
				"_cardinalhq.tid": "not-an-int",
			},
			want: false,
		},
		{
			name:    "Both missing TID",
			prev:    map[string]any{},
			current: map[string]any{},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GroupTIDGroupFunc(tt.prev, tt.current)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMakekey_VariousCases(t *testing.T) {
	tests := []struct {
		name       string
		rec        map[string]any
		interval   int32
		wantKey    mergekey
		wantSketch []byte
		wantErr    bool
		errIs      error
	}{
		{
			name: "valid input with []byte sketch",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(123),
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1, 2, 3},
			},
			interval:   60,
			wantKey:    mergekey{tid: 1, timestamp: 120, name: "foo"},
			wantSketch: []byte{1, 2, 3},
			wantErr:    false,
		},
		{
			name: "valid input with string sketch",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(2),
				"_cardinalhq.timestamp": int64(180),
				"_cardinalhq.name":      "bar",
				"sketch":                "abc",
			},
			interval:   60,
			wantKey:    mergekey{tid: 2, timestamp: 180, name: "bar"},
			wantSketch: []byte("abc"),
			wantErr:    false,
		},
		{
			name: "missing tid",
			rec: map[string]any{
				"_cardinalhq.timestamp": int64(123),
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1},
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidTID,
		},
		{
			name: "tid wrong type",
			rec: map[string]any{
				"_cardinalhq.tid":       "notint",
				"_cardinalhq.timestamp": int64(123),
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1},
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidTID,
		},
		{
			name: "missing timestamp",
			rec: map[string]any{
				"_cardinalhq.tid":  int64(1),
				"_cardinalhq.name": "foo",
				"sketch":           []byte{1},
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidTimestamp,
		},
		{
			name: "timestamp wrong type",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": "notint",
				"_cardinalhq.name":      "foo",
				"sketch":                []byte{1},
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidTimestamp,
		},
		{
			name: "missing name",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(123),
				"sketch":                []byte{1},
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidName,
		},
		{
			name: "name wrong type",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(123),
				"_cardinalhq.name":      123,
				"sketch":                []byte{1},
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidName,
		},
		{
			name: "missing sketch",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(123),
				"_cardinalhq.name":      "foo",
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidSketchType,
		},
		{
			name: "sketch wrong type",
			rec: map[string]any{
				"_cardinalhq.tid":       int64(1),
				"_cardinalhq.timestamp": int64(123),
				"_cardinalhq.name":      "foo",
				"sketch":                123,
			},
			interval: 60,
			wantErr:  true,
			errIs:    ErrorInvalidSketchType,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			key, sketch, err := makekey(tc.rec, tc.interval)
			if tc.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.errIs)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantKey, key)
				assert.Equal(t, tc.wantSketch, sketch)
			}
		})
	}
}
