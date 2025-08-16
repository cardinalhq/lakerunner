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

func TestUpdateFromSketch_AllFieldsSet(t *testing.T) {
	sketch, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	err = sketch.Add(2)
	require.NoError(t, err)
	err = sketch.Add(2)
	require.NoError(t, err)
	err = sketch.Add(2)
	require.NoError(t, err)

	row := make(map[string]any)

	acc := &mergeaccumulator{
		row:    row,
		sketch: sketch,
	}

	err = updateFromSketch(acc)
	require.NoError(t, err)

	b, ok := row["sketch"].([]byte)
	require.True(t, ok, "sketch field should be of type []byte")
	assert.NotEmpty(t, b, "sketch field should not be empty")

	expected := map[string]any{
		"rollup_count": float64(3),
		"rollup_sum":   float64(5.980985104252033),
		"rollup_avg":   float64(1.9936617014173443),
		"rollup_max":   float64(1.9936617014173443),
		"rollup_min":   float64(1.9936617014173443),
		"rollup_p25":   float64(1.9936617014173443),
		"rollup_p50":   float64(1.9936617014173443),
		"rollup_p75":   float64(1.9936617014173443),
		"rollup_p90":   float64(1.9936617014173443),
		"rollup_p95":   float64(1.9936617014173443),
		"rollup_p99":   float64(1.9936617014173443),
	}

	for k, v := range expected {
		if k == "rollup_count" {
			assert.Equal(t, v, row[k], "field %s not set correctly", k)
		} else {
			assert.InDelta(t, v, row[k], 0.00000001, "field %s not set correctly", k)
		}
	}
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
