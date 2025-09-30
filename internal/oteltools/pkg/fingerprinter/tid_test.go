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

package fingerprinter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeTags(t *testing.T) {
	tests := []struct {
		name string
		rec  map[string]any
		want map[string]any
	}{
		{
			name: "all valid keys and values",
			rec:  map[string]any{"a": "1", "b": 2, "c": true},
			want: map[string]any{"a": "1", "b": 2, "c": true},
		},
		{
			name: "nil value is skipped",
			rec:  map[string]any{"a": nil, "b": 2},
			want: map[string]any{"b": 2},
		},
		{
			name: "empty key is skipped",
			rec:  map[string]any{"": "skip", "b": 2},
			want: map[string]any{"b": 2},
		},
		{
			name: "empty key and nil value both skipped",
			rec:  map[string]any{"": nil, "a": 1},
			want: map[string]any{"a": 1},
		},
		{
			name: "all skipped",
			rec:  map[string]any{"": nil},
			want: map[string]any{},
		},
		{
			name: "underscore-prefixed keys are skipped",
			rec:  map[string]any{"_private": "skip", "public": "keep", "_internal": 123},
			want: map[string]any{"public": "keep"},
		},
		{
			name: "empty string values are skipped",
			rec:  map[string]any{"empty": "", "valid": "value", "zero": 0},
			want: map[string]any{"valid": "value", "zero": 0},
		},
		{
			name: "combination of filters",
			rec:  map[string]any{"_skip": "private", "": "empty_key", "empty_val": "", "keep": "this"},
			want: map[string]any{"keep": "this"},
		},
		{
			name: "empty input",
			rec:  map[string]any{},
			want: map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MakeTags(tt.rec)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestComputeTID_NewBehavior(t *testing.T) {
	// Test that TID changes when specific fields change
	t.Run("TID changes with _cardinalhq_name", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric2",
			"resource_host":    "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when _cardinalhq_name changes")
	})

	t.Run("TID changes with _cardinalhq_metric_type", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name":        "metric1",
			"_cardinalhq_metric_type": "gauge",
			"resource_host":           "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name":        "metric1",
			"_cardinalhq_metric_type": "counter",
			"resource_host":           "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when _cardinalhq_metric_type changes")
	})

	t.Run("TID changes with resource.* fields", func(t *testing.T) {
		// Test adding a resource field
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
			"resource_region":  "us-east",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when resource field is added")

		// Test changing a resource field value
		tags3 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server2",
		}
		tid3 := ComputeTID(tags3)
		assert.NotEqual(t, tid1, tid3, "TID should change when resource field value changes")

		// Test removing a resource field
		tags4 := map[string]any{
			"_cardinalhq_name": "metric1",
		}
		tid4 := ComputeTID(tags4)
		assert.NotEqual(t, tid1, tid4, "TID should change when resource field is removed")
	})

	t.Run("TID changes with metric.* fields", func(t *testing.T) {
		// Test adding a metric field
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"metric_label1":    "value1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric1",
			"metric_label1":    "value1",
			"metric_label2":    "value2",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when metric field is added")

		// Test changing a metric field value
		tags3 := map[string]any{
			"_cardinalhq_name": "metric1",
			"metric_label1":    "value3",
		}
		tid3 := ComputeTID(tags3)
		assert.NotEqual(t, tid1, tid3, "TID should change when metric field value changes")
	})

	t.Run("TID ignores non-string resource.* and metric.* fields", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
			"resource_count":   123,   // non-string, should be ignored
			"metric_value":     45.67, // non-string, should be ignored
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when non-string resource/metric fields are added")
	})

	t.Run("TID does not change with scope.* fields", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
			"scope_name":       "my-scope",
			"scope_version":    "1.0.0",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when scope fields are added")
	})

	t.Run("TID does not change with arbitrary fields", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
			"alice":            "value",
			"bob":              "another",
			"random_field":     "ignored",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when arbitrary fields are added")
	})

	t.Run("TID ignores other _cardinalhq.* fields", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name":        "metric1",
			"_cardinalhq.metric_type": "gauge",
			"resource_host":           "server1",
		}
		tags2 := map[string]any{
			"_cardinalhq_name":        "metric1",
			"_cardinalhq.metric_type": "gauge",
			"resource_host":           "server1",
			"_cardinalhq_timestamp":   123456789,
			"_cardinalhq.description": "some description",
			"_cardinalhq.unit":        "bytes",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when other _cardinalhq fields are added")
	})

	t.Run("TID is deterministic", func(t *testing.T) {
		tags := map[string]any{
			"_cardinalhq_name":        "metric1",
			"_cardinalhq.metric_type": "gauge",
			"resource_host":           "server1",
			"resource_region":         "us-east",
			"metric_label1":           "value1",
			"metric_label2":           "value2",
		}
		tid1 := ComputeTID(tags)
		tid2 := ComputeTID(tags)
		tid3 := ComputeTID(tags)
		assert.Equal(t, tid1, tid2, "TID should be deterministic")
		assert.Equal(t, tid1, tid3, "TID should be deterministic")
	})

	t.Run("Empty values are filtered", func(t *testing.T) {
		tags1 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
			"resource_region":  "", // empty string should be filtered
		}
		tags2 := map[string]any{
			"_cardinalhq_name": "metric1",
			"resource_host":    "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "Empty string values should be filtered out")
	})
}

// TestComputeTID_Legacy tests for backward compatibility with the old test structure
// The new behavior is comprehensively tested in TestComputeTID_NewBehavior
func TestComputeTID_Legacy(t *testing.T) {
	tests := []struct {
		name       string
		metricName string
		tags       map[string]any
		expectSame bool
		sameTags   map[string]any
	}{
		{
			name:       "basic hash computation with proper fields",
			metricName: "ignored", // first param is ignored now
			tags:       map[string]any{"_cardinalhq_name": "cpu_usage", "resource_host": "server1", "resource_env": "prod"},
			expectSame: false,
		},
		{
			name:       "tag order should not matter",
			metricName: "ignored",
			tags:       map[string]any{"_cardinalhq_name": "memory_usage", "resource_host": "server1", "resource_env": "prod"},
			expectSame: true,
			sameTags:   map[string]any{"_cardinalhq_name": "memory_usage", "resource_env": "prod", "resource_host": "server1"},
		},
		{
			name:       "empty value filtered out",
			metricName: "ignored",
			tags:       map[string]any{"_cardinalhq_name": "disk_usage", "resource_host": "server1", "resource_env": "", "resource_region": "us-east"},
			expectSame: true,
			sameTags:   map[string]any{"_cardinalhq_name": "disk_usage", "resource_host": "server1", "resource_region": "us-east"},
		},
		{
			name:       "underscore-prefixed keys filtered",
			metricName: "ignored",
			tags:       map[string]any{"_cardinalhq_name": "network_io", "resource_host": "server1", "_internal": "skip", "resource_env": "prod"},
			expectSame: true,
			sameTags:   map[string]any{"_cardinalhq_name": "network_io", "resource_host": "server1", "resource_env": "prod"},
		},
		{
			name:       "only string resource/metric fields included",
			metricName: "ignored",
			tags:       map[string]any{"_cardinalhq_name": "metrics", "resource_string": "value", "resource_int": 42, "metric_float": 3.14, "metric_bool": true},
			expectSame: true,
			sameTags:   map[string]any{"_cardinalhq_name": "metrics", "resource_string": "value"}, // non-string values ignored
		},
		{
			name:       "nil tags map",
			metricName: "ignored",
			tags:       nil,
			expectSame: false,
		},
		{
			name:       "empty tags map",
			metricName: "ignored",
			tags:       map[string]any{},
			expectSame: false,
		},
		{
			name:       "all non-compliant tags filtered out",
			metricName: "ignored",
			tags:       map[string]any{"_private": "skip", "empty_val": "", "arbitrary": "ignored"},
			expectSame: true,
			sameTags:   map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tid1 := ComputeTID(tt.tags)

			// TID should be deterministic
			tid2 := ComputeTID(tt.tags)
			assert.Equal(t, tid1, tid2, "TID should be deterministic")

			if tt.expectSame && tt.sameTags != nil {
				tidSame := ComputeTID(tt.sameTags)
				assert.Equal(t, tid1, tidSame, "TIDs should be the same")
			}

		})
	}
}

func TestGetFloat64Value(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]any
		key    string
		want   float64
		wantOk bool
	}{
		{
			name:   "valid float64 value",
			m:      map[string]any{"key": float64(42.5)},
			key:    "key",
			want:   42.5,
			wantOk: true,
		},
		{
			name:   "zero float64 value",
			m:      map[string]any{"key": float64(0)},
			key:    "key",
			want:   0,
			wantOk: true,
		},
		{
			name:   "negative float64 value",
			m:      map[string]any{"key": float64(-123.45)},
			key:    "key",
			want:   -123.45,
			wantOk: true,
		},
		{
			name:   "missing key",
			m:      map[string]any{"other": "value"},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "nil value",
			m:      map[string]any{"key": nil},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "string value",
			m:      map[string]any{"key": "42.5"},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "int value",
			m:      map[string]any{"key": 42},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "bool value",
			m:      map[string]any{"key": true},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "empty map",
			m:      map[string]any{},
			key:    "key",
			want:   0,
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := GetFloat64Value(tt.m, tt.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestGetStringValue(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]any
		key    string
		want   string
		wantOk bool
	}{
		{
			name:   "valid string value",
			m:      map[string]any{"key": "hello world"},
			key:    "key",
			want:   "hello world",
			wantOk: true,
		},
		{
			name:   "empty string value",
			m:      map[string]any{"key": ""},
			key:    "key",
			want:   "",
			wantOk: true,
		},
		{
			name:   "missing key",
			m:      map[string]any{"other": "value"},
			key:    "key",
			want:   "",
			wantOk: false,
		},
		{
			name:   "nil value",
			m:      map[string]any{"key": nil},
			key:    "key",
			want:   "",
			wantOk: false,
		},
		{
			name:   "int value",
			m:      map[string]any{"key": 42},
			key:    "key",
			want:   "",
			wantOk: false,
		},
		{
			name:   "float64 value",
			m:      map[string]any{"key": 42.5},
			key:    "key",
			want:   "",
			wantOk: false,
		},
		{
			name:   "bool value",
			m:      map[string]any{"key": true},
			key:    "key",
			want:   "",
			wantOk: false,
		},
		{
			name:   "empty map",
			m:      map[string]any{},
			key:    "key",
			want:   "",
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := GetStringValue(tt.m, tt.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestGetInt64Value(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]any
		key    string
		want   int64
		wantOk bool
	}{
		{
			name:   "valid int64 value",
			m:      map[string]any{"key": int64(42)},
			key:    "key",
			want:   42,
			wantOk: true,
		},
		{
			name:   "zero int64 value",
			m:      map[string]any{"key": int64(0)},
			key:    "key",
			want:   0,
			wantOk: true,
		},
		{
			name:   "negative int64 value",
			m:      map[string]any{"key": int64(-123)},
			key:    "key",
			want:   -123,
			wantOk: true,
		},
		{
			name:   "missing key",
			m:      map[string]any{"other": "value"},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "nil value",
			m:      map[string]any{"key": nil},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "string value",
			m:      map[string]any{"key": "42"},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "int value (not int64)",
			m:      map[string]any{"key": 42},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "float64 value",
			m:      map[string]any{"key": 42.0},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "bool value",
			m:      map[string]any{"key": true},
			key:    "key",
			want:   0,
			wantOk: false,
		},
		{
			name:   "empty map",
			m:      map[string]any{},
			key:    "key",
			want:   0,
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := GetInt64Value(tt.m, tt.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestGetTIDValue(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]any
		key    string
		want   int64
		wantOk bool
	}{
		{
			name:   "int64 value",
			m:      map[string]any{"_cardinalhq.tid": int64(123)},
			key:    "_cardinalhq.tid",
			want:   123,
			wantOk: true,
		},
		{
			name:   "string value convertible to int64",
			m:      map[string]any{"_cardinalhq.tid": "456"},
			key:    "_cardinalhq.tid",
			want:   456,
			wantOk: true,
		},
		{
			name:   "string value not convertible",
			m:      map[string]any{"_cardinalhq.tid": "not-a-number"},
			key:    "_cardinalhq.tid",
			want:   0,
			wantOk: false,
		},
		{
			name:   "missing key",
			m:      map[string]any{},
			key:    "_cardinalhq.tid",
			want:   0,
			wantOk: false,
		},
		{
			name:   "nil value",
			m:      map[string]any{"_cardinalhq.tid": nil},
			key:    "_cardinalhq.tid",
			want:   0,
			wantOk: false,
		},
		{
			name:   "wrong type",
			m:      map[string]any{"_cardinalhq.tid": 123.45},
			key:    "_cardinalhq.tid",
			want:   0,
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := GetTIDValue(tt.m, tt.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}

func TestGetFloat64SliceJSON(t *testing.T) {
	tests := []struct {
		name   string
		m      map[string]any
		key    string
		want   []float64
		wantOk bool
	}{
		{
			name:   "valid JSON array",
			m:      map[string]any{"key": `[1.0, 2.5, 3.14]`},
			key:    "key",
			want:   []float64{1.0, 2.5, 3.14},
			wantOk: true,
		},
		{
			name:   "empty JSON array",
			m:      map[string]any{"key": `[]`},
			key:    "key",
			want:   []float64{},
			wantOk: true,
		},
		{
			name:   "single element array",
			m:      map[string]any{"key": `[42.5]`},
			key:    "key",
			want:   []float64{42.5},
			wantOk: true,
		},
		{
			name:   "array with zero values",
			m:      map[string]any{"key": `[0, 1.5, 0]`},
			key:    "key",
			want:   []float64{0, 1.5, 0},
			wantOk: true,
		},
		{
			name:   "array with negative values",
			m:      map[string]any{"key": `[-1.5, -2.0]`},
			key:    "key",
			want:   []float64{-1.5, -2.0},
			wantOk: true,
		},
		{
			name:   "missing key",
			m:      map[string]any{"other": "value"},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "nil value",
			m:      map[string]any{"key": nil},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "non-string value",
			m:      map[string]any{"key": []float64{1.0, 2.0}},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "invalid JSON",
			m:      map[string]any{"key": `[1.0, 2.5`},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "JSON with non-numeric values",
			m:      map[string]any{"key": `["1.0", "2.5"]`},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "JSON object instead of array",
			m:      map[string]any{"key": `{"a": 1.0}`},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "empty string",
			m:      map[string]any{"key": ""},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
		{
			name:   "empty map",
			m:      map[string]any{},
			key:    "key",
			want:   nil,
			wantOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gotOk := GetFloat64SliceJSON(tt.m, tt.key)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantOk, gotOk)
		})
	}
}
