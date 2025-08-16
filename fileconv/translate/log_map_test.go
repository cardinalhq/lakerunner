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

package translate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToString(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	nowPtr := &now

	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"string", "hello", "hello"},
		{"[]byte", []byte("world"), "world"},
		{"time.Time", now, now.Format(time.RFC3339Nano)},
		{"*time.Time", nowPtr, now.Format(time.RFC3339Nano)},
		{"nil *time.Time", (*time.Time)(nil), ""},
		{"int", int(42), "42"},
		{"int8", int8(-8), "-8"},
		{"int16", int16(1600), "1600"},
		{"int32", int32(-3200), "-3200"},
		{"int64", int64(6400), "6400"},
		{"uint", uint(7), "7"},
		{"uint8", uint8(8), "8"},
		{"uint16", uint16(1600), "1600"},
		{"uint32", uint32(3200), "3200"},
		{"uint64", uint64(6400), "6400"},
		{"float32", float32(3.14), "3.14"},
		{"float64", float64(-2.71828), "-2.71828"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
		{"default", struct{ X int }{X: 1}, "{1}"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toString(tt.input)
			if got != tt.want {
				t.Errorf("toString(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestFlattenValue(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		input  any
		want   map[string]string
	}{
		{
			name:   "simple scalar",
			prefix: "foo",
			input:  123,
			want:   map[string]string{"foo": "123"},
		},
		{
			name:   "map[string]any",
			prefix: "root",
			input: map[string]any{
				"a": "x",
				"b": 42,
			},
			want: map[string]string{
				"root.a": "x",
				"root.b": "42",
			},
		},
		{
			name:   "map[any]any",
			prefix: "yaml",
			input: map[any]any{
				"foo": "bar",
				1:     true,
			},
			want: map[string]string{
				"yaml.foo": "bar",
				"yaml.1":   "true",
			},
		},
		{
			name:   "slice of any",
			prefix: "arr",
			input:  []any{"a", 2, true},
			want: map[string]string{
				"arr[0]": "a",
				"arr[1]": "2",
				"arr[2]": "true",
			},
		},
		{
			name:   "slice of string",
			prefix: "strs",
			input:  []string{"x", "y"},
			want: map[string]string{
				"strs[0]": "x",
				"strs[1]": "y",
			},
		},
		{
			name:   "slice of int",
			prefix: "ints",
			input:  []int{1, 2},
			want: map[string]string{
				"ints[0]": "1",
				"ints[1]": "2",
			},
		},
		{
			name:   "slice of int64",
			prefix: "int64s",
			input:  []int64{10, 20},
			want: map[string]string{
				"int64s[0]": "10",
				"int64s[1]": "20",
			},
		},
		{
			name:   "slice of float64",
			prefix: "floats",
			input:  []float64{1.1, 2.2},
			want: map[string]string{
				"floats[0]": "1.1",
				"floats[1]": "2.2",
			},
		},
		{
			name:   "nested map and slice",
			prefix: "nest",
			input: map[string]any{
				"foo": []any{"bar", map[string]any{"baz": 1}},
			},
			want: map[string]string{
				"nest.foo[0]":     "bar",
				"nest.foo[1].baz": "1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := make(map[string]string)
			flattenValue(tt.prefix, tt.input, out)
			if len(out) != len(tt.want) {
				t.Errorf("flattenValue() got %d keys, want %d", len(out), len(tt.want))
			}
			for k, v := range tt.want {
				if got, ok := out[k]; !ok || got != v {
					t.Errorf("flattenValue() key %q = %q, want %q", k, out[k], v)
				}
			}
		})
	}
}

func TestNormalizeEpochToMillis(t *testing.T) {
	type tc struct {
		name   string
		input  int64
		want   int64
		wantOK bool
	}
	tests := []tc{
		{
			name:   "negative input rejected",
			input:  -1,
			want:   0,
			wantOK: false,
		},
		{
			name:   "seconds typical (now-ish seconds)",
			input:  1_725_000_000,
			want:   1_725_000_000 * 1_000, // seconds → ms
			wantOK: true,
		},
		{
			name:   "milliseconds typical",
			input:  1_725_000_000_123,
			want:   1_725_000_000_123, // ms → ms
			wantOK: true,
		},
		{
			name:   "microseconds typical",
			input:  1_725_000_000_123_456,
			want:   1_725_000_000_123_456 / 1_000, // µs → ms
			wantOK: true,
		},
		{
			name:   "nanoseconds typical",
			input:  1_725_000_000_123_456_789,
			want:   1_725_000_000_123_456_789 / 1_000_000, // ns → ms
			wantOK: true,
		},
		{
			name:   "zero treated as seconds",
			input:  0,
			want:   0,
			wantOK: true,
		},
		{
			name:   "just below seconds upper bound",
			input:  secondsUpper - 1,
			want:   (secondsUpper - 1) * 1_000,
			wantOK: true,
		},
		{
			name:   "at seconds threshold -> ms domain",
			input:  secondsUpper,
			want:   secondsUpper,
			wantOK: true,
		},
		{
			name:   "just below ms upper bound",
			input:  millisecondsUpper - 1,
			want:   millisecondsUpper - 1,
			wantOK: true,
		},
		{
			name:   "at ms threshold -> µs domain",
			input:  millisecondsUpper,
			want:   millisecondsUpper / 1_000,
			wantOK: true,
		},
		{
			name:   "just below µs upper bound",
			input:  microsecondsUpper - 1,
			want:   (microsecondsUpper - 1) / 1_000,
			wantOK: true,
		},
		{
			name:   "at µs threshold -> ns domain",
			input:  microsecondsUpper,
			want:   microsecondsUpper / 1_000_000,
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeEpochToMillis(tt.input)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("normalizeEpochToMillis(%d) = (%d, %v), want (%d, %v)",
					tt.input, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestCoerceToUnixMillis(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	nowMs := now.UnixMilli()
	nowPtr := &now

	type tc struct {
		name   string
		input  any
		want   int64
		wantOK bool
	}
	tests := []tc{
		{"time.Time", now, nowMs, true},
		{"*time.Time", nowPtr, nowMs, true},
		{"nil *time.Time", (*time.Time)(nil), 0, false},
		{"int64 seconds", int64(1_725_000_000), 1_725_000_000 * 1_000, true},
		{"int milliseconds", int(1_725_000_000_123), 1_725_000_000_123, true},
		{"uint64 microseconds", uint64(1_725_000_000_123_456), 1_725_000_000_123_456 / 1_000, true},
		{"uint nanoseconds", uint(1_725_000_000_123_456_789), 1_725_000_000_123_456_789 / 1_000_000, true},
		{"float64 seconds", float64(1_725_000_000), 1_725_000_000 * 1_000, true},
		{"string seconds", "1725000000", 1_725_000_000 * 1_000, true},
		{"string ms", "1725000000123", 1_725_000_000_123, true},
		{"string µs", "1725000000123456", 1_725_000_000_123_456 / 1_000, true},
		{"string ns", "1725000000123456789", 1_725_000_000_123_456_789 / 1_000_000, true},
		{"string RFC3339Nano", now.Format(time.RFC3339Nano), nowMs, true},
		{"string with spaces", " 1725000000 ", 1_725_000_000 * 1_000, true},
		{"empty string", "", 0, false},
		{"invalid string", "not-a-timestamp", 0, false},
		{"negative int64", int64(-1), 0, false},
		{"unsupported type", struct{}{}, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := coerceToUnixMillis(tt.input, time.RFC3339Nano)
			if got != tt.want || ok != tt.wantOK {
				t.Errorf("coerceToUnixMillis(%#v) = (%d, %v), want (%d, %v)",
					tt.input, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestTranslateLogRow(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	nowNano := now.UnixMilli()

	mapper := &Mapper{
		TimestampColumns: []string{"ts", "timestamp"},
		MessageColumns:   []string{"msg", "message"},
		ResourceColumns:  []string{"host", "region"},
		ScopeColumns:     []string{"scope", "env"},
		TimeFormat:       time.RFC3339Nano,
	}

	tests := []struct {
		name   string
		input  map[string]any
		mapper *Mapper
		want   TranslatedLog
	}{
		{
			name: "basic timestamp and message",
			input: map[string]any{
				"ts":  now,
				"msg": "hello world",
			},
			mapper: mapper,
			want: TranslatedLog{
				Timestamp:          nowNano,
				Body:               "hello world",
				ResourceAttributes: map[string]string{},
				ScopeAttributes:    map[string]string{},
				RecordAttributes:   map[string]string{},
			},
		},
		{
			name: "resource and scope attributes",
			input: map[string]any{
				"host":   "server1",
				"region": "us-west",
				"scope":  "api",
				"env":    "prod",
				"msg":    "test",
			},
			mapper: mapper,
			want: TranslatedLog{
				Body: "test",
				ResourceAttributes: map[string]string{
					"host":   "server1",
					"region": "us-west",
				},
				ScopeAttributes: map[string]string{
					"scope": "api",
					"env":   "prod",
				},
				RecordAttributes: map[string]string{},
			},
		},
		{
			name: "nested map and slice flattening",
			input: map[string]any{
				"msg": "nested",
				"host": map[string]any{
					"name": "srv",
					"tags": []string{"a", "b"},
				},
			},
			mapper: mapper,
			want: TranslatedLog{
				Body: "nested",
				ResourceAttributes: map[string]string{
					"host.name":    "srv",
					"host.tags[0]": "a",
					"host.tags[1]": "b",
				},
				ScopeAttributes:  map[string]string{},
				RecordAttributes: map[string]string{},
			},
		},
		{
			name: "timestamp missing, uses now",
			input: map[string]any{
				"msg": "no ts",
			},
			mapper: mapper,
			want: TranslatedLog{
				Body:               "no ts",
				ResourceAttributes: map[string]string{},
				ScopeAttributes:    map[string]string{},
				RecordAttributes:   map[string]string{},
			},
		},
		{
			name: "nil values skipped",
			input: map[string]any{
				"ts":   nil,
				"msg":  nil,
				"host": nil,
			},
			mapper: mapper,
			want: TranslatedLog{
				ResourceAttributes: map[string]string{},
				ScopeAttributes:    map[string]string{},
				RecordAttributes:   map[string]string{},
			},
		},
		// {
		// 	name: "multiple timestamp columns, uses first valid",
		// 	input: map[string]any{
		// 		"timestamp": now,
		// 		"ts":        "not-a-timestamp",
		// 		"msg":       "first valid ts",
		// 	},
		// 	mapper: mapper,
		// 	want: TranslatedLog{
		// 		Timestamp:          nowNano,
		// 		Body:               "first valid ts",
		// 		ResourceAttributes: map[string]string{},
		// 		ScopeAttributes:    map[string]string{},
		// 		RecordAttributes: map[string]string{
		// 			"ts": "not-a-timestamp",
		// 		},
		// 	},
		// },
		// {
		// 	name: "message column first wins",
		// 	input: map[string]any{
		// 		"msg":     "first message",
		// 		"message": "second message",
		// 	},
		// 	mapper: mapper,
		// 	want: TranslatedLog{
		// 		Body:               "first message",
		// 		ResourceAttributes: map[string]string{},
		// 		ScopeAttributes:    map[string]string{},
		// 		RecordAttributes: map[string]string{
		// 			"message": "second message",
		// 		},
		// 	},
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ParseLogRow(tt.mapper, tt.input)
			// Timestamp: only check exact value if input had a timestamp, else check >0
			if ts, ok := tt.input["ts"]; ok && ts != nil {
				if got.Timestamp != tt.want.Timestamp {
					t.Errorf("Timestamp = %d, want %d", got.Timestamp, tt.want.Timestamp)
				}
			} else if got.Timestamp <= 0 {
				t.Errorf("Timestamp not set to now, got %d", got.Timestamp)
			}
			if got.Body != tt.want.Body {
				t.Errorf("Body = %q, want %q", got.Body, tt.want.Body)
			}
			compareMaps := func(got, want map[string]string, label string) {
				if len(got) != len(want) {
					t.Errorf("%s len = %d, want %d", label, len(got), len(want))
				}
				for k, v := range want {
					if got[k] != v {
						t.Errorf("%s[%q] = %q, want %q", label, k, got[k], v)
					}
				}
			}
			compareMaps(got.ResourceAttributes, tt.want.ResourceAttributes, "ResourceAttributes")
			compareMaps(got.ScopeAttributes, tt.want.ScopeAttributes, "ScopeAttributes")
			compareMaps(got.RecordAttributes, tt.want.RecordAttributes, "RecordAttributes")
		})
	}
}

func TestNewMapper(t *testing.T) {
	tests := []struct {
		name     string
		opts     []MapperOption
		expected *Mapper
	}{
		{
			name: "default mapper",
			opts: nil,
			expected: &Mapper{
				TimestampColumns: []string{"timestamp", "time", "ts"},
				MessageColumns:   []string{"message", "msg"},
				ResourceColumns:  nil,
				ScopeColumns:     nil,
				TimeFormat:       time.RFC3339Nano,
			},
		},
		{
			name: "with timestamp column",
			opts: []MapperOption{WithTimestampColumn("date")},
			expected: &Mapper{
				TimestampColumns: []string{"date"},
				MessageColumns:   []string{"message", "msg"},
				ResourceColumns:  nil,
				ScopeColumns:     nil,
				TimeFormat:       time.RFC3339Nano,
			},
		},
		{
			name: "with message column",
			opts: []MapperOption{WithMessageColumn("body")},
			expected: &Mapper{
				TimestampColumns: []string{"timestamp", "time", "ts"},
				MessageColumns:   []string{"body"},
				ResourceColumns:  nil,
				ScopeColumns:     nil,
				TimeFormat:       time.RFC3339Nano,
			},
		},
		{
			name: "with resource columns",
			opts: []MapperOption{WithResourceColumns([]string{"host", "service"})},
			expected: &Mapper{
				TimestampColumns: []string{"timestamp", "time", "ts"},
				MessageColumns:   []string{"message", "msg"},
				ResourceColumns:  []string{"host", "service"},
				ScopeColumns:     nil,
				TimeFormat:       time.RFC3339Nano,
			},
		},
		{
			name: "with scope columns",
			opts: []MapperOption{WithScopeColumn([]string{"env", "team"})},
			expected: &Mapper{
				TimestampColumns: []string{"timestamp", "time", "ts"},
				MessageColumns:   []string{"message", "msg"},
				ResourceColumns:  nil,
				ScopeColumns:     []string{"env", "team"},
				TimeFormat:       time.RFC3339Nano,
			},
		},
		{
			name: "with time format",
			opts: []MapperOption{WithTimeFormat(time.RFC3339)},
			expected: &Mapper{
				TimestampColumns: []string{"timestamp", "time", "ts"},
				MessageColumns:   []string{"message", "msg"},
				ResourceColumns:  nil,
				ScopeColumns:     nil,
				TimeFormat:       time.RFC3339,
			},
		},
		{
			name: "multiple options combined",
			opts: []MapperOption{
				WithTimestampColumn("created_at"),
				WithMessageColumn("log_message"),
				WithResourceColumns([]string{"region", "zone"}),
				WithScopeColumn([]string{"service"}),
				WithTimeFormat("2006-01-02 15:04:05"),
			},
			expected: &Mapper{
				TimestampColumns: []string{"created_at"},
				MessageColumns:   []string{"log_message"},
				ResourceColumns:  []string{"region", "zone"},
				ScopeColumns:     []string{"service"},
				TimeFormat:       "2006-01-02 15:04:05",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := NewMapper(tt.opts...)
			assert.Equal(t, tt.expected.TimestampColumns, mapper.TimestampColumns)
			assert.Equal(t, tt.expected.MessageColumns, mapper.MessageColumns)
			assert.Equal(t, tt.expected.ResourceColumns, mapper.ResourceColumns)
			assert.Equal(t, tt.expected.ScopeColumns, mapper.ScopeColumns)
			assert.Equal(t, tt.expected.TimeFormat, mapper.TimeFormat)
		})
	}
}

func TestWithTimestampColumn(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple column name",
			input:    "timestamp",
			expected: []string{"timestamp"},
		},
		{
			name:     "uppercase converted to lowercase",
			input:    "TIMESTAMP",
			expected: []string{"timestamp"},
		},
		{
			name:     "mixed case converted to lowercase",
			input:    "CreatedAt",
			expected: []string{"createdat"},
		},
		{
			name:     "column with underscores",
			input:    "created_at",
			expected: []string{"created_at"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := NewMapper(WithTimestampColumn(tt.input))
			assert.Equal(t, tt.expected, mapper.TimestampColumns)
		})
	}
}

func TestWithMessageColumn(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "simple column name",
			input:    "message",
			expected: []string{"message"},
		},
		{
			name:     "uppercase converted to lowercase",
			input:    "MESSAGE",
			expected: []string{"message"},
		},
		{
			name:     "mixed case converted to lowercase",
			input:    "LogMessage",
			expected: []string{"logmessage"},
		},
		{
			name:     "column with underscores",
			input:    "log_message",
			expected: []string{"log_message"},
		},
		{
			name:     "empty string",
			input:    "",
			expected: []string{""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := NewMapper(WithMessageColumn(tt.input))
			assert.Equal(t, tt.expected, mapper.MessageColumns)
		})
	}
}

func TestWithResourceColumns(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "single column",
			input:    []string{"host"},
			expected: []string{"host"},
		},
		{
			name:     "multiple columns",
			input:    []string{"host", "service", "region"},
			expected: []string{"host", "service", "region"},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "nil slice",
			input:    nil,
			expected: nil,
		},
		{
			name:     "columns with various formats",
			input:    []string{"host.name", "service_id", "region-zone"},
			expected: []string{"host.name", "service_id", "region-zone"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := NewMapper(WithResourceColumns(tt.input))
			assert.Equal(t, tt.expected, mapper.ResourceColumns)
		})
	}
}

func TestWithScopeColumn(t *testing.T) {
	tests := []struct {
		name     string
		input    []string
		expected []string
	}{
		{
			name:     "single column",
			input:    []string{"scope"},
			expected: []string{"scope"},
		},
		{
			name:     "multiple columns",
			input:    []string{"env", "team", "service"},
			expected: []string{"env", "team", "service"},
		},
		{
			name:     "empty slice",
			input:    []string{},
			expected: []string{},
		},
		{
			name:     "nil slice",
			input:    nil,
			expected: nil,
		},
		{
			name:     "columns with various formats",
			input:    []string{"env.name", "team_id", "service-version"},
			expected: []string{"env.name", "team_id", "service-version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := NewMapper(WithScopeColumn(tt.input))
			assert.Equal(t, tt.expected, mapper.ScopeColumns)
		})
	}
}

func TestWithTimeFormat(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "RFC3339 format",
			input:    time.RFC3339,
			expected: time.RFC3339,
		},
		{
			name:     "RFC3339Nano format",
			input:    time.RFC3339Nano,
			expected: time.RFC3339Nano,
		},
		{
			name:     "custom format",
			input:    "2006-01-02 15:04:05",
			expected: "2006-01-02 15:04:05",
		},
		{
			name:     "epoch format",
			input:    "epoch",
			expected: "epoch",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mapper := NewMapper(WithTimeFormat(tt.input))
			assert.Equal(t, tt.expected, mapper.TimeFormat)
		})
	}
}

func TestExtractJSONFromString(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		prefix string
		want   map[string]string
	}{
		{
			name:   "no JSON content",
			input:  "This is just a plain text message",
			prefix: "message",
			want:   map[string]string{},
		},
		{
			name:   "single JSON object",
			input:  `Processing request {"user": "jason", "action": "login"}`,
			prefix: "message",
			want: map[string]string{
				"message.user":   "jason",
				"message.action": "login",
			},
		},
		{
			name:   "multiple JSON objects",
			input:  `Processing request {"user": "jason", "action": "login"} {"status": "success", "code": 200}`,
			prefix: "message",
			want: map[string]string{
				"message.user":   "jason",
				"message.action": "login",
				"message.status": "success",
				"message.code":   "200",
			},
		},
		{
			name:   "nested JSON structure",
			input:  `User data {"profile": {"name": "Alice", "age": 30, "active": true}}`,
			prefix: "user",
			want: map[string]string{
				"user.profile.name":   "Alice",
				"user.profile.age":    "30",
				"user.profile.active": "true",
			},
		},
		{
			name:   "JSON with arrays",
			input:  `Event data {"tags": ["error", "critical"], "counts": [1, 2, 3]}`,
			prefix: "event",
			want: map[string]string{
				"event.tags[0]":   "error",
				"event.tags[1]":   "critical",
				"event.counts[0]": "1",
				"event.counts[1]": "2",
				"event.counts[2]": "3",
			},
		},
		{
			name:   "malformed JSON should be skipped",
			input:  `Processing {"valid": "json"} and {invalid json} and {"another": "valid"}`,
			prefix: "log",
			want: map[string]string{
				"log.valid":   "json",
				"log.another": "valid",
			},
		},
		{
			name:   "unmatched braces should not cause infinite loop",
			input:  `Processing {"valid": "json"} and {unmatched braces`,
			prefix: "log",
			want: map[string]string{
				"log.valid": "json",
			},
		},
		{
			name:   "empty JSON object",
			input:  `Processing {} and {"data": "value"}`,
			prefix: "log",
			want: map[string]string{
				"log.data": "value",
			},
		},
		{
			name:   "JSON with different data types",
			input:  `Data {"string": "text", "number": 42, "boolean": true, "null": null}`,
			prefix: "data",
			want: map[string]string{
				"data.string":  "text",
				"data.number":  "42",
				"data.boolean": "true",
				"data.null":    "<nil>",
			},
		},
		{
			name:   "custom prefix",
			input:  `{"user": "admin", "role": "superuser"}`,
			prefix: "auth",
			want: map[string]string{
				"auth.user": "admin",
				"auth.role": "superuser",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSONFromString(tt.input, tt.prefix)

			if len(got) != len(tt.want) {
				t.Errorf("extractJSONFromString() returned %d items, want %d", len(got), len(tt.want))
			}

			for key, wantValue := range tt.want {
				if gotValue, exists := got[key]; !exists {
					t.Errorf("extractJSONFromString() missing key %q", key)
				} else if gotValue != wantValue {
					t.Errorf("extractJSONFromString()[%q] = %q, want %q", key, gotValue, wantValue)
				}
			}

			for key := range got {
				if _, exists := tt.want[key]; !exists {
					t.Errorf("extractJSONFromString() returned unexpected key %q with value %q", key, got[key])
				}
			}
		})
	}
}
