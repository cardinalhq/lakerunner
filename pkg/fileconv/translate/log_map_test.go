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

package translate

import (
	"testing"
	"time"
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

func TestNormalizeEpochNumber(t *testing.T) {
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
			input:  1_725_000_000, // a plausible 2025 era seconds timestamp
			want:   1_725_000_000 * int64(time.Second),
			wantOK: true,
		},
		{
			name:   "milliseconds typical",
			input:  1_725_000_000_123, // ms
			want:   1_725_000_000_123 * int64(time.Millisecond),
			wantOK: true,
		},
		{
			name:   "microseconds typical",
			input:  1_725_000_000_123_456, // µs
			want:   1_725_000_000_123_456 * int64(time.Microsecond),
			wantOK: true,
		},
		{
			name:   "nanoseconds typical",
			input:  1_725_000_000_123_456_789, // ns
			want:   1_725_000_000_123_456_789,
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
			want:   (secondsUpper - 1) * int64(time.Second),
			wantOK: true,
		},
		{
			name:   "at seconds threshold -> milliseconds domain",
			input:  secondsUpper,
			want:   secondsUpper * int64(time.Millisecond),
			wantOK: true,
		},
		{
			name:   "just below milliseconds upper bound",
			input:  millisecondsUpper - 1,
			want:   (millisecondsUpper - 1) * int64(time.Millisecond),
			wantOK: true,
		},
		{
			name:   "at milliseconds threshold -> microseconds domain",
			input:  millisecondsUpper,
			want:   millisecondsUpper * int64(time.Microsecond),
			wantOK: true,
		},
		{
			name:   "just below microseconds upper bound",
			input:  microsecondsUpper - 1,
			want:   (microsecondsUpper - 1) * int64(time.Microsecond),
			wantOK: true,
		},
		{
			name:   "at microseconds threshold -> nanoseconds domain",
			input:  microsecondsUpper,
			want:   microsecondsUpper,
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := normalizeEpochNumber(tt.input)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("normalizeEpochNumber(%d) = (%d,%v) want (%d,%v)",
					tt.input, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestCoerceToUnixNanos(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	nowNano := now.UnixNano()
	nowPtr := &now

	type tc struct {
		name  string
		input any
		want  int64
		ok    bool
	}
	tests := []tc{
		{
			name:  "time.Time",
			input: now,
			want:  nowNano,
			ok:    true,
		},
		{
			name:  "*time.Time",
			input: nowPtr,
			want:  nowNano,
			ok:    true,
		},
		{
			name:  "nil *time.Time",
			input: (*time.Time)(nil),
			want:  0,
			ok:    false,
		},
		{
			name:  "int64 seconds",
			input: int64(1_725_000_000),
			want:  1_725_000_000 * int64(time.Second),
			ok:    true,
		},
		{
			name:  "int milliseconds",
			input: int(1_725_000_000_123),
			want:  1_725_000_000_123 * int64(time.Millisecond),
			ok:    true,
		},
		{
			name:  "uint64 microseconds",
			input: uint64(1_725_000_000_123_456),
			want:  1_725_000_000_123_456 * int64(time.Microsecond),
			ok:    true,
		},
		{
			name:  "uint nanoseconds",
			input: uint(1_725_000_000_123_456_789),
			want:  1_725_000_000_123_456_789,
			ok:    true,
		},
		{
			name:  "float64 seconds",
			input: float64(1_725_000_000),
			want:  1_725_000_000 * int64(time.Second),
			ok:    true,
		},
		{
			name:  "string seconds",
			input: "1725000000",
			want:  1_725_000_000 * int64(time.Second),
			ok:    true,
		},
		{
			name:  "string milliseconds",
			input: "1725000000123",
			want:  1_725_000_000_123 * int64(time.Millisecond),
			ok:    true,
		},
		{
			name:  "string microseconds",
			input: "1725000000123456",
			want:  1_725_000_000_123_456 * int64(time.Microsecond),
			ok:    true,
		},
		{
			name:  "string nanoseconds",
			input: "1725000000123456789",
			want:  1_725_000_000_123_456_789,
			ok:    true,
		},
		{
			name:  "string RFC3339Nano",
			input: now.Format(time.RFC3339Nano),
			want:  nowNano,
			ok:    true,
		},
		{
			name:  "string with spaces",
			input: " 1725000000 ",
			want:  1_725_000_000 * int64(time.Second),
			ok:    true,
		},
		{
			name:  "empty string",
			input: "",
			want:  0,
			ok:    false,
		},
		{
			name:  "invalid string",
			input: "not-a-timestamp",
			want:  0,
			ok:    false,
		},
		{
			name:  "negative int64",
			input: int64(-1),
			want:  0,
			ok:    false,
		},
		{
			name:  "unsupported type",
			input: struct{}{},
			want:  0,
			ok:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := coerceToUnixNanos(tt.input, time.RFC3339Nano)
			if got != tt.want || ok != tt.ok {
				t.Errorf("coerceToUnixNanos(%#v) = (%d,%v), want (%d,%v)", tt.input, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestTranslateLogRow(t *testing.T) {
	now := time.Date(2024, 6, 1, 12, 34, 56, 789000000, time.UTC)
	nowNano := now.UnixNano()

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
