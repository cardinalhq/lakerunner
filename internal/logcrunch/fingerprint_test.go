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

package logcrunch

import (
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
)

func TestComputeHash(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect int64
	}{
		{
			name:   "empty string",
			input:  "",
			expect: 0,
		},
		{
			name:   "single ascii",
			input:  "x",
			expect: 120,
		},
		{
			name:   "two ascii",
			input:  "hi",
			expect: 3329,
		},
		{
			name:   "three ascii",
			input:  "cat",
			expect: 98262,
		},
		{
			name:   "four ascii",
			input:  "test",
			expect: 3556498,
		},
		{
			name:   "five ascii",
			input:  "tests",
			expect: 110251553,
		},
		{
			name:   "eight ascii",
			input:  "abcdefgh",
			expect: 2758628677764,
		},
		{
			name:   "unicode ascii",
			input:  "a😀b",
			expect: 3003559594,
		},
		{
			name:   "long string",
			input:  "the quick brown fox jumps over the lazy dog",
			expect: 9189841723308291443,
		},
		{
			name:   "ending with sisteen As",
			input:  "aaaaaaaaaaaaaaaa",
			expect: 4664096450436235520,
		},
		{
			name:   "ending with 26 As",
			input:  "aaaaaaaaaaaaaaaaaaaaaaaaaa",
			expect: -3949595362870219360,
		},
		{
			name:   "_cardinalhq.telemetry_type:log",
			input:  "_cardinalhq.telemetry_type:log",
			expect: -2057809196342244688,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeHash(tt.input)
			assert.Equal(t, tt.expect, got, "ComputeHash(%q) = %d; want %d", tt.input, got, tt.expect)
		})
	}
}

func TestToTrigrams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:  "empty string",
			input: "",
			expected: []string{
				ExistsRegex,
			},
		},
		{
			name:  "short string less than 3",
			input: "ab",
			expected: []string{
				ExistsRegex,
			},
		},
		{
			name:  "exactly 3 chars",
			input: "abc",
			expected: []string{
				"abc",
				ExistsRegex,
			},
		},
		{
			name:  "4 chars",
			input: "abcd",
			expected: []string{
				"abc",
				"bcd",
				ExistsRegex,
			},
		},
		{
			name:  "5 chars",
			input: "abcde",
			expected: []string{
				"abc",
				"bcd",
				"cde",
				ExistsRegex,
			},
		},
		{
			name:  "unicode string",
			input: "a😀b",
			expected: []string{
				ExistsRegex,
				"a😀b",
			},
		},
		{
			name:  "repeated chars",
			input: "aaaaa",
			expected: []string{
				"aaa",
				ExistsRegex,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToTrigrams(tt.input)
			assert.ElementsMatch(t, tt.expected, got)
		})
	}
}

func TestToFingerprints(t *testing.T) {
	tests := []struct {
		name            string
		input           map[string]mapset.Set[string]
		expectedFingerp mapset.Set[int64]
	}{
		{
			name:            "empty input",
			input:           map[string]mapset.Set[string]{},
			expectedFingerp: mapset.NewSet[int64](),
		},
		{
			name: "single indexed dimension, single value",
			input: map[string]mapset.Set[string]{
				"_cardinalhq.name": mapset.NewSet("foo"),
			},
			expectedFingerp: mapset.NewSet[int64](
				49634475688306877,
				-4163792710976729371,
			),
		},
		{
			name: "single indexed dimension, multiple values",
			input: map[string]mapset.Set[string]{
				"_cardinalhq.name": mapset.NewSet("foo", "bar"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("_cardinalhq.name", "foo"),
				ComputeFingerprint("_cardinalhq.name", "bar"),
				ComputeFingerprint("_cardinalhq.name", ExistsRegex),
			),
		},
		{
			name: "single unindexed dimension",
			input: map[string]mapset.Set[string]{
				"custom": mapset.NewSet("abc"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("custom", ExistsRegex),
			),
		},
		{
			name: "indexed and unindexed dimensions",
			input: map[string]mapset.Set[string]{
				"_cardinalhq.name": mapset.NewSet("foo"),
				"custom":           mapset.NewSet("abc"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("_cardinalhq.name", "foo"),
				ComputeFingerprint("_cardinalhq.name", ExistsRegex),
				ComputeFingerprint("custom", ExistsRegex),
			),
		},
		{
			name: "multiple values, mixed dimensions",
			input: map[string]mapset.Set[string]{
				"_cardinalhq.name": mapset.NewSet("foo", "bar"),
				"resource.file":    mapset.NewSet("file1", "file2"),
				"custom":           mapset.NewSet("baz"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("_cardinalhq.name", "foo"),
				ComputeFingerprint("_cardinalhq.name", "bar"),
				ComputeFingerprint("_cardinalhq.name", ExistsRegex),
				ComputeFingerprint("resource.file", "file1"),
				ComputeFingerprint("resource.file", "file2"),
				ComputeFingerprint("resource.file", ExistsRegex),
				ComputeFingerprint("custom", ExistsRegex),
			),
		},
		{
			name: "verify query-api generated log check",
			input: map[string]mapset.Set[string]{
				"_cardinalhq.telemetry_type": mapset.NewSet("logs"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("_cardinalhq.telemetry_type", ExistsRegex),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToFingerprints(tt.input)
			assert.True(t, tt.expectedFingerp.Equal(got), "expected %v, got %v", tt.expectedFingerp.ToSlice(), got.ToSlice())
		})
	}
}
