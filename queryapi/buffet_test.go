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

package queryapi

import (
	"testing"

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
			name:   "_cardinalhq_telemetry_type:log",
			input:  "_cardinalhq_telemetry_type:log",
			expect: -1269926491184316255,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeHash(tt.input)
			assert.Equal(t, tt.expect, got, "computeHash(%q) = %d; want %d", tt.input, got, tt.expect)
		})
	}
}

func TestComputeFingerprint(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		trigram   string
		expected  int64
	}{
		{
			name:      "simple case",
			fieldName: "_cardinalhq_name",
			trigram:   "foo",
			expected:  computeHash("_cardinalhq_name:foo"),
		},
		{
			name:      "exists regex",
			fieldName: "_cardinalhq_message",
			trigram:   existsRegex,
			expected:  computeHash("_cardinalhq_message:.*"),
		},
		{
			name:      "telemetry type",
			fieldName: "_cardinalhq_telemetry_type",
			trigram:   "log",
			expected:  computeHash("_cardinalhq_telemetry_type:log"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeFingerprint(tt.fieldName, tt.trigram)
			assert.Equal(t, tt.expected, got)
		})
	}
}
