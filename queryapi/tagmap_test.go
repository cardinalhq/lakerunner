// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagMap_MarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    TagMap
		expected string
	}{
		{
			name:     "empty map",
			input:    TagMap{},
			expected: `{}`,
		},
		{
			name:     "int64 preserved",
			input:    TagMap{"fingerprint": int64(8609019944874823681)},
			expected: `{"fingerprint":8609019944874823681}`,
		},
		{
			name:     "uint64 preserved",
			input:    TagMap{"id": uint64(18446744073709551615)},
			expected: `{"id":18446744073709551615}`,
		},
		{
			name:     "string value",
			input:    TagMap{"service": "api-gateway"},
			expected: `{"service":"api-gateway"}`,
		},
		{
			name:     "mixed types",
			input:    TagMap{"count": int64(42), "name": "test", "rate": 3.14},
			expected: `{"count":42,"name":"test","rate":3.14}`,
		},
		{
			name:     "keys sorted",
			input:    TagMap{"z": "last", "a": "first", "m": "middle"},
			expected: `{"a":"first","m":"middle","z":"last"}`,
		},
		{
			name:     "nil map",
			input:    nil,
			expected: `{}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := json.Marshal(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, string(got))
		})
	}
}

func TestTagMap_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TagMap
	}{
		{
			name:     "empty object",
			input:    `{}`,
			expected: TagMap{},
		},
		{
			name:     "integer preserved as int64",
			input:    `{"fingerprint":8609019944874823681}`,
			expected: TagMap{"fingerprint": int64(8609019944874823681)},
		},
		{
			name:     "float stays float",
			input:    `{"rate":3.14}`,
			expected: TagMap{"rate": 3.14},
		},
		{
			name:     "string value",
			input:    `{"service":"api-gateway"}`,
			expected: TagMap{"service": "api-gateway"},
		},
		{
			name:     "mixed types",
			input:    `{"count":42,"name":"test","rate":3.14}`,
			expected: TagMap{"count": int64(42), "name": "test", "rate": 3.14},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got TagMap
			err := json.Unmarshal([]byte(tt.input), &got)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestTagMap_RoundTrip(t *testing.T) {
	original := TagMap{
		"chq_fingerprint": int64(8609019944874823681),
		"service":         "api-gateway",
		"count":           int64(100),
		"rate":            99.5,
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded TagMap
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, original, decoded)
}
