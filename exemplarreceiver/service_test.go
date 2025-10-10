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

package exemplarreceiver

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestRowGetString(t *testing.T) {
	tests := []struct {
		name     string
		row      pipeline.Row
		key      string
		expected string
	}{
		{
			name:     "string value",
			row:      pipeline.Row{wkk.NewRowKey("key"): "value"},
			key:      "key",
			expected: "value",
		},
		{
			name:     "missing key",
			row:      pipeline.Row{},
			key:      "key",
			expected: "",
		},
		{
			name:     "non-string value",
			row:      pipeline.Row{wkk.NewRowKey("key"): 123},
			key:      "key",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.row.GetString(wkk.NewRowKey(tt.key))
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestRowGetInt64(t *testing.T) {
	tests := []struct {
		name       string
		row        pipeline.Row
		key        string
		expected   int64
		expectedOk bool
	}{
		{
			name:       "int64 value",
			row:        pipeline.Row{wkk.NewRowKey("key"): int64(123)},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "int value",
			row:        pipeline.Row{wkk.NewRowKey("key"): 123},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "float64 value",
			row:        pipeline.Row{wkk.NewRowKey("key"): float64(123.5)},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "missing key",
			row:        pipeline.Row{},
			key:        "key",
			expected:   0,
			expectedOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := tt.row.GetInt64(wkk.NewRowKey(tt.key))
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.expectedOk, ok)
		})
	}
}

func TestRowGetInt32(t *testing.T) {
	tests := []struct {
		name       string
		row        pipeline.Row
		key        string
		expected   int32
		expectedOk bool
	}{
		{
			name:       "int32 value",
			row:        pipeline.Row{wkk.NewRowKey("key"): int32(123)},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "int value",
			row:        pipeline.Row{wkk.NewRowKey("key"): 123},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "int64 value",
			row:        pipeline.Row{wkk.NewRowKey("key"): int64(123)},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "float64 value",
			row:        pipeline.Row{wkk.NewRowKey("key"): float64(123.5)},
			key:        "key",
			expected:   123,
			expectedOk: true,
		},
		{
			name:       "missing key",
			row:        pipeline.Row{},
			key:        "key",
			expected:   0,
			expectedOk: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := tt.row.GetInt32(wkk.NewRowKey(tt.key))
			assert.Equal(t, tt.expected, result)
			assert.Equal(t, tt.expectedOk, ok)
		})
	}
}
