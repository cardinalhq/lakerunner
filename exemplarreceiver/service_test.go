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
)

func TestGetStringFromMap(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]any
		key      string
		expected string
	}{
		{
			name:     "string value",
			m:        map[string]any{"key": "value"},
			key:      "key",
			expected: "value",
		},
		{
			name:     "missing key",
			m:        map[string]any{},
			key:      "key",
			expected: "",
		},
		{
			name:     "non-string value",
			m:        map[string]any{"key": 123},
			key:      "key",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getStringFromMap(tt.m, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetInt64FromMap(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]any
		key      string
		expected int64
	}{
		{
			name:     "int64 value",
			m:        map[string]any{"key": int64(123)},
			key:      "key",
			expected: 123,
		},
		{
			name:     "int value",
			m:        map[string]any{"key": 123},
			key:      "key",
			expected: 123,
		},
		{
			name:     "float64 value",
			m:        map[string]any{"key": float64(123.5)},
			key:      "key",
			expected: 123,
		},
		{
			name:     "string value",
			m:        map[string]any{"key": "123"},
			key:      "key",
			expected: 123,
		},
		{
			name:     "missing key",
			m:        map[string]any{},
			key:      "key",
			expected: 0,
		},
		{
			name:     "invalid string",
			m:        map[string]any{"key": "abc"},
			key:      "key",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getInt64FromMap(tt.m, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetInt32FromMap(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]any
		key      string
		expected int32
	}{
		{
			name:     "int32 value",
			m:        map[string]any{"key": int32(123)},
			key:      "key",
			expected: 123,
		},
		{
			name:     "int value",
			m:        map[string]any{"key": 123},
			key:      "key",
			expected: 123,
		},
		{
			name:     "int64 value",
			m:        map[string]any{"key": int64(123)},
			key:      "key",
			expected: 123,
		},
		{
			name:     "float64 value",
			m:        map[string]any{"key": float64(123.5)},
			key:      "key",
			expected: 123,
		},
		{
			name:     "string value",
			m:        map[string]any{"key": "123"},
			key:      "key",
			expected: 123,
		},
		{
			name:     "missing key",
			m:        map[string]any{},
			key:      "key",
			expected: 0,
		},
		{
			name:     "invalid string",
			m:        map[string]any{"key": "abc"},
			key:      "key",
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getInt32FromMap(tt.m, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}
