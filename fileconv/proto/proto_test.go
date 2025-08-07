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

package proto

import (
	"testing"

	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProtoReader(t *testing.T) {
	tests := []struct {
		name        string
		fname       string
		mapper      *translate.Mapper
		tags        map[string]string
		expectError bool
	}{
		{
			name:        "valid proto file",
			fname:       "testdata/logs_160396104.binpb",
			mapper:      translate.NewMapper(),
			tags:        map[string]string{"test": "value"},
			expectError: false,
		},
		{
			name:        "non-existent file",
			fname:       "testdata/nonexistent.binpb",
			mapper:      translate.NewMapper(),
			tags:        nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader, err := NewProtoReader(tt.fname, tt.mapper, tt.tags)
			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, reader)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reader)
				defer reader.Close()
			}
		})
	}
}

func TestProtoReader_GetRow(t *testing.T) {
	reader, err := NewProtoReader("testdata/logs_160396104.binpb", translate.NewMapper(), map[string]string{
		"test_tag": "test_value",
		"env":      "test",
	})
	require.NoError(t, err)
	defer reader.Close()

	// Test that we can read multiple rows
	rowCount := 0
	maxRows := 10 // Limit to avoid long test runs

	for {
		row, done, err := reader.GetRow()
		if err != nil {
			t.Fatalf("Error reading row: %v", err)
		}
		if done {
			break
		}

		// Verify required fields are present
		assert.NotEmpty(t, row, "Row should not be empty")

		// Check that tags were added
		assert.Equal(t, "test_value", row["test_tag"], "Test tag should be present")
		assert.Equal(t, "test", row["env"], "Environment tag should be present")

		// Check that required cardinalhq fields are present
		assert.Contains(t, row, "_cardinalhq.telemetry_type", "Should have telemetry type")
		assert.Equal(t, "logs", row["_cardinalhq.telemetry_type"], "Telemetry type should be logs")

		assert.Contains(t, row, "_cardinalhq.name", "Should have name field")
		assert.Equal(t, "log.events", row["_cardinalhq.name"], "Name should be log.events")

		assert.Contains(t, row, "_cardinalhq.value", "Should have value field")
		assert.Equal(t, float64(1), row["_cardinalhq.value"], "Value should be 1")

		// Check that we have either message or timestamp
		hasMessage := false
		hasTimestamp := false
		for k, v := range row {
			if k == "_cardinalhq.message" && v != "" {
				hasMessage = true
			}
			if k == "_cardinalhq.timestamp" && v != nil {
				hasTimestamp = true
			}
		}
		assert.True(t, hasMessage || hasTimestamp, "Row should have either message or timestamp")

		rowCount++
		if rowCount >= maxRows {
			break
		}
	}

	assert.Greater(t, rowCount, 0, "Should have read at least one row")
}

func TestProtoReader_Close(t *testing.T) {
	reader, err := NewProtoReader("testdata/logs_160396104.binpb", translate.NewMapper(), nil)
	require.NoError(t, err)

	// Test that close doesn't error
	err = reader.Close()
	assert.NoError(t, err)

	// Test that close is idempotent
	err = reader.Close()
	assert.NoError(t, err)
}

func TestProtoReader_EmptyFile(t *testing.T) {
	// Test with a file that doesn't exist
	reader, err := NewProtoReader("testdata/nonexistent.binpb", translate.NewMapper(), nil)
	assert.Error(t, err)
	assert.Nil(t, reader)
}
