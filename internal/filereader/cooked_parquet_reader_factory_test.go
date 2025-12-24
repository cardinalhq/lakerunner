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

package filereader

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// createTestLogParquet creates a log Parquet file in memory for testing
func createTestLogParquet(t *testing.T, rowCount int) []byte {
	t.Helper()

	rows := make([]map[string]any, rowCount)
	for i := range rows {
		rows[i] = map[string]any{
			"chq_timestamp": int64(1000000 + i),
			"log_message":   fmt.Sprintf("Log message %d", i),
			"log_level":     "INFO",
		}
	}

	// Build schema
	nodes := make(map[string]parquet.Node)
	for key, value := range rows[0] {
		var node parquet.Node
		switch value.(type) {
		case int64:
			node = parquet.Optional(parquet.Int(64))
		case string:
			node = parquet.Optional(parquet.String())
		default:
			t.Fatalf("Unsupported type %T for key %s", value, key)
		}
		nodes[key] = node
	}

	schema := parquet.NewSchema("logs", parquet.Group(nodes))

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[map[string]any](&buf, schema)

	for _, row := range rows {
		_, err := writer.Write([]map[string]any{row})
		require.NoError(t, err, "Failed to write row")
	}

	err := writer.Close()
	require.NoError(t, err, "Failed to close writer")

	return buf.Bytes()
}

func TestNewCookedLogParquetReader(t *testing.T) {
	// Generate log data
	data := createTestLogParquet(t, 100)

	reader := bytes.NewReader(data)
	logReader, err := NewCookedLogParquetReader(reader, int64(len(data)), 1000)
	require.NoError(t, err)
	defer func() { _ = logReader.Close() }()

	batch, err := logReader.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Greater(t, batch.Len(), 0, "should have at least one row")

	// Verify log-specific fields are present and properly formatted
	row := batch.Get(0)

	// Check for required timestamp field
	timestamp, hasTimestamp := row[wkk.RowKeyCTimestamp]
	assert.True(t, hasTimestamp, "should have chq_timestamp")
	assert.IsType(t, int64(0), timestamp, "timestamp should be int64")

	// Check message field if present
	if message, hasMessage := row[wkk.RowKeyCMessage]; hasMessage {
		assert.IsType(t, "", message, "message should be string")
	}
}
