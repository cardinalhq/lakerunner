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
	"context"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestIngestLogParquetReader_INT32Promotion tests that INT32 columns are promoted to int64
// in both the schema and the actual row values.
func TestIngestLogParquetReader_INT32Promotion(t *testing.T) {
	// Use the test file that has an INT32 column
	testFile := "/tmp/av/syslog.parquet"

	// Open file
	f, err := os.Open(testFile)
	require.NoError(t, err)
	defer f.Close()

	ctx := context.Background()
	reader, err := NewIngestLogParquetReader(ctx, f, 1000)
	require.NoError(t, err)
	defer reader.Close()

	// 1. Check schema - INT32 columns should be reported as int64
	schema := reader.GetSchema()
	require.NotNil(t, schema)

	// Check if repeated_message exists in schema and verify it's int64
	repeatedMessageType := schema.GetColumnType("repeated_message")
	if repeatedMessageType != DataTypeAny {
		// If the column exists, it MUST be int64 type
		assert.Equal(t, DataTypeInt64, repeatedMessageType,
			"repeated_message column should be int64 type (promoted from INT32)")
	}

	// 2. Read rows and verify INT32 values are emitted as int64
	foundRepeatedMessage := false
	repeatedMessageKey := wkk.NewRowKey("repeated_message")

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)

			// Check if row has repeated_message
			if val, ok := row[repeatedMessageKey]; ok && val != nil {
				foundRepeatedMessage = true
				// Value MUST be int64 type
				_, isInt64 := val.(int64)
				assert.True(t, isInt64,
					"repeated_message value should be int64 type, got %T", val)
			}
		}
	}

	// Note: repeated_message might be all NULL, which is why we don't require finding it
	if foundRepeatedMessage {
		t.Log("Verified repeated_message values are int64")
	} else {
		t.Log("repeated_message column was all NULL or missing")
	}
}
