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
	// Use a test file with actual INT32 values (not all NULL)
	testFile := "testdata/test_int32.parquet"

	// Open file
	f, err := os.Open(testFile)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	ctx := context.Background()
	reader, err := NewIngestLogParquetReader(ctx, f, 1000)
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	// 1. Check schema - INT32 columns MUST be reported as int64
	schema := reader.GetSchema()
	require.NotNil(t, schema)

	int32ValueType := schema.GetColumnType("int32_value")
	assert.Equal(t, DataTypeInt64, int32ValueType,
		"int32_value column MUST be int64 type in schema (promoted from INT32)")

	// 2. Read rows and verify INT32 values are emitted as int64
	foundValues := false
	int32ValueKey := wkk.NewRowKey("int32_value")
	expectedValues := []int64{100, 200, 300, 400, 500}
	actualValues := []int64{}

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)

			// Check if row has int32_value
			if val, ok := row[int32ValueKey]; ok && val != nil {
				foundValues = true
				// Value MUST be int64 type (not int32)
				int64Val, isInt64 := val.(int64)
				assert.True(t, isInt64,
					"int32_value MUST be int64 type in row, got %T with value %v", val, val)
				if isInt64 {
					actualValues = append(actualValues, int64Val)
				}
			}
		}
	}

	// MUST have found the values
	require.True(t, foundValues, "MUST find int32_value in rows")
	assert.Equal(t, expectedValues, actualValues,
		"int32_value values MUST match expected (promoted to int64)")
}
