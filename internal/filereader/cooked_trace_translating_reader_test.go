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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestCookedTraceTranslatingReader_GetSchema tests that schema is augmented with chq_tsns.
func TestCookedTraceTranslatingReader_GetSchema(t *testing.T) {
	// Create test parquet data
	rows := []map[string]any{
		{
			"chq_timestamp": int64(1000),
			"chq_span_id":   "span123",
			"chq_trace_id":  "trace456",
		},
	}

	parquetData, _ := createTestParquetInMemory(t, rows)
	reader := bytes.NewReader(parquetData)

	// ParquetRawReader implements Reader
	rawReader, err := NewParquetRawReader(reader, int64(len(parquetData)), 1000)
	require.NoError(t, err)
	defer func() { _ = rawReader.Close() }()

	// CookedTraceTranslatingReader wraps it and augments schema
	cookedReader := NewCookedTraceTranslatingReader(rawReader)
	defer func() { _ = cookedReader.Close() }()

	// Schema should be valid and include chq_tsns
	schema := cookedReader.GetSchema()
	assert.NotNil(t, schema, "Schema must not be nil")
	assert.True(t, schema.HasColumn(wkk.RowKeyValue(wkk.RowKeyCTsns)), "Schema should include chq_tsns column")
}

// TestCookedTraceTranslatingReader_GetSchema_NoSchema tests behavior with non-schema reader.
func TestCookedTraceTranslatingReader_GetSchema_NoSchema(t *testing.T) {
	// Create a mock reader without schema support
	mockReader := newMockReader("test", []pipeline.Row{
		{wkk.NewRowKey("test"): "value"},
	}, nil)

	cookedReader := NewCookedTraceTranslatingReader(mockReader)
	defer func() { _ = cookedReader.Close() }()

	// Should return valid schema with chq_tsns even when wrapped reader doesn't provide schema
	schema := cookedReader.GetSchema()
	assert.NotNil(t, schema, "Schema must not be nil even when wrapped reader has no schema")
	assert.True(t, schema.HasColumn(wkk.RowKeyValue(wkk.RowKeyCTsns)), "Schema should include chq_tsns column")
}
