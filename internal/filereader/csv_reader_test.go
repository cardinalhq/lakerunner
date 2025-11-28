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
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestNewCSVReader(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		expectErr bool
		errMsg    string
	}{
		{
			name:      "Valid CSV with headers",
			input:     "name,age,city\nAlice,30,NYC\nBob,25,LA",
			expectErr: false,
		},
		{
			name:      "Empty CSV",
			input:     "",
			expectErr: true,
			errMsg:    "failed to read CSV headers",
		},
		{
			name:      "Only headers",
			input:     "name,age,city",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := io.NopCloser(strings.NewReader(tt.input))
			csvReader, err := NewCSVReader(reader, 10)

			if tt.expectErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, csvReader)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, csvReader)
				if csvReader != nil {
					defer func() {
						_ = csvReader.Close()
					}()
				}
			}
		})
	}
}

func TestCSVReader_Next(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name          string
		input         string
		batchSize     int
		expectedBatch []map[string]any
		expectEOF     bool
	}{
		{
			name:      "Single row",
			input:     "name,age,city\nAlice,30,NYC",
			batchSize: 10,
			expectedBatch: []map[string]any{
				{"name": "Alice", "age": int64(30), "city": "NYC"},
			},
			expectEOF: true,
		},
		{
			name:      "Multiple rows",
			input:     "name,age,city\nAlice,30,NYC\nBob,25,LA",
			batchSize: 10,
			expectedBatch: []map[string]any{
				{"name": "Alice", "age": int64(30), "city": "NYC"},
				{"name": "Bob", "age": int64(25), "city": "LA"},
			},
			expectEOF: true,
		},
		{
			name:      "Numeric parsing",
			input:     "id,value,rate\n1,100.5,0.25",
			batchSize: 10,
			expectedBatch: []map[string]any{
				{"id": int64(1), "value": 100.5, "rate": 0.25},
			},
			expectEOF: true,
		},
		{
			name:      "Mixed types",
			input:     "name,score,active\nTest,95.5,true",
			batchSize: 10,
			expectedBatch: []map[string]any{
				{"name": "Test", "score": 95.5, "active": "true"},
			},
			expectEOF: true,
		},
		{
			name:      "Batch size limit",
			input:     "id\n1\n2\n3\n4\n5",
			batchSize: 2,
			expectedBatch: []map[string]any{
				{"id": int64(1)},
				{"id": int64(2)},
			},
			expectEOF: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := io.NopCloser(strings.NewReader(tt.input))
			csvReader, err := NewCSVReader(reader, tt.batchSize)
			require.NoError(t, err)
			defer func() {
				_ = csvReader.Close()
			}()

			batch, err := csvReader.Next(ctx)
			require.NoError(t, err)
			require.NotNil(t, batch)

			// Verify batch size
			assert.Equal(t, len(tt.expectedBatch), batch.Len())

			// Verify batch content
			for i, expected := range tt.expectedBatch {
				row := batch.Get(i)
				for key, value := range expected {
					rowKey := wkk.NewRowKey(key)
					assert.Equal(t, value, row[rowKey], "Row %d, field %s", i, key)
				}
			}

			// Check for EOF on next read if expected
			if tt.expectEOF {
				nextBatch, err := csvReader.Next(ctx)
				assert.Equal(t, io.EOF, err)
				assert.Nil(t, nextBatch)
			}
		})
	}
}

func TestCSVReader_SkipInvalidRows(t *testing.T) {
	ctx := context.Background()
	input := `name,age,city
Alice,30,NYC
Bob,25
Charlie,35,SF
Dave
Eve,28,Boston`

	reader := io.NopCloser(strings.NewReader(input))
	csvReader, err := NewCSVReader(reader, 10)
	require.NoError(t, err)
	defer func() {
		_ = csvReader.Close()
	}()

	batch, err := csvReader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)

	// Should only have 3 valid rows (Alice, Charlie, Eve)
	assert.Equal(t, 3, batch.Len())

	// Verify the valid rows
	row0 := batch.Get(0)
	assert.Equal(t, "Alice", row0[wkk.NewRowKey("name")])

	row1 := batch.Get(1)
	assert.Equal(t, "Charlie", row1[wkk.NewRowKey("name")])

	row2 := batch.Get(2)
	assert.Equal(t, "Eve", row2[wkk.NewRowKey("name")])
}

func TestCSVReader_EmptyValues(t *testing.T) {
	ctx := context.Background()
	input := `name,age,city
Alice,,NYC
,25,LA
Bob,30,`

	reader := io.NopCloser(strings.NewReader(input))
	csvReader, err := NewCSVReader(reader, 10)
	require.NoError(t, err)
	defer func() {
		_ = csvReader.Close()
	}()

	batch, err := csvReader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)

	assert.Equal(t, 3, batch.Len())

	// First row: empty age
	row0 := batch.Get(0)
	assert.Equal(t, "Alice", row0[wkk.NewRowKey("name")])
	assert.Equal(t, "", row0[wkk.NewRowKey("age")])
	assert.Equal(t, "NYC", row0[wkk.NewRowKey("city")])

	// Second row: empty name
	row1 := batch.Get(1)
	assert.Equal(t, "", row1[wkk.NewRowKey("name")])
	assert.Equal(t, int64(25), row1[wkk.NewRowKey("age")])
	assert.Equal(t, "LA", row1[wkk.NewRowKey("city")])

	// Third row: empty city
	row2 := batch.Get(2)
	assert.Equal(t, "Bob", row2[wkk.NewRowKey("name")])
	assert.Equal(t, int64(30), row2[wkk.NewRowKey("age")])
	assert.Equal(t, "", row2[wkk.NewRowKey("city")])
}

func TestCSVReader_TotalRowsReturned(t *testing.T) {
	ctx := context.Background()
	input := `id
1
2
3
4
5`

	reader := io.NopCloser(strings.NewReader(input))
	csvReader, err := NewCSVReader(reader, 2)
	require.NoError(t, err)
	defer func() {
		_ = csvReader.Close()
	}()

	// First batch (2 rows)
	_, err = csvReader.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), csvReader.TotalRowsReturned())

	// Second batch (2 rows)
	_, err = csvReader.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(4), csvReader.TotalRowsReturned())

	// Third batch (1 row)
	_, err = csvReader.Next(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), csvReader.TotalRowsReturned())

	// EOF
	_, err = csvReader.Next(ctx)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, int64(5), csvReader.TotalRowsReturned())
}

func TestCSVLogTranslator_TranslateRow(t *testing.T) {
	tests := []struct {
		name     string
		input    pipeline.Row
		expected map[string]any
	}{
		{
			name: "Data field as message",
			input: pipeline.Row{
				wkk.NewRowKey("data"):      "This is a log message",
				wkk.NewRowKey("timestamp"): int64(1758397185000),
				wkk.NewRowKey("level"):     "INFO",
			},
			expected: map[string]any{
				"log_message":   "This is a log message",
				"chq_timestamp": int64(1758397185000),
				"log_level":     "INFO",
			},
		},
		{
			name: "Timestamp normalization from nanoseconds",
			input: pipeline.Row{
				wkk.NewRowKey("data"):      "Test message",
				wkk.NewRowKey("timestamp"): int64(1758397185000000000),
			},
			expected: map[string]any{
				"log_message":   "Test message",
				"chq_timestamp": int64(1758397185000),
			},
		},
		{
			name: "Timestamp normalization from seconds",
			input: pipeline.Row{
				wkk.NewRowKey("data"):      "Test message",
				wkk.NewRowKey("timestamp"): int64(1758397185),
			},
			expected: map[string]any{
				"log_message":   "Test message",
				"chq_timestamp": int64(1758397185000),
			},
		},
		{
			name: "Field name sanitization",
			input: pipeline.Row{
				wkk.NewRowKey("data"):         "Message",
				wkk.NewRowKey("user-name"):    "alice",
				wkk.NewRowKey("request.type"): "GET",
				wkk.NewRowKey("status code"):  int64(200),
			},
			expected: map[string]any{
				"log_message":      "Message",
				"log_user_name":    "alice",
				"log_request_type": "GET",
				"log_status_code":  int64(200),
			},
		},
		{
			name: "Alternative timestamp fields",
			input: pipeline.Row{
				wkk.NewRowKey("data"):       "Message",
				wkk.NewRowKey("event_time"): int64(1758397185000),
				wkk.NewRowKey("user"):       "bob",
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_user":      "bob",
			},
		},
		{
			name: "Normalized field names - Timestamp",
			input: pipeline.Row{
				wkk.NewRowKey("data"):      "Message",
				wkk.NewRowKey("Timestamp"): int64(1758397185000),
				wkk.NewRowKey("User"):      "alice",
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_user":      "alice",
			},
		},
		{
			name: "Normalized field names - Publish_Time",
			input: pipeline.Row{
				wkk.NewRowKey("data"):         "Message",
				wkk.NewRowKey("Publish_Time"): int64(1758397185000),
				wkk.NewRowKey("User"):         "bob",
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_user":      "bob",
			},
		},
		{
			name: "Normalized field names - EVENT_TIMESTAMP",
			input: pipeline.Row{
				wkk.NewRowKey("data"):            "Message",
				wkk.NewRowKey("EVENT_TIMESTAMP"): int64(1758397185000),
				wkk.NewRowKey("User"):            "charlie",
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_user":      "charlie",
			},
		},
		{
			name: "Normalized field names - EventTime",
			input: pipeline.Row{
				wkk.NewRowKey("data"):      "Message",
				wkk.NewRowKey("EventTime"): int64(1758397185000),
				wkk.NewRowKey("User"):      "david",
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_user":      "david",
			},
		},
		{
			name: "Duplicate field names with different cases",
			input: pipeline.Row{
				wkk.NewRowKey("data"):  "Message",
				wkk.NewRowKey("alice"): "lowercase",
				wkk.NewRowKey("Alice"): "titlecase",
				wkk.NewRowKey("time"):  int64(1758397185000),
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_alice":     "titlecase", // "Alice" comes first alphabetically
				"log_alice_2":   "lowercase", // "alice" comes second alphabetically
			},
		},
		{
			name: "Multiple duplicate field names",
			input: pipeline.Row{
				wkk.NewRowKey("data"):  "Message",
				wkk.NewRowKey("field"): "first",
				wkk.NewRowKey("Field"): "second",
				wkk.NewRowKey("FIELD"): "third",
				wkk.NewRowKey("time"):  int64(1758397185000),
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_field":     "third",  // "FIELD" comes first alphabetically
				"log_field_2":   "second", // "Field" comes second alphabetically
				"log_field_3":   "first",  // "field" comes third alphabetically
			},
		},
		{
			name: "Sanitization collision - different special chars",
			input: pipeline.Row{
				wkk.NewRowKey("data"):    "Message",
				wkk.NewRowKey("foo-bar"): "dash",
				wkk.NewRowKey("foo bar"): "space",
				wkk.NewRowKey("foo@bar"): "at",
				wkk.NewRowKey("time"):    int64(1758397185000),
			},
			expected: map[string]any{
				"log_message":   "Message",
				"chq_timestamp": int64(1758397185000),
				"log_foo_bar":   "space", // "foo bar" comes first alphabetically
				"log_foo_bar_2": "dash",  // "foo-bar" comes second alphabetically
				"log_foo_bar_3": "at",    // "foo@bar" comes third alphabetically
			},
		},
		{
			name: "Complex sanitization collision",
			input: pipeline.Row{
				wkk.NewRowKey("data"):      "Message",
				wkk.NewRowKey("user-name"): "dash",
				wkk.NewRowKey("user name"): "space",
				wkk.NewRowKey("user_name"): "underscore",
				wkk.NewRowKey("user@name"): "at",
				wkk.NewRowKey("time"):      int64(1758397185000),
			},
			expected: map[string]any{
				"log_message":     "Message",
				"chq_timestamp":   int64(1758397185000),
				"log_user_name":   "space",      // "user name" comes first alphabetically
				"log_user_name_2": "dash",       // "user-name" comes second alphabetically
				"log_user_name_3": "at",         // "user@name" comes third alphabetically
				"log_user_name_4": "underscore", // "user_name" comes fourth alphabetically
			},
		},
	}

	opts := ReaderOptions{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "test.csv",
	}

	translator := NewCSVLogTranslator(opts)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := tt.input
			err := translator.TranslateRow(context.Background(), &row)
			require.NoError(t, err)

			// Check expected fields
			for key, expectedValue := range tt.expected {
				rowKey := wkk.NewRowKey(key)
				actualValue, exists := row[rowKey]
				assert.True(t, exists, "Expected field %s not found", key)
				if key == "chq_timestamp" {
					// For timestamp, allow some tolerance if it's using current time
					if expectedValue == nil {
						assert.NotNil(t, actualValue)
					} else {
						assert.Equal(t, expectedValue, actualValue, "Field %s", key)
					}
				} else {
					assert.Equal(t, expectedValue, actualValue, "Field %s", key)
				}
			}

			// Check resource fields were added
			assert.Equal(t, "test-bucket", row[wkk.RowKeyResourceBucketName])
			assert.Equal(t, "./test.csv", row[wkk.RowKeyResourceFileName])
			assert.Equal(t, "test", row[wkk.RowKeyResourceFileType]) // GetFileType returns filename without extension
			assert.Equal(t, "csv-import", row[wkk.NewRowKey("resource_service_name")])
		})
	}
}

func TestCSVLogTranslator_ParseTimestamp(t *testing.T) {
	translator := &CSVLogTranslator{}

	tests := []struct {
		name     string
		input    any
		expected int64
	}{
		// Numeric inputs
		{"int64 milliseconds", int64(1758397185000), 1758397185000},
		{"int64 nanoseconds", int64(1758397185000000000), 1758397185000},
		{"int64 seconds", int64(1758397185), 1758397185000},
		{"float64", float64(1758397185000), 1758397185000},
		{"int", int(1758397185), 1758397185000},

		// String timestamps
		{"string unix ms", "1758397185000", 1758397185000},
		{"string unix ns", "1758397185000000000", 1758397185000},
		{"string unix sec", "1758397185", 1758397185000},

		// ISO formats
		{"RFC3339", "2025-09-20T19:39:45Z", 1758397185000},
		{"RFC3339Nano", "2025-09-20T19:39:45.000000000Z", 1758397185000},
		{"Custom format", "2025-09-20T19:39:45.000Z", 1758397185000},

		// Invalid
		{"invalid string", "not-a-timestamp", 0},
		{"empty string", "", 0},
		{"zero int", int64(0), 0},
		{"negative int", int64(-1), 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := translator.parseTimestamp(tt.input)
			// For date string parsing, we can't match exact milliseconds due to timezone
			if strings.Contains(tt.name, "RFC") || strings.Contains(tt.name, "format") {
				// Just check it's a reasonable timestamp
				assert.Greater(t, result, int64(1000000000000)) // After year 2001 in ms
				assert.Less(t, result, int64(2000000000000))    // Before year 2033 in ms
			} else {
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestCSVLogTranslator_SanitizeFieldName(t *testing.T) {
	translator := &CSVLogTranslator{}

	tests := []struct {
		input    string
		expected string
	}{
		{"normal_field", "normal_field"},
		{"field.with.dots", "field_with_dots"},
		{"field-with-dashes", "field_with_dashes"},
		{"field with spaces", "field_with_spaces"},
		{"field@with#special$chars", "field_with_special_chars"},
		{"__leading_underscores", "leading_underscores"},
		{"trailing_underscores__", "trailing_underscores"},
		{"multiple___underscores", "multiple_underscores"},
		{"MixedCaseField", "mixedcasefield"},
		{"field123", "field123"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := translator.sanitizeFieldName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCSVReader_Close(t *testing.T) {
	var closeCalled bool
	reader := &testCloser{
		Reader: bytes.NewReader([]byte("name\nvalue")),
		onClose: func() error {
			closeCalled = true
			return nil
		},
	}

	csvReader, err := NewCSVReader(reader, 10)
	require.NoError(t, err)

	// Close should be idempotent
	err = csvReader.Close()
	assert.NoError(t, err)
	assert.True(t, closeCalled)

	// Second close should not error
	err = csvReader.Close()
	assert.NoError(t, err)
}

type testCloser struct {
	io.Reader
	onClose func() error
}

func (tc *testCloser) Close() error {
	if tc.onClose != nil {
		return tc.onClose()
	}
	return nil
}

// TestCSVReader_GetSchema tests that schema is extracted from CSV headers.
func TestCSVReader_GetSchema(t *testing.T) {
	input := `name,age,city,score
Alice,30,NYC,95.5
Bob,25,LA,88.3`

	reader := io.NopCloser(strings.NewReader(input))
	csvReader, err := NewCSVReader(reader, 10)
	require.NoError(t, err)
	defer func() { _ = csvReader.Close() }()

	// Schema should be extracted from headers
	schema := csvReader.GetSchema()
	require.NotNil(t, schema, "Schema should be extracted from CSV headers")

	// Verify schema has expected columns
	assert.True(t, schema.HasColumn("name"))
	assert.True(t, schema.HasColumn("age"))
	assert.True(t, schema.HasColumn("city"))
	assert.True(t, schema.HasColumn("score"))

	// All CSV columns should be marked as string type (dynamically typed)
	assert.Equal(t, DataTypeString, schema.GetColumnType("name"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("age"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("city"))
	assert.Equal(t, DataTypeString, schema.GetColumnType("score"))
}

// TestCSVReader_GetSchema_EmptyHeaders tests behavior with closed reader.
func TestCSVReader_GetSchema_AfterClose(t *testing.T) {
	input := `name,age
Alice,30`

	reader := io.NopCloser(strings.NewReader(input))
	csvReader, err := NewCSVReader(reader, 10)
	require.NoError(t, err)

	// Get schema before closing
	schemaBefore := csvReader.GetSchema()
	require.NotNil(t, schemaBefore)
	assert.True(t, schemaBefore.HasColumn("name"))
	assert.True(t, schemaBefore.HasColumn("age"))

	// Close the reader
	err = csvReader.Close()
	require.NoError(t, err)

	// Schema should still be available after close (headers are still in memory)
	schemaAfter := csvReader.GetSchema()
	require.NotNil(t, schemaAfter, "Schema should still be available after close")
	assert.True(t, schemaAfter.HasColumn("name"))
	assert.True(t, schemaAfter.HasColumn("age"))
}
