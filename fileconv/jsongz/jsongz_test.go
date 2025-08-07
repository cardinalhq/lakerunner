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

package jsongz

import (
	"compress/gzip"
	"os"
	"testing"

	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/stretchr/testify/assert"
)

func TestJSONGzReader_ActualFile(t *testing.T) {
	filePath := "testdata/sample.json.gz"

	mapper := translate.NewMapper()

	reader, err := NewJSONGzReader(filePath, mapper, nil)
	assert.NoError(t, err)
	defer reader.Close()

	count := 0
	for {
		row, done, err := reader.GetRow()
		if err != nil {
			t.Fatalf("Error reading row: %v", err)
		}
		if done {
			break
		}
		assert.NotNil(t, row)
		assert.Contains(t, row, "_cardinalhq.timestamp")
		assert.Contains(t, row, "_cardinalhq.message")
		count++
	}
	assert.Equal(t, 3, count, "Expected to read 3 rows from the JSON.gz file")
}

func TestJSONGzReaderWithEmptyLines(t *testing.T) {
	// Create a test JSON.gz file with empty lines
	testData := []string{
		`{"timestamp": 1640995200000, "message": "Test log message", "level": "INFO"}`,
		``, // empty line
		`{"timestamp": 1640995260000, "message": "Another test message", "level": "ERROR"}`,
		``, // empty line
		``, // another empty line
		`{"timestamp": 1640995320000, "message": "Third message", "level": "WARN"}`,
	}

	// Create temporary file
	tmpfile, err := os.CreateTemp("", "test-*.json.gz")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// Write gzipped JSON data
	gzWriter := gzip.NewWriter(tmpfile)
	for _, line := range testData {
		_, err := gzWriter.Write([]byte(line + "\n"))
		assert.NoError(t, err)
	}
	gzWriter.Close()
	tmpfile.Close()

	// Test the reader
	mapper := translate.NewMapper()
	reader, err := NewJSONGzReader(tmpfile.Name(), mapper, nil)
	assert.NoError(t, err)
	defer reader.Close()

	count := 0
	for {
		row, done, err := reader.GetRow()
		if err != nil {
			t.Fatalf("Error reading row: %v", err)
		}
		if done {
			break
		}
		assert.NotNil(t, row)
		assert.Contains(t, row, "_cardinalhq.timestamp")
		assert.Contains(t, row, "_cardinalhq.message")
		count++
	}
	assert.Equal(t, 3, count) // Should only count non-empty lines
}
