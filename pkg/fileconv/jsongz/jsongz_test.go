// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jsongz

import (
	"compress/gzip"
	"os"
	"testing"

	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
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
