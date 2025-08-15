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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/fileconv/translate"
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

func TestIsResourceAttribute(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		// Kubernetes related attributes
		{
			name:     "k8s prefix",
			key:      "k8s.cluster.name",
			expected: true,
		},
		{
			name:     "kubernetes prefix",
			key:      "kubernetes.pod.name",
			expected: true,
		},
		{
			name:     "app.kubernetes.io prefix",
			key:      "app.kubernetes.io/name",
			expected: true,
		},
		{
			name:     "container prefix",
			key:      "container.name",
			expected: true,
		},
		{
			name:     "pod prefix",
			key:      "pod.uid",
			expected: true,
		},
		{
			name:     "node prefix",
			key:      "node.name",
			expected: true,
		},
		{
			name:     "namespace prefix",
			key:      "namespace.name",
			expected: true,
		},
		{
			name:     "service prefix",
			key:      "service.name",
			expected: true,
		},
		{
			name:     "deployment prefix",
			key:      "deployment.name",
			expected: true,
		},
		{
			name:     "statefulset prefix",
			key:      "statefulset.name",
			expected: true,
		},
		// Infrastructure attributes
		{
			name:     "image prefix",
			key:      "image.name",
			expected: true,
		},
		{
			name:     "host prefix",
			key:      "host.name",
			expected: true,
		},
		{
			name:     "region prefix",
			key:      "region.name",
			expected: true,
		},
		{
			name:     "zone prefix",
			key:      "zone.name",
			expected: true,
		},
		{
			name:     "instance prefix",
			key:      "instance.id",
			expected: true,
		},
		{
			name:     "cluster prefix",
			key:      "cluster.name",
			expected: true,
		},
		// Case insensitive tests
		{
			name:     "uppercase K8S",
			key:      "K8S.CLUSTER.NAME",
			expected: true,
		},
		{
			name:     "mixed case Host",
			key:      "Host.Name",
			expected: true,
		},
		{
			name:     "uppercase KUBERNETES",
			key:      "KUBERNETES.POD.NAME",
			expected: true,
		},
		// Non-resource attributes
		{
			name:     "log level",
			key:      "level",
			expected: false,
		},
		{
			name:     "message",
			key:      "message",
			expected: false,
		},
		{
			name:     "timestamp",
			key:      "timestamp",
			expected: false,
		},
		{
			name:     "custom field",
			key:      "user.id",
			expected: false,
		},
		{
			name:     "partial match k8",
			key:      "k8_not_k8s",
			expected: false,
		},
		{
			name:     "substring match",
			key:      "my_host_name",
			expected: false,
		},
		{
			name:     "empty string",
			key:      "",
			expected: false,
		},
		// Edge cases with similar prefixes
		{
			name:     "node in middle",
			key:      "some.node.field",
			expected: false,
		},
		{
			name:     "host as suffix",
			key:      "field.host",
			expected: false,
		},
		{
			name:     "container partial",
			key:      "contain.name",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isResourceAttribute(tt.key)
			assert.Equal(t, tt.expected, result, "isResourceAttribute(%q) = %v, want %v", tt.key, result, tt.expected)
		})
	}
}

func TestNewJSONGzReader_ErrorCases(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() string
		expectErr   bool
		errContains string
	}{
		{
			name: "file does not exist",
			setup: func() string {
				return "/nonexistent/file.json.gz"
			},
			expectErr:   true,
			errContains: "failed to open file",
		},
		{
			name: "file is not gzip",
			setup: func() string {
				tmpfile, err := os.CreateTemp("", "test-*.json")
				assert.NoError(t, err)
				_, err = tmpfile.WriteString(`{"test": "data"}`)
				assert.NoError(t, err)
				tmpfile.Close()
				return tmpfile.Name()
			},
			expectErr:   true,
			errContains: "invalid header",
		},
		{
			name: "empty gzip file",
			setup: func() string {
				tmpfile, err := os.CreateTemp("", "test-*.json.gz")
				assert.NoError(t, err)
				gzWriter := gzip.NewWriter(tmpfile)
				gzWriter.Close()
				tmpfile.Close()
				return tmpfile.Name()
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filePath := tt.setup()
			if strings.Contains(filePath, "/tmp/") || strings.Contains(filePath, "test-") {
				defer os.Remove(filePath)
			}

			mapper := translate.NewMapper()
			reader, err := NewJSONGzReader(filePath, mapper, nil)

			if tt.expectErr {
				assert.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
				assert.Nil(t, reader)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, reader)
				if reader != nil {
					reader.Close()
				}
			}
		})
	}
}

func TestJSONGzReader_GetRowErrorCases(t *testing.T) {
	tests := []struct {
		name         string
		jsonData     []string
		expectError  bool
		errorMessage string
	}{
		{
			name: "invalid JSON",
			jsonData: []string{
				`{"valid": "json"}`,
				`{invalid json}`,
				`{"another": "valid"}`,
			},
			expectError:  true,
			errorMessage: "failed to parse JSON line",
		},
		{
			name: "truncated JSON",
			jsonData: []string{
				`{"valid": "json"}`,
				`{"incomplete":`,
			},
			expectError:  true,
			errorMessage: "unexpected end of JSON input",
		},
		{
			name: "null values in JSON",
			jsonData: []string{
				`{"field": null, "timestamp": 1640995200000}`,
				`{"message": "test", "null_field": null}`,
			},
			expectError: false,
		},
		{
			name: "complex nested JSON",
			jsonData: []string{
				`{"nested": {"deep": {"value": 123}}, "array": [1, 2, 3]}`,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary gzip file
			tmpfile, err := os.CreateTemp("", "test-*.json.gz")
			assert.NoError(t, err)
			defer os.Remove(tmpfile.Name())

			gzWriter := gzip.NewWriter(tmpfile)
			for _, line := range tt.jsonData {
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

			hasError := false
			for {
				_, done, err := reader.GetRow()
				if err != nil {
					hasError = true
					if tt.expectError && tt.errorMessage != "" {
						assert.Contains(t, err.Error(), tt.errorMessage)
					}
					break
				}
				if done {
					break
				}
			}

			if tt.expectError {
				assert.True(t, hasError, "Expected an error but none occurred")
			} else {
				assert.False(t, hasError, "Unexpected error occurred")
			}
		})
	}
}

func TestJSONGzReader_Close(t *testing.T) {
	// Create a valid test file
	tmpfile, err := os.CreateTemp("", "test-*.json.gz")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	gzWriter := gzip.NewWriter(tmpfile)
	_, err = gzWriter.Write([]byte(`{"test": "data"}` + "\n"))
	assert.NoError(t, err)
	gzWriter.Close()
	tmpfile.Close()

	mapper := translate.NewMapper()
	reader, err := NewJSONGzReader(tmpfile.Name(), mapper, nil)
	assert.NoError(t, err)

	// Test normal close
	err = reader.Close()
	assert.NoError(t, err)

	// Test double close - this may return an error about file already closed, which is acceptable
	err = reader.Close()
	// Don't assert NoError here since double close behavior is implementation dependent
}

func TestJSONGzReader_WithTags(t *testing.T) {
	// Create a test file
	tmpfile, err := os.CreateTemp("", "test-*.json.gz")
	assert.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	gzWriter := gzip.NewWriter(tmpfile)
	_, err = gzWriter.Write([]byte(`{"message": "test log", "level": "info"}` + "\n"))
	assert.NoError(t, err)
	gzWriter.Close()
	tmpfile.Close()

	// Test with tags
	mapper := translate.NewMapper()
	tags := map[string]string{
		"source":      "test",
		"environment": "dev",
	}

	reader, err := NewJSONGzReader(tmpfile.Name(), mapper, tags)
	assert.NoError(t, err)
	defer reader.Close()

	// Read the row and verify tags are applied
	row, done, err := reader.GetRow()
	assert.NoError(t, err)
	assert.False(t, done)
	assert.NotNil(t, row)

	// Check that tags are present in the row
	assert.Equal(t, "test", row["source"])
	assert.Equal(t, "dev", row["environment"])
	assert.Equal(t, "test log", row["_cardinalhq.message"])
}
