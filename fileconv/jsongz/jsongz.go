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
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"strings"

	"github.com/cardinalhq/lakerunner/fileconv"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
)

type JSONGzReader struct {
	fname    string
	file     *os.File
	gzReader *gzip.Reader
	scanner  *bufio.Scanner
	mapper   *translate.Mapper
	tags     map[string]string
	rowIndex int
}

var _ fileconv.Reader = (*JSONGzReader)(nil)

func NewJSONGzReader(fname string, mapper *translate.Mapper, tags map[string]string) (*JSONGzReader, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", fname, err)
	}

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	scanner := bufio.NewScanner(gzReader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // 1MB max line size

	return &JSONGzReader{
		fname:    fname,
		file:     file,
		gzReader: gzReader,
		scanner:  scanner,
		mapper:   mapper,
		tags:     tags,
		rowIndex: 0,
	}, nil
}

func (r *JSONGzReader) Close() error {
	var err error
	if r.gzReader != nil {
		if err = r.gzReader.Close(); err != nil {
			err = fmt.Errorf("failed to close gzip reader: %w", err)
		}
	}
	if r.file != nil {
		if closeErr := r.file.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("failed to close file: %w, gzip error: %w", closeErr, err)
			} else {
				err = fmt.Errorf("failed to close file: %w", closeErr)
			}
		}
	}
	return err
}

func (r *JSONGzReader) GetRow() (row map[string]any, done bool, err error) {
	if r.scanner == nil {
		return nil, true, fmt.Errorf("scanner is not initialized")
	}

	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, false, fmt.Errorf("failed to scan line: %w", err)
		}
		// EOF reached
		return nil, true, nil
	}

	line := r.scanner.Text()
	if line == "" {
		return r.GetRow()
	}

	var jsonData map[string]any
	if err := json.Unmarshal([]byte(line), &jsonData); err != nil {
		return nil, false, fmt.Errorf("failed to parse JSON line %d: %w", r.rowIndex+1, err)
	}

	if tags, ok := jsonData["tags"].([]any); ok {
		for _, tag := range tags {
			if tagStr, ok := tag.(string); ok && strings.Contains(tagStr, ":") {
				parts := strings.SplitN(tagStr, ":", 2)
				if len(parts) == 2 {
					key := strings.TrimSpace(parts[0])
					value := strings.TrimSpace(parts[1])

					if isResourceAttribute(key) {
						jsonData["resource."+key] = value
					} else {
						jsonData[key] = value
					}
				}
			}
		}
		delete(jsonData, "tags")
	}

	parsedRow := translate.ParseLogRow(r.mapper, jsonData)

	ret := make(map[string]any)
	for k, v := range parsedRow.ResourceAttributes {
		ret["resource."+k] = v
	}
	for k, v := range parsedRow.ScopeAttributes {
		ret["scope."+k] = v
	}
	for k, v := range parsedRow.RecordAttributes {
		if strings.HasPrefix(k, "log.") {
			ret[strings.TrimPrefix(k, "log.")] = v
		} else {
			ret[k] = v
		}
	}
	maps.Copy(ret, parsedRow.RawAttributes)

	if _, ok := ret["_cardinalhq.timestamp"]; !ok && parsedRow.Timestamp > 0 {
		ret["_cardinalhq.timestamp"] = parsedRow.Timestamp / 1000000 // Convert nanoseconds to milliseconds
	}
	if _, ok := ret["_cardinalhq.message"]; !ok && parsedRow.Body != "" {
		ret["_cardinalhq.message"] = parsedRow.Body
	}
	ret["_cardinalhq.name"] = "log.events"
	ret["_cardinalhq.telemetry_type"] = "logs"
	ret["_cardinalhq.value"] = float64(1)

	// Add tags
	for k, v := range r.tags {
		ret[k] = v
	}

	r.rowIndex++
	return ret, false, nil
}

// isResourceAttribute determines if a field should be treated as a resource attribute
func isResourceAttribute(key string) bool {
	resourcePrefixes := []string{
		"k8s.", "kubernetes.", "app.kubernetes.io/",
		"container.", "pod.", "node.", "namespace.",
		"service.", "deployment.", "statefulset.",
		"image.", "host.", "region.", "zone.",
		"instance.", "cluster.", "node.",
	}

	keyLower := strings.ToLower(key)
	for _, prefix := range resourcePrefixes {
		if strings.HasPrefix(keyLower, prefix) {
			return true
		}
	}

	return false
}
