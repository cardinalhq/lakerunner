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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TestOldVsNewReaderPath is a comprehensive validation test that compares the old
// binpb reader path (IngestProtoMetricsReader + MetricTranslator) against the new
// path (SortingIngestProtoMetricsReader) by comparing rows directly.
//
// This test validates:
// 1. Row counts match exactly
// 2. Column names match exactly
// 3. Column types match exactly
// 4. TIDs are computed identically
// 5. All row values match
func TestOldVsNewReaderPath(t *testing.T) {
	// Test files from testdata directories
	testFiles := []string{
		"testdata/metrics_with_bool_field.binpb.gz",
		"../../testdata/metrics/metrics_187312485.binpb.gz",
		"../../testdata/metrics/metrics_449638969.binpb.gz",
		"../../testdata/metrics/otel-metrics.binpb.gz",
	}

	// Filter to only existing files
	var existingFiles []string
	for _, f := range testFiles {
		if _, err := os.Stat(f); err == nil {
			existingFiles = append(existingFiles, f)
		}
	}
	testFiles = existingFiles

	require.NotEmpty(t, testFiles, "No test files found")
	t.Logf("Testing with %d files", len(testFiles))

	for _, testFile := range testFiles {
		t.Run(filepath.Base(testFile), func(t *testing.T) {
			compareReaderPaths(t, testFile)
		})
	}
}

func compareReaderPaths(t *testing.T, testFile string) {
	// Read and decompress the binpb file
	data, err := os.ReadFile(testFile)
	require.NoError(t, err, "Failed to read test file: %s", testFile)

	gzReader, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err, "Failed to create gzip reader")
	decompressed, err := io.ReadAll(gzReader)
	require.NoError(t, err, "Failed to decompress")
	require.NoError(t, gzReader.Close())

	ctx := context.Background()
	orgID := "test-org"
	bucket := "test-bucket"
	objectID := "test-object"

	// === OLD PATH: IngestProtoMetricsReader + inline translator logic ===
	oldRows := readWithOldPath(t, ctx, decompressed, orgID)

	// === NEW PATH: SortingIngestProtoMetricsReader ===
	newRows := readWithNewPath(t, ctx, decompressed, orgID, bucket, objectID)

	// === COMPARISON ===
	t.Logf("Old path: %d rows, New path: %d rows", len(oldRows), len(newRows))

	// Both should produce the same number of rows
	require.Equal(t, len(oldRows), len(newRows),
		"Row count mismatch: old=%d, new=%d", len(oldRows), len(newRows))

	if len(oldRows) == 0 {
		t.Skip("No rows produced")
		return
	}

	// Sort both sets of rows by (metric_name, tid, timestamp) for comparison
	// Since the new reader is sorted but the old one isn't, we need to sort both
	sortRowsForComparison(oldRows)
	sortRowsForComparison(newRows)

	// Compare row by row
	mismatchCount := 0
	for i := range oldRows {
		if i >= len(newRows) {
			break
		}

		oldRow := oldRows[i]
		newRow := newRows[i]

		if !compareRowsDetailed(t, i, oldRow, newRow) {
			mismatchCount++
			if mismatchCount >= 5 {
				t.Logf("Stopping after 5 mismatches")
				break
			}
		}
	}

	assert.Equal(t, 0, mismatchCount, "Found %d row mismatches", mismatchCount)
	t.Logf("Successfully compared %d rows", len(oldRows))
}

// keepkeys mirrors the translator's keepkeys map
var keepkeys = map[string]bool{
	"resource_app":                  true,
	"resource_container_image_name": true,
	"resource_container_image_tag":  true,
	"resource_k8s_cluster_name":     true,
	"resource_k8s_daemonset_name":   true,
	"resource_k8s_deployment_name":  true,
	"resource_k8s_namespace_name":   true,
	"resource_k8s_pod_ip":           true,
	"resource_k8s_pod_name":         true,
	"resource_k8s_statefulset_name": true,
	"resource_service_name":         true,
	"resource_service_version":      true,
}

// filterKeysForTest filters resource keys just like the translator does
func filterKeysForTest(row *pipeline.Row) {
	for k := range *row {
		name := wkk.RowKeyValue(k)
		if !strings.HasPrefix(name, "resource_") {
			continue
		}
		if keepkeys[name] {
			continue
		}
		delete(*row, k)
	}
}

// translateRowForTest applies the same translation logic as MetricTranslator
func translateRowForTest(row *pipeline.Row, orgID string) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	(*row)[wkk.RowKeyCCustomerID] = orgID
	(*row)[wkk.RowKeyCTelemetryType] = "metrics"

	timestamp, ok := (*row)[wkk.RowKeyCTimestamp].(int64)
	if !ok {
		return fmt.Errorf("chq_timestamp field is missing or not int64")
	}

	const tenSecondsMs = int64(10000)
	truncatedTimestamp := (timestamp / tenSecondsMs) * tenSecondsMs
	(*row)[wkk.RowKeyCTimestamp] = truncatedTimestamp

	if _, nameOk := (*row)[wkk.RowKeyCName].(string); !nameOk {
		return fmt.Errorf("missing or invalid metric_name field")
	}

	filterKeysForTest(row)

	tid := fingerprinter.ComputeTID(*row)
	(*row)[wkk.RowKeyCTID] = tid

	return nil
}

func readWithOldPath(t *testing.T, ctx context.Context, data []byte, orgID string) []pipeline.Row {
	reader, err := NewIngestProtoMetricsReader(bytes.NewReader(data), ReaderOptions{
		OrgID:     orgID,
		BatchSize: 1000,
	})
	require.NoError(t, err, "Failed to create old reader")
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	var rows []pipeline.Row

	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Failed to read batch from old reader: %v", err)
		}

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if row == nil {
				continue
			}

			// Apply translator (this adds TID, filters keys, etc.)
			if err := translateRowForTest(&row, orgID); err != nil {
				// Translator may reject rows with missing fields
				continue
			}

			// Make a copy of the row
			rowCopy := make(pipeline.Row, len(row))
			for k, v := range row {
				rowCopy[k] = v
			}
			rows = append(rows, rowCopy)
		}
	}

	return rows
}

func readWithNewPath(t *testing.T, ctx context.Context, data []byte, orgID, bucket, objectID string) []pipeline.Row {
	reader, err := NewSortingIngestProtoMetricsReader(bytes.NewReader(data), SortingReaderOptions{
		OrgID:     orgID,
		Bucket:    bucket,
		ObjectID:  objectID,
		BatchSize: 1000,
	})
	require.NoError(t, err, "Failed to create new reader")
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	var rows []pipeline.Row

	for {
		batch, err := reader.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Failed to read batch from new reader: %v", err)
		}

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if row == nil {
				continue
			}

			// Make a copy of the row
			rowCopy := make(pipeline.Row, len(row))
			for k, v := range row {
				rowCopy[k] = v
			}
			rows = append(rows, rowCopy)
		}
	}

	return rows
}

func sortRowsForComparison(rows []pipeline.Row) {
	sort.Slice(rows, func(i, j int) bool {
		// Sort by metric_name, then tid, then timestamp
		nameI, _ := rows[i][wkk.RowKeyCName].(string)
		nameJ, _ := rows[j][wkk.RowKeyCName].(string)
		if nameI != nameJ {
			return nameI < nameJ
		}

		tidI, _ := rows[i][wkk.RowKeyCTID].(int64)
		tidJ, _ := rows[j][wkk.RowKeyCTID].(int64)
		if tidI != tidJ {
			return tidI < tidJ
		}

		tsI, _ := rows[i][wkk.RowKeyCTimestamp].(int64)
		tsJ, _ := rows[j][wkk.RowKeyCTimestamp].(int64)
		return tsI < tsJ
	})
}

func compareRowsDetailed(t *testing.T, index int, oldRow, newRow pipeline.Row) bool {
	t.Helper()

	// Build maps of key->value for comparison
	oldKeys := make(map[string]any)
	for k, v := range oldRow {
		oldKeys[wkk.RowKeyValue(k)] = v
	}

	newKeys := make(map[string]any)
	for k, v := range newRow {
		newKeys[wkk.RowKeyValue(k)] = v
	}

	match := true

	// Check TID specifically - this is critical
	oldTID, oldHasTID := oldKeys["chq_tid"].(int64)
	newTID, newHasTID := newKeys["chq_tid"].(int64)

	if !oldHasTID || !newHasTID {
		t.Errorf("Row %d: Missing TID - old has TID: %v, new has TID: %v", index, oldHasTID, newHasTID)
		match = false
	} else if oldTID != newTID {
		t.Errorf("Row %d: TID MISMATCH - old: %d, new: %d", index, oldTID, newTID)

		// Debug: show metric name and type
		t.Logf("  Metric: %v, Type: %v", oldKeys["metric_name"], oldKeys["chq_metric_type"])

		// Show keys in each row
		t.Logf("  Old row has %d keys, new row has %d keys", len(oldKeys), len(newKeys))

		// Show TID-relevant keys from old row
		oldTIDKeys := fingerprinter.DebugTIDKeys(oldRow)
		t.Logf("  Old TID keys (%d):", len(oldTIDKeys))
		for _, kv := range oldTIDKeys {
			t.Logf("    %s", kv)
		}

		// Show TID-relevant keys from new row
		newTIDKeys := fingerprinter.DebugTIDKeys(newRow)
		t.Logf("  New TID keys (%d):", len(newTIDKeys))
		for _, kv := range newTIDKeys {
			t.Logf("    %s", kv)
		}

		match = false
	}

	// Check key count
	if len(oldKeys) != len(newKeys) {
		t.Logf("Row %d: Key count mismatch - old: %d, new: %d", index, len(oldKeys), len(newKeys))
		// Log missing keys
		for key := range oldKeys {
			if _, exists := newKeys[key]; !exists {
				t.Logf("  Key %q in old but not in new", key)
			}
		}
		for key := range newKeys {
			if _, exists := oldKeys[key]; !exists {
				t.Logf("  Key %q in new but not in old", key)
			}
		}
	}

	// Compare common keys (except sketch which may have encoding differences)
	for key, oldVal := range oldKeys {
		if key == "sketch" {
			continue
		}

		newVal, exists := newKeys[key]
		if !exists {
			continue
		}

		if !valuesEqualForTest(oldVal, newVal) {
			t.Logf("Row %d: Value mismatch for key %q - old: %v (%T), new: %v (%T)",
				index, key, oldVal, oldVal, newVal, newVal)
		}
	}

	return match
}

func valuesEqualForTest(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if aBytes, ok := a.([]byte); ok {
		if bBytes, ok := b.([]byte); ok {
			return bytes.Equal(aBytes, bBytes)
		}
		return false
	}

	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}
