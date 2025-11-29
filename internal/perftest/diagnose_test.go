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

package perftest

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// TestDiagnoseSchemaTypes checks for type mismatches between schema and actual data
func TestDiagnoseSchemaTypes(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) == 0 {
		t.Skip("No raw test data found")
	}

	testFile := files[0]
	t.Logf("Analyzing: %s", testFile)

	ctx := context.Background()
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      "test-org",
	}

	reader, err := filereader.ReaderForFileWithOptions(testFile, options)
	if err != nil {
		t.Fatalf("Error creating reader: %v", err)
	}
	defer func() { _ = reader.Close() }()

	schema := reader.GetSchema()
	t.Logf("Schema from reader (%d columns):", len(schema.Columns()))
	for _, col := range schema.Columns() {
		t.Logf("  %s: %s (hasNonNull=%v)", col.Name.Value(), col.DataType, col.HasNonNull)
	}

	// Read ALL batches and check actual types
	typeMismatches := make(map[string]map[string]int) // field -> actualType -> count
	totalRows := 0

	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Error reading batch: %v", err)
		}

		totalRows += batch.Len()

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if row == nil {
				continue
			}

			for key, value := range row {
				if value == nil {
					continue
				}

				fieldName := string(key.Value())
				actualType := fmt.Sprintf("%T", value)

				// Find this field in schema
				var schemaType string
				for _, col := range schema.Columns() {
					if string(col.Name.Value()) == fieldName {
						schemaType = col.DataType.String()
						break
					}
				}

				if schemaType == "" {
					continue // Field not in schema
				}

				// Check for type mismatch
				expectedGoType := getExpectedGoType(schemaType)
				if actualType != expectedGoType {
					key := fieldName + " (schema:" + schemaType + " expected:" + expectedGoType + ")"
					if typeMismatches[key] == nil {
						typeMismatches[key] = make(map[string]int)
					}
					typeMismatches[key][actualType]++
				}
			}
		}

		pipeline.ReturnBatch(batch)
	}

	t.Logf("Checked %d total rows", totalRows)

	if len(typeMismatches) > 0 {
		t.Logf("⚠️  TYPE MISMATCHES FOUND:")
		for field, types := range typeMismatches {
			t.Logf("  %s", field)
			for typ, count := range types {
				t.Logf("    - %s: %d occurrences", typ, count)
			}
		}
		t.Errorf("Found %d fields with type mismatches", len(typeMismatches))
	} else {
		t.Logf("✓ No type mismatches found")
	}
}

func getExpectedGoType(dataType string) string {
	switch dataType {
	case "int64":
		return "int64"
	case "float64":
		return "float64"
	case "string":
		return "string"
	case "bool":
		return "bool"
	case "bytes":
		return "[]uint8"
	default:
		return "unknown"
	}
}
