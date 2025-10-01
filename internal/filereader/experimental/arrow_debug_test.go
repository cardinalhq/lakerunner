//go:build experimental

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

package experimental

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TestArrowReaderNullFieldHandling investigates the null field issue
func TestArrowReaderNullFieldHandling(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	// Read first row from each reader
	parquetRow, err := getFirstRowParquetRaw(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to get ParquetRaw row: %v", err)
	}

	duckdbRow, err := getFirstRowDuckDB([]string{absPath})
	if err != nil {
		t.Fatalf("Failed to get DuckDB row: %v", err)
	}

	arrowRow, err := getFirstRowArrow(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to get Arrow row: %v", err)
	}

	t.Logf("Field counts - ParquetRaw: %d, DuckDB: %d, Arrow: %d",
		len(parquetRow), len(duckdbRow), len(arrowRow))

	// Check what values the missing fields have in ParquetRaw and DuckDB
	missingInArrow := []string{
		"metric_transport", "metric_context", "metric_metricName", "metric_metricType",
		"metric_resolver", "metric_version", "metric_organization_id", "metric_data_type",
	}

	t.Logf("\n=== Analysis of missing fields ===")
	for _, fieldName := range missingInArrow {
		key := wkk.NewRowKeyFromBytes([]byte(fieldName))

		pVal, pExists := parquetRow[key]
		dVal, dExists := duckdbRow[key]
		_, aExists := arrowRow[key]

		t.Logf("Field '%s':", fieldName)
		t.Logf("  ParquetRaw: exists=%t, value=%v", pExists, pVal)
		t.Logf("  DuckDB:     exists=%t, value=%v", dExists, dVal)
		t.Logf("  Arrow:      exists=%t", aExists)

		if pExists && pVal == nil {
			t.Logf("  *** ParquetRaw has NULL value ***")
		}
		if dExists && dVal == nil {
			t.Logf("  *** DuckDB has NULL value ***")
		}
	}
}

// TestArrowReaderSchemaInspection inspects the Arrow schema directly
func TestArrowReaderSchemaInspection(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	// Create Arrow reader and inspect its schema
	reader := bytes.NewReader(data)
	arrowReader, err := NewArrowCookedReader(context.TODO(), reader, 1000)
	if err != nil {
		t.Fatalf("Failed to create Arrow reader: %v", err)
	}
	defer arrowReader.Close()

	// Get the first record to inspect schema
	rec, err := arrowReader.rr.Read()
	if err != nil {
		t.Fatalf("Failed to read first record: %v", err)
	}
	defer rec.Release()

	schema := rec.Schema()
	fields := schema.Fields()

	t.Logf("Arrow schema has %d fields:", len(fields))
	for i, field := range fields {
		col := rec.Column(i)
		nullCount := col.NullN()
		totalRows := col.Len()

		t.Logf("  Field[%d]: %s (type: %s, nulls: %d/%d)",
			i, field.Name, field.Type.String(), nullCount, totalRows)
	}
}

// TestCompareArrowAndParquetSchemas compares what each reader sees in the schema
func TestCompareArrowAndParquetSchemas(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	// Get schema info from ParquetRaw reader
	reader := bytes.NewReader(data)
	parquetReader, err := filereader.NewParquetRawReader(reader, int64(len(data)), 1000)
	if err != nil {
		t.Fatalf("Failed to create ParquetRaw reader: %v", err)
	}

	// Get first batch to see what fields ParquetRaw finds
	parquetBatch, err := parquetReader.Next(context.TODO())
	if err != nil {
		t.Fatalf("Failed to read ParquetRaw batch: %v", err)
	}
	parquetReader.Close()

	parquetFields := make(map[string]bool)
	if parquetBatch.Len() > 0 {
		firstRow := parquetBatch.Get(0)
		for k := range firstRow {
			parquetFields[string(k.Value())] = true
		}
	}

	// Get schema info from Arrow reader
	reader2 := bytes.NewReader(data)
	arrowReader, err := NewArrowCookedReader(context.TODO(), reader2, 1000)
	if err != nil {
		t.Fatalf("Failed to create Arrow reader: %v", err)
	}
	defer arrowReader.Close()

	rec, err := arrowReader.rr.Read()
	if err != nil {
		t.Fatalf("Failed to read Arrow record: %v", err)
	}
	defer rec.Release()

	arrowFields := make(map[string]bool)
	for _, field := range rec.Schema().Fields() {
		arrowFields[field.Name] = true
	}

	t.Logf("ParquetRaw schema: %d fields", len(parquetFields))
	t.Logf("Arrow schema: %d fields", len(arrowFields))

	// Find fields in Parquet but not in Arrow schema
	var missingFromArrowSchema []string
	for field := range parquetFields {
		if !arrowFields[field] {
			missingFromArrowSchema = append(missingFromArrowSchema, field)
		}
	}

	// Find fields in Arrow but not in Parquet schema
	var extraInArrowSchema []string
	for field := range arrowFields {
		if !parquetFields[field] {
			extraInArrowSchema = append(extraInArrowSchema, field)
		}
	}

	if len(missingFromArrowSchema) > 0 {
		t.Logf("\nFields missing from Arrow schema (%d): %v",
			len(missingFromArrowSchema), missingFromArrowSchema)
	}

	if len(extraInArrowSchema) > 0 {
		t.Logf("\nExtra fields in Arrow schema (%d): %v",
			len(extraInArrowSchema), extraInArrowSchema)
	}

	if len(missingFromArrowSchema) == 0 && len(extraInArrowSchema) == 0 {
		t.Logf("\n✅ Arrow and ParquetRaw schemas are identical")
	} else {
		t.Logf("\n❌ Schema differences detected")
	}
}
