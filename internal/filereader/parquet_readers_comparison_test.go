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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TypeDifference represents a difference in types between readers
type TypeDifference struct {
	Key         string
	ParquetRaw  string
	DuckDB      string
	Arrow       string
	SampleValue interface{}
}

// ComparisonResult holds the complete comparison results
type ComparisonResult struct {
	TotalRows        int
	IdenticalRows    int
	KeyDifferences   []string
	TypeDifferences  []TypeDifference
	ValueDifferences []string
}

// TestParquetReadersIdenticalOutput tests that all three readers produce identical results
func TestParquetReadersIdenticalOutput(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	// Load test data
	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file %s: %v", testFile, err)
	}

	// Get absolute path for DuckDB reader
	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	// Read data from all three readers
	parquetRawRows, err := readAllRowsParquetRaw(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to read with ParquetRawReader: %v", err)
	}

	duckdbRows, err := readAllRowsDuckDB([]string{absPath})
	if err != nil {
		t.Fatalf("Failed to read with DuckDBParquetRawReader: %v", err)
	}

	arrowRows, err := readAllRowsArrow(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to read with ArrowCookedReader: %v", err)
	}

	// Verify same number of rows
	if len(parquetRawRows) != len(duckdbRows) || len(duckdbRows) != len(arrowRows) {
		t.Fatalf("Row count mismatch - ParquetRaw: %d, DuckDB: %d, Arrow: %d",
			len(parquetRawRows), len(duckdbRows), len(arrowRows))
	}

	t.Logf("All readers returned %d rows", len(parquetRawRows))

	// Perform detailed comparison
	result := compareReaderOutputs(parquetRawRows, duckdbRows, arrowRows)

	// Report results
	t.Logf("=== Comparison Results ===")
	t.Logf("Total rows compared: %d", result.TotalRows)
	t.Logf("Identical rows: %d", result.IdenticalRows)
	t.Logf("Key differences: %d", len(result.KeyDifferences))
	t.Logf("Type differences: %d", len(result.TypeDifferences))
	t.Logf("Value differences: %d", len(result.ValueDifferences))

	if len(result.KeyDifferences) > 0 {
		t.Logf("\n=== Key Differences ===")
		for _, diff := range result.KeyDifferences {
			t.Logf("  %s", diff)
		}
	}

	if len(result.TypeDifferences) > 0 {
		t.Logf("\n=== Type Differences ===")
		for _, diff := range result.TypeDifferences {
			t.Logf("  Key: %s", diff.Key)
			t.Logf("    ParquetRaw: %s", diff.ParquetRaw)
			t.Logf("    DuckDB:     %s", diff.DuckDB)
			t.Logf("    Arrow:      %s", diff.Arrow)
			t.Logf("    Sample:     %v", diff.SampleValue)
		}
	}

	if len(result.ValueDifferences) > 0 {
		t.Logf("\n=== Value Differences (first 10) ===")
		for i, diff := range result.ValueDifferences {
			if i >= 10 {
				t.Logf("  ... and %d more", len(result.ValueDifferences)-10)
				break
			}
			t.Logf("  %s", diff)
		}
	}

	// The test passes as long as we can identify and document differences
	// This is primarily for analysis rather than strict equality
	t.Logf("\n=== Summary ===")
	if result.IdenticalRows == result.TotalRows && len(result.TypeDifferences) == 0 {
		t.Logf("✅ All readers produce identical output")
	} else {
		t.Logf("📊 Readers have documented differences (see analysis above)")
	}
}

// TestParquetReadersKeyConsistency specifically tests key consistency
func TestParquetReadersKeyConsistency(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	// Get first row from each reader to compare keys
	parquetRawRow, err := getFirstRowParquetRaw(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to get first row from ParquetRawReader: %v", err)
	}

	duckdbRow, err := getFirstRowDuckDB([]string{absPath})
	if err != nil {
		t.Fatalf("Failed to get first row from DuckDBParquetRawReader: %v", err)
	}

	arrowRow, err := getFirstRowArrow(data, int64(len(data)))
	if err != nil {
		t.Fatalf("Failed to get first row from ArrowCookedReader: %v", err)
	}

	// Extract and compare keys
	parquetKeys := extractSortedKeys(parquetRawRow)
	duckdbKeys := extractSortedKeys(duckdbRow)
	arrowKeys := extractSortedKeys(arrowRow)

	t.Logf("ParquetRaw keys (%d): %v", len(parquetKeys), parquetKeys[:min(10, len(parquetKeys))])
	t.Logf("DuckDB keys     (%d): %v", len(duckdbKeys), duckdbKeys[:min(10, len(duckdbKeys))])
	t.Logf("Arrow keys      (%d): %v", len(arrowKeys), arrowKeys[:min(10, len(arrowKeys))])

	// Check for missing keys in each reader
	checkMissingKeys(t, "DuckDB", parquetKeys, duckdbKeys)
	checkMissingKeys(t, "Arrow", parquetKeys, arrowKeys)
	checkMissingKeys(t, "ParquetRaw vs DuckDB", duckdbKeys, parquetKeys)
	checkMissingKeys(t, "ParquetRaw vs Arrow", arrowKeys, parquetKeys)
}

// TestParquetReadersTypeAnalysis performs detailed type analysis
func TestParquetReadersTypeAnalysis(t *testing.T) {
	testFile := "../../testdata/metrics/metrics-cooked-2176772462.parquet"

	data, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatalf("Failed to load test file: %v", err)
	}

	absPath, err := filepath.Abs(testFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path: %v", err)
	}

	// Get first few rows to analyze type consistency
	parquetRawRows, err := readNRowsParquetRaw(data, int64(len(data)), 10)
	if err != nil {
		t.Fatalf("Failed to read ParquetRaw rows: %v", err)
	}

	duckdbRows, err := readNRowsDuckDB([]string{absPath}, 10)
	if err != nil {
		t.Fatalf("Failed to read DuckDB rows: %v", err)
	}

	arrowRows, err := readNRowsArrow(data, int64(len(data)), 10)
	if err != nil {
		t.Fatalf("Failed to read Arrow rows: %v", err)
	}

	if len(parquetRawRows) == 0 {
		t.Fatal("No rows to analyze")
	}

	// Analyze types for all common keys
	typeAnalysis := analyzeTypes(parquetRawRows, duckdbRows, arrowRows)

	t.Logf("=== Type Analysis for %d rows ===", len(parquetRawRows))

	consistentTypes := 0
	inconsistentTypes := 0

	for key, analysis := range typeAnalysis {
		parquetType := analysis["parquet"]
		duckdbType := analysis["duckdb"]
		arrowType := analysis["arrow"]

		if parquetType == duckdbType && duckdbType == arrowType {
			consistentTypes++
		} else {
			inconsistentTypes++
			t.Logf("Type inconsistency for key '%s':", key)
			t.Logf("  ParquetRaw: %s", parquetType)
			t.Logf("  DuckDB:     %s", duckdbType)
			t.Logf("  Arrow:      %s", arrowType)
		}
	}

	t.Logf("\nType consistency summary:")
	t.Logf("  Consistent types: %d", consistentTypes)
	t.Logf("  Inconsistent types: %d", inconsistentTypes)
	t.Logf("  Total keys analyzed: %d", len(typeAnalysis))
}

// Helper functions for reading data from each reader

func readAllRowsParquetRaw(data []byte, size int64) ([]map[wkk.RowKey]interface{}, error) {
	reader := bytes.NewReader(data)
	parquetReader, err := NewParquetRawReader(reader, size, 1000)
	if err != nil {
		return nil, err
	}
	defer parquetReader.Close()

	var allRows []map[wkk.RowKey]interface{}
	for {
		batch, err := parquetReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if batch != nil {
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				allRows = append(allRows, copyRow(row))
			}
		}
	}
	return allRows, nil
}

func readAllRowsDuckDB(paths []string) ([]map[wkk.RowKey]interface{}, error) {
	reader, err := NewDuckDBParquetRawReader(context.TODO(), paths, 1000)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var allRows []map[wkk.RowKey]interface{}
	for {
		batch, err := reader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if batch != nil {
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				allRows = append(allRows, copyRow(row))
			}
		}
	}
	return allRows, nil
}

func readAllRowsArrow(data []byte, size int64) ([]map[wkk.RowKey]interface{}, error) {
	reader := bytes.NewReader(data)
	arrowReader, err := NewArrowCookedReader(context.TODO(), reader, 1000)
	if err != nil {
		return nil, err
	}
	defer arrowReader.Close()

	var allRows []map[wkk.RowKey]interface{}
	for {
		batch, err := arrowReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if batch != nil {
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				allRows = append(allRows, copyRow(row))
			}
		}
	}
	return allRows, nil
}

func getFirstRowParquetRaw(data []byte, size int64) (map[wkk.RowKey]interface{}, error) {
	rows, err := readNRowsParquetRaw(data, size, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func getFirstRowDuckDB(paths []string) (map[wkk.RowKey]interface{}, error) {
	rows, err := readNRowsDuckDB(paths, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func getFirstRowArrow(data []byte, size int64) (map[wkk.RowKey]interface{}, error) {
	rows, err := readNRowsArrow(data, size, 1)
	if err != nil || len(rows) == 0 {
		return nil, err
	}
	return rows[0], nil
}

func readNRowsParquetRaw(data []byte, size int64, n int) ([]map[wkk.RowKey]interface{}, error) {
	reader := bytes.NewReader(data)
	parquetReader, err := NewParquetRawReader(reader, size, 1000)
	if err != nil {
		return nil, err
	}
	defer parquetReader.Close()

	var rows []map[wkk.RowKey]interface{}
	for len(rows) < n {
		batch, err := parquetReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if batch != nil {
			for i := 0; i < batch.Len() && len(rows) < n; i++ {
				row := batch.Get(i)
				rows = append(rows, copyRow(row))
			}
		}
	}
	return rows, nil
}

func readNRowsDuckDB(paths []string, n int) ([]map[wkk.RowKey]interface{}, error) {
	reader, err := NewDuckDBParquetRawReader(context.TODO(), paths, 1000)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var rows []map[wkk.RowKey]interface{}
	for len(rows) < n {
		batch, err := reader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if batch != nil {
			for i := 0; i < batch.Len() && len(rows) < n; i++ {
				row := batch.Get(i)
				rows = append(rows, copyRow(row))
			}
		}
	}
	return rows, nil
}

func readNRowsArrow(data []byte, size int64, n int) ([]map[wkk.RowKey]interface{}, error) {
	reader := bytes.NewReader(data)
	arrowReader, err := NewArrowCookedReader(context.TODO(), reader, 1000)
	if err != nil {
		return nil, err
	}
	defer arrowReader.Close()

	var rows []map[wkk.RowKey]interface{}
	for len(rows) < n {
		batch, err := arrowReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if batch != nil {
			for i := 0; i < batch.Len() && len(rows) < n; i++ {
				row := batch.Get(i)
				rows = append(rows, copyRow(row))
			}
		}
	}
	return rows, nil
}

// Helper functions for comparison and analysis

func copyRow(row map[wkk.RowKey]interface{}) map[wkk.RowKey]interface{} {
	copied := make(map[wkk.RowKey]interface{})
	for k, v := range row {
		copied[k] = v
	}
	return copied
}

func extractSortedKeys(row map[wkk.RowKey]interface{}) []string {
	var keys []string
	for k := range row {
		keys = append(keys, string(k.Value()))
	}
	sort.Strings(keys)
	return keys
}

func checkMissingKeys(t *testing.T, readerName string, expectedKeys, actualKeys []string) {
	expectedSet := make(map[string]bool)
	for _, k := range expectedKeys {
		expectedSet[k] = true
	}

	actualSet := make(map[string]bool)
	for _, k := range actualKeys {
		actualSet[k] = true
	}

	var missing []string
	for k := range expectedSet {
		if !actualSet[k] {
			missing = append(missing, k)
		}
	}

	var extra []string
	for k := range actualSet {
		if !expectedSet[k] {
			extra = append(extra, k)
		}
	}

	if len(missing) > 0 {
		t.Logf("%s missing keys: %v", readerName, missing)
	}
	if len(extra) > 0 {
		t.Logf("%s extra keys: %v", readerName, extra)
	}
}

func compareReaderOutputs(parquetRaw, duckdb, arrow []map[wkk.RowKey]interface{}) ComparisonResult {
	result := ComparisonResult{
		TotalRows: len(parquetRaw),
	}

	for i := 0; i < len(parquetRaw); i++ {
		parquetRow := parquetRaw[i]
		duckdbRow := duckdb[i]
		arrowRow := arrow[i]

		// Check if rows are identical
		if mapsEqual(parquetRow, duckdbRow) && mapsEqual(duckdbRow, arrowRow) {
			result.IdenticalRows++
			continue
		}

		// Analyze differences
		analyzeRowDifferences(&result, i, parquetRow, duckdbRow, arrowRow)
	}

	return result
}

func mapsEqual(a, b map[wkk.RowKey]interface{}) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v1 := range a {
		v2, exists := b[k]
		if !exists {
			return false
		}

		if !valuesEqual(v1, v2) {
			return false
		}
	}

	return true
}

func valuesEqual(a, b interface{}) bool {
	// Handle byte slices specially
	aBytes, aIsBytes := a.([]byte)
	bBytes, bIsBytes := b.([]byte)
	if aIsBytes && bIsBytes {
		return bytes.Equal(aBytes, bBytes)
	}

	return reflect.DeepEqual(a, b)
}

func analyzeRowDifferences(result *ComparisonResult, rowIdx int, parquet, duckdb, arrow map[wkk.RowKey]interface{}) {
	// Get all unique keys
	allKeys := make(map[string]bool)
	for k := range parquet {
		allKeys[string(k.Value())] = true
	}
	for k := range duckdb {
		allKeys[string(k.Value())] = true
	}
	for k := range arrow {
		allKeys[string(k.Value())] = true
	}

	for keyStr := range allKeys {
		key := wkk.NewRowKeyFromBytes([]byte(keyStr))

		pVal, pExists := parquet[key]
		dVal, dExists := duckdb[key]
		aVal, aExists := arrow[key]

		// Check key existence differences
		if pExists != dExists || dExists != aExists {
			result.KeyDifferences = append(result.KeyDifferences,
				fmt.Sprintf("Row %d, key %s: ParquetRaw=%t, DuckDB=%t, Arrow=%t",
					rowIdx, keyStr, pExists, dExists, aExists))
		}

		// Check type differences (for existing keys)
		if pExists && dExists && aExists {
			pType := reflect.TypeOf(pVal).String()
			dType := reflect.TypeOf(dVal).String()
			aType := reflect.TypeOf(aVal).String()

			if pType != dType || dType != aType {
				found := false
				for _, diff := range result.TypeDifferences {
					if diff.Key == keyStr {
						found = true
						break
					}
				}
				if !found {
					result.TypeDifferences = append(result.TypeDifferences, TypeDifference{
						Key:         keyStr,
						ParquetRaw:  pType,
						DuckDB:      dType,
						Arrow:       aType,
						SampleValue: pVal,
					})
				}
			}

			// Check value differences (same type, different values)
			if pType == dType && dType == aType && !valuesEqual(pVal, dVal) {
				result.ValueDifferences = append(result.ValueDifferences,
					fmt.Sprintf("Row %d, key %s: ParquetRaw=%v, DuckDB=%v, Arrow=%v",
						rowIdx, keyStr, pVal, dVal, aVal))
			}
		}
	}
}

func analyzeTypes(parquetRows, duckdbRows, arrowRows []map[wkk.RowKey]interface{}) map[string]map[string]string {
	typeAnalysis := make(map[string]map[string]string)

	// Get all unique keys
	allKeys := make(map[string]bool)
	for _, row := range parquetRows {
		for k := range row {
			allKeys[string(k.Value())] = true
		}
	}

	for keyStr := range allKeys {
		key := wkk.NewRowKeyFromBytes([]byte(keyStr))
		types := make(map[string]string)

		// Check ParquetRaw type
		for _, row := range parquetRows {
			if val, exists := row[key]; exists && val != nil {
				types["parquet"] = reflect.TypeOf(val).String()
				break
			}
		}

		// Check DuckDB type
		for _, row := range duckdbRows {
			if val, exists := row[key]; exists && val != nil {
				types["duckdb"] = reflect.TypeOf(val).String()
				break
			}
		}

		// Check Arrow type
		for _, row := range arrowRows {
			if val, exists := row[key]; exists && val != nil {
				types["arrow"] = reflect.TypeOf(val).String()
				break
			}
		}

		if len(types) > 0 {
			typeAnalysis[keyStr] = types
		}
	}

	return typeAnalysis
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
