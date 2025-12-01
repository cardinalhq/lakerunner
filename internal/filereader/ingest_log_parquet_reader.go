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
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// IngestLogParquetReader reads parquet files using Apache Arrow for log ingestion.
// This reader performs two-pass reading:
// 1. First pass: Scan the file to extract schema with flattening
// 2. Second pass: Read rows using the extracted schema with flattening applied
//
// It handles NULL-type columns gracefully and flattens nested structures (maps, structs, lists).
type IngestLogParquetReader struct {
	reader       parquet.ReaderAtSeeker
	batchSize    int
	rowCount     int64
	closed       bool
	exhausted    bool
	readerSchema *ReaderSchema // schema extracted with flattening applied

	// Second pass reader state
	pr *file.Reader
	fr *pqarrow.FileReader
	rr pqarrow.RecordReader
}

var _ Reader = (*IngestLogParquetReader)(nil)

// NewIngestLogParquetReader creates an IngestLogParquetReader for the given parquet.ReaderAtSeeker.
// It performs a first pass to extract the schema with flattening, then prepares for reading rows.
func NewIngestLogParquetReader(ctx context.Context, reader parquet.ReaderAtSeeker, batchSize int) (*IngestLogParquetReader, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	// Open parquet file
	pf, err := file.NewParquetReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	props := pqarrow.ArrowReadProperties{BatchSize: int64(batchSize)}
	fr, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("failed to create arrow file reader: %w", err)
	}

	// Extract complete schema by scanning all rows (two-pass approach)
	// This discovers all map keys and nested structures across the entire file
	readerSchema, err := ExtractCompleteParquetSchema(ctx, pf, fr)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("failed to extract schema: %w", err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	return &IngestLogParquetReader{
		reader:       reader,
		batchSize:    batchSize,
		readerSchema: readerSchema,
		pr:           pf,
		fr:           fr,
		rr:           rr,
	}, nil
}

// Next returns the next batch of rows from the parquet file with flattening applied.
func (r *IngestLogParquetReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, errors.New("reader is closed")
	}
	if r.exhausted {
		return nil, io.EOF
	}

	rec, err := r.rr.Read()
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.exhausted = true
			return nil, io.EOF
		}
		return nil, fmt.Errorf("arrow read error: %w", err)
	}
	if rec == nil || rec.NumRows() == 0 {
		r.exhausted = true
		return nil, io.EOF
	}
	defer rec.Release()

	fields := rec.Schema().Fields()
	numRows := int(rec.NumRows())

	// Track rows read
	rowsInCounter.Add(ctx, int64(numRows), otelmetric.WithAttributes(
		attribute.String("reader", "IngestLogParquetReader"),
	))

	batch := pipeline.GetBatch()

	for i := range numRows {
		br := batch.AddRow()
		for j, f := range fields {
			col := rec.Column(j)
			if !col.IsNull(i) {
				// Flatten the value into the row, tracking both dotted and underscored paths
				// dottedPath is the original path (e.g., "foo.bar.whatever")
				// underscoredPath is the flattened path (e.g., "foo_bar_whatever")
				flattenValueIntoRow(br, f.Name, f.Name, col, i, r.readerSchema)
			}
		}
		// Apply schema normalization
		if err := normalizeRow(ctx, br, r.readerSchema); err != nil {
			return nil, fmt.Errorf("schema normalization failed: %w", err)
		}
	}

	r.rowCount += int64(numRows)
	return batch, nil
}

// flattenValueIntoRow flattens a value from the Arrow array into the row.
// dottedPath tracks the original dotted path (e.g., "foo.bar.whatever")
// underscoredPath tracks the flattened underscored path (e.g., "foo_bar_whatever")
// schema is updated with mappings as new fields are discovered
func flattenValueIntoRow(row pipeline.Row, dottedPath, underscoredPath string, col arrow.Array, i int, schema *ReaderSchema) {
	switch c := col.(type) {
	case *array.Struct:
		// Flatten struct fields
		dt := c.DataType().(*arrow.StructType)
		fields := dt.Fields()
		for j, field := range fields {
			nestedCol := c.Field(j)
			if !nestedCol.IsNull(i) {
				// Build nested paths: append field name to both paths
				nestedDotted := dottedPath + "." + field.Name
				nestedUnderscored := underscoredPath + "_" + field.Name
				flattenValueIntoRow(row, nestedDotted, nestedUnderscored, nestedCol, i, schema)
			}
		}

	case *array.Map:
		// Flatten map entries
		if !c.IsNull(i) {
			start, end := c.ValueOffsets(i)
			keys := c.Keys()
			items := c.Items()

			for j := start; j < end; j++ {
				key := convertArrowValue(keys, int(j))
				value := convertArrowValue(items, int(j))
				if keyStr, ok := key.(string); ok {
					// Build nested paths: append map key to both paths
					nestedUnderscored := underscoredPath + "_" + keyStr
					// Store the value
					finalUnderscored := strings.ReplaceAll(nestedUnderscored, ".", "_")
					rowKey := wkk.NewRowKeyFromBytes([]byte(finalUnderscored))
					row[rowKey] = value
					// Schema already complete from two-pass scan
				}
			}
		}

	case *array.List:
		// Lists are NOT flattened - store as-is (as []any)
		val := convertListValue(c, i)
		if val != nil {
			finalUnderscored := strings.ReplaceAll(underscoredPath, ".", "_")
			rowKey := wkk.NewRowKeyFromBytes([]byte(finalUnderscored))
			row[rowKey] = val
			// Schema already complete from two-pass scan
		}

	default:
		// Leaf value - convert and store
		val := convertArrowValue(col, i)
		if val != nil {
			finalUnderscored := strings.ReplaceAll(underscoredPath, ".", "_")
			rowKey := wkk.NewRowKeyFromBytes([]byte(finalUnderscored))
			row[rowKey] = val
			// Schema already complete from two-pass scan
		}
	}
}

// GetSchema returns a copy of the schema extracted from the Arrow metadata with flattening.
// Returns a copy to prevent external mutation of the internal schema.
func (r *IngestLogParquetReader) GetSchema() *ReaderSchema {
	return r.readerSchema.Copy()
}

// Close releases resources associated with the reader.
func (r *IngestLogParquetReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.rr != nil {
		r.rr.Release()
		r.rr = nil
	}
	if r.fr != nil {
		r.fr = nil
	}
	if r.pr != nil {
		if err := r.pr.Close(); err != nil {
			return err
		}
		r.pr = nil
	}
	return nil
}

// TotalRowsReturned returns the total number of rows successfully returned.
func (r *IngestLogParquetReader) TotalRowsReturned() int64 {
	return r.rowCount
}

// convertArrowValue converts an Arrow array value at index i to a Go interface{}.
func convertArrowValue(col arrow.Array, i int) any {
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(i)
	case *array.Int8:
		return int64(c.Value(i))
	case *array.Int16:
		return int64(c.Value(i))
	case *array.Int32:
		return int64(c.Value(i))
	case *array.Int64:
		return c.Value(i)
	case *array.Uint8:
		return uint64(c.Value(i))
	case *array.Uint16:
		return uint64(c.Value(i))
	case *array.Uint32:
		return uint64(c.Value(i))
	case *array.Uint64:
		return c.Value(i)
	case *array.Float32:
		return float64(c.Value(i))
	case *array.Float64:
		return c.Value(i)
	case *array.String:
		// Copy the string to avoid holding reference to Arrow buffer memory
		return strings.Clone(c.Value(i))
	case *array.LargeString:
		// Copy the string to avoid holding reference to Arrow buffer memory
		return strings.Clone(c.Value(i))
	case *array.Binary:
		// Copy the bytes to avoid holding reference to Arrow buffer memory
		b := c.Value(i)
		copied := make([]byte, len(b))
		copy(copied, b)
		return copied
	case *array.LargeBinary:
		// Copy the bytes to avoid holding reference to Arrow buffer memory
		b := c.Value(i)
		copied := make([]byte, len(b))
		copy(copied, b)
		return copied
	case *array.Timestamp:
		tsType := c.DataType().(*arrow.TimestampType)
		ts := c.Value(i)
		switch tsType.Unit {
		case arrow.Second:
			ts *= 1000
		case arrow.Millisecond:
			// already ms
		case arrow.Microsecond:
			ts /= 1000
		case arrow.Nanosecond:
			ts /= 1000000
		}
		return ts
	case *array.List:
		return convertListValue(c, i)
	case *array.Struct:
		return convertStructValue(c, i)
	case *array.Map:
		return convertMapValue(c, i)
	case *array.Null:
		// NULL columns (all values are null) - return nil
		return nil
	default:
		// For unknown types, try to get string representation
		return fmt.Sprintf("%v", c.ValueStr(i))
	}
}

// convertListValue converts a List array value to a Go slice.
func convertListValue(arr *array.List, i int) any {
	if arr.IsNull(i) {
		return nil
	}

	start, end := arr.ValueOffsets(i)
	values := arr.ListValues()

	result := make([]any, 0, end-start)
	for j := start; j < end; j++ {
		if values.IsNull(int(j)) {
			result = append(result, nil)
		} else {
			result = append(result, convertArrowValue(values, int(j)))
		}
	}
	return result
}

// convertStructValue converts a Struct array value to a Go map.
func convertStructValue(arr *array.Struct, i int) any {
	if arr.IsNull(i) {
		return nil
	}

	dt := arr.DataType().(*arrow.StructType)
	fields := dt.Fields()

	result := make(map[string]any)
	for j, field := range fields {
		col := arr.Field(j)
		if !col.IsNull(i) {
			result[field.Name] = convertArrowValue(col, i)
		}
	}
	return result
}

// convertMapValue converts a Map array value to a Go map.
func convertMapValue(arr *array.Map, i int) any {
	if arr.IsNull(i) {
		return nil
	}

	start, end := arr.ValueOffsets(i)
	keys := arr.Keys()
	items := arr.Items()

	result := make(map[string]any)
	for j := start; j < end; j++ {
		key := convertArrowValue(keys, int(j))
		value := convertArrowValue(items, int(j))
		if keyStr, ok := key.(string); ok {
			result[keyStr] = value
		} else {
			result[fmt.Sprintf("%v", key)] = value
		}
	}
	return result
}
