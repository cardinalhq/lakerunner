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

// ArrowRawReader reads parquet files using Apache Arrow and returns raw rows.
// This reader provides raw parquet data without any opinionated transformations.
// It handles NULL-type columns gracefully, unlike the parquet-go library.
type ArrowRawReader struct {
	pr           *file.Reader
	fr           *pqarrow.FileReader
	rr           pqarrow.RecordReader
	batchSize    int
	rowCount     int64
	closed       bool
	exhausted    bool
	readerSchema *ReaderSchema // our schema extracted from Arrow schema
}

var _ Reader = (*ArrowRawReader)(nil)
var _ Reader = (*ArrowRawReader)(nil)

// NewArrowRawReader creates an ArrowRawReader for the given parquet.ReaderAtSeeker.
func NewArrowRawReader(ctx context.Context, reader parquet.ReaderAtSeeker, batchSize int) (*ArrowRawReader, error) {
	pf, err := file.NewParquetReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	if batchSize <= 0 {
		batchSize = 1000
	}

	props := pqarrow.ArrowReadProperties{BatchSize: int64(batchSize)}
	fr, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("failed to create arrow file reader: %w", err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	// Extract schema from Arrow metadata
	arrowSchema, err := fr.Schema()
	if err != nil {
		_ = pf.Close()
		return nil, fmt.Errorf("failed to get arrow schema: %w", err)
	}
	readerSchema := extractSchemaFromArrowSchema(arrowSchema)

	return &ArrowRawReader{
		pr:           pf,
		fr:           fr,
		rr:           rr,
		batchSize:    batchSize,
		readerSchema: readerSchema,
	}, nil
}

// Next returns the next batch of rows from the parquet file.
func (r *ArrowRawReader) Next(ctx context.Context) (*Batch, error) {
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
		attribute.String("reader", "ArrowRawReader"),
	))

	batch := pipeline.GetBatch()

	for i := range numRows {
		br := batch.AddRow()
		for j, f := range fields {
			col := rec.Column(j)
			if !col.IsNull(i) {
				val := convertArrowValue(col, i)
				if val != nil {
					br[wkk.NewRowKeyFromBytes([]byte(f.Name))] = val
				}
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

// GetSchema returns the schema extracted from the Arrow metadata.
func (r *ArrowRawReader) GetSchema() *ReaderSchema {
	return r.readerSchema
}

// Close releases resources associated with the reader.
func (r *ArrowRawReader) Close() error {
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
func (r *ArrowRawReader) TotalRowsReturned() int64 {
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
