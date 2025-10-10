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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// ArrowCookedReader reads parquet files using Apache Arrow and returns raw rows.
type ArrowCookedReader struct {
	pr        *file.Reader
	fr        *pqarrow.FileReader
	rr        pqarrow.RecordReader
	batchSize int
	rowCount  int64
	closed    bool
	exhausted bool
}

// NewArrowCookedReader creates an ArrowCookedReader for the given parquet.ReaderAtSeeker.
// The caller is responsible for closing the underlying reader.
func NewArrowCookedReader(ctx context.Context, r parquet.ReaderAtSeeker, batchSize int) (*ArrowCookedReader, error) {
	pf, err := file.NewParquetReader(r)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	props := pqarrow.ArrowReadProperties{BatchSize: int64(batchSize)}
	fr, err := pqarrow.NewFileReader(pf, props, memory.DefaultAllocator)
	if err != nil {
		pf.Close()
		return nil, fmt.Errorf("failed to create arrow file reader: %w", err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		pf.Close()
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}

	return &ArrowCookedReader{
		pr:        pf,
		fr:        fr,
		rr:        rr,
		batchSize: batchSize,
	}, nil
}

// Next returns the next batch of rows from the parquet file.
func (r *ArrowCookedReader) Next(ctx context.Context) (*filereader.Batch, error) {
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

	batch := pipeline.GetBatch()
	fields := rec.Schema().Fields()
	numRows := int(rec.NumRows())

	for i := range numRows {
		br := batch.AddRow()
		for j, f := range fields {
			col := rec.Column(j)
			if col.IsNull(i) {
				br[wkk.NewRowKeyFromBytes([]byte(f.Name))] = nil
				continue
			}
			val := arrowValue(col, i)
			if val != nil {
				br[wkk.NewRowKeyFromBytes([]byte(f.Name))] = val
			}
		}
	}

	r.rowCount += int64(numRows)
	return batch, nil
}

// Close releases resources associated with the reader.
func (r *ArrowCookedReader) Close() error {
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
func (r *ArrowCookedReader) TotalRowsReturned() int64 { return r.rowCount }

// arrowValue converts an Arrow array value at index i to a Go interface{}.
func arrowValue(col arrow.Array, i int) any {
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
		return c.Value(i)
	case *array.LargeString:
		return c.Value(i)
	case *array.Binary:
		return c.Value(i)
	case *array.LargeBinary:
		return c.Value(i)
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
	default:
		return nil
	}
}
