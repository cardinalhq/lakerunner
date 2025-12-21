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
	"io"
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// benchSortKey implements SortKey for benchmarking
type benchSortKey struct {
	ts int64
}

func (k *benchSortKey) Compare(other SortKey) int {
	otherKey := other.(*benchSortKey)
	if k.ts < otherKey.ts {
		return -1
	}
	if k.ts > otherKey.ts {
		return 1
	}
	return 0
}

func (k *benchSortKey) Release() {}

// benchSortKeyProvider implements SortKeyProvider for benchmarking
type benchSortKeyProvider struct{}

func (p *benchSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	ts, _ := row.GetInt64(wkk.NewRowKey("timestamp"))
	return &benchSortKey{ts: ts}
}

// benchmarkReader generates rows for benchmarking
type benchmarkReader struct {
	schema    *ReaderSchema
	rowsLeft  int
	batchSize int
	startTs   int64
}

func newBenchmarkReader(numRows, batchSize int, startTs int64) *benchmarkReader {
	schema := NewReaderSchema()
	schema.AddColumn(wkk.NewRowKey("timestamp"), wkk.NewRowKey("timestamp"), DataTypeInt64, true)
	schema.AddColumn(wkk.NewRowKey("value"), wkk.NewRowKey("value"), DataTypeFloat64, true)
	schema.AddColumn(wkk.NewRowKey("metric_name"), wkk.NewRowKey("metric_name"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("service"), wkk.NewRowKey("service"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("host"), wkk.NewRowKey("host"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("env"), wkk.NewRowKey("env"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("region"), wkk.NewRowKey("region"), DataTypeString, true)
	schema.AddColumn(wkk.NewRowKey("cluster"), wkk.NewRowKey("cluster"), DataTypeString, true)

	return &benchmarkReader{
		schema:    schema,
		rowsLeft:  numRows,
		batchSize: batchSize,
		startTs:   startTs,
	}
}

func (r *benchmarkReader) Next(ctx context.Context) (*Batch, error) {
	if r.rowsLeft <= 0 {
		return nil, io.EOF
	}

	batch := pipeline.GetBatch()
	toRead := r.batchSize
	if toRead > r.rowsLeft {
		toRead = r.rowsLeft
	}

	tsKey := wkk.NewRowKey("timestamp")
	valueKey := wkk.NewRowKey("value")
	metricKey := wkk.NewRowKey("metric_name")
	serviceKey := wkk.NewRowKey("service")
	hostKey := wkk.NewRowKey("host")
	envKey := wkk.NewRowKey("env")
	regionKey := wkk.NewRowKey("region")
	clusterKey := wkk.NewRowKey("cluster")

	for i := 0; i < toRead; i++ {
		row := batch.AddRow()
		row[tsKey] = r.startTs
		row[valueKey] = float64(42.5)
		row[metricKey] = "http_requests_total"
		row[serviceKey] = "api-gateway"
		row[hostKey] = "prod-node-001"
		row[envKey] = "production"
		row[regionKey] = "us-west-2"
		row[clusterKey] = "prod-cluster-01"
		r.startTs += 1000 // Increment timestamp to maintain sort order
	}

	r.rowsLeft -= toRead
	return batch, nil
}

func (r *benchmarkReader) Close() error {
	return nil
}

func (r *benchmarkReader) GetSchema() *ReaderSchema {
	return r.schema
}

func (r *benchmarkReader) TotalRowsReturned() int64 {
	return 0
}

// BenchmarkMergesortReader_SingleReader benchmarks mergesort with a single reader
func BenchmarkMergesortReader_SingleReader(b *testing.B) {
	ctx := context.Background()
	keyProvider := &benchSortKeyProvider{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader := newBenchmarkReader(1000, 100, 0)
		mr, err := NewMergesortReader(ctx, []Reader{reader}, keyProvider, 100)
		if err != nil {
			b.Fatal(err)
		}

		rowCount := 0
		for {
			batch, err := mr.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			rowCount += batch.Len()
			pipeline.ReturnBatch(batch)
		}

		if rowCount != 1000 {
			b.Fatalf("expected 1000 rows, got %d", rowCount)
		}

		_ = mr.Close()
	}
}

// BenchmarkMergesortReader_TwoReaders benchmarks mergesort with two interleaved readers
func BenchmarkMergesortReader_TwoReaders(b *testing.B) {
	ctx := context.Background()
	keyProvider := &benchSortKeyProvider{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Two readers with interleaved timestamps
		reader1 := newBenchmarkReader(500, 100, 0)    // 0, 2000, 4000, ...
		reader2 := newBenchmarkReader(500, 100, 1000) // 1000, 3000, 5000, ...
		reader1.startTs = 0
		reader2.startTs = 1000

		// Adjust increment to interleave
		mr, err := NewMergesortReader(ctx, []Reader{reader1, reader2}, keyProvider, 100)
		if err != nil {
			b.Fatal(err)
		}

		rowCount := 0
		for {
			batch, err := mr.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			rowCount += batch.Len()
			pipeline.ReturnBatch(batch)
		}

		if rowCount != 1000 {
			b.Fatalf("expected 1000 rows, got %d", rowCount)
		}

		_ = mr.Close()
	}
}

// BenchmarkMergesortReader_FiveReaders benchmarks mergesort with five readers
func BenchmarkMergesortReader_FiveReaders(b *testing.B) {
	ctx := context.Background()
	keyProvider := &benchSortKeyProvider{}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		readers := make([]Reader, 5)
		for j := 0; j < 5; j++ {
			readers[j] = newBenchmarkReader(200, 100, int64(j*200))
		}

		mr, err := NewMergesortReader(ctx, readers, keyProvider, 100)
		if err != nil {
			b.Fatal(err)
		}

		rowCount := 0
		for {
			batch, err := mr.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			rowCount += batch.Len()
			pipeline.ReturnBatch(batch)
		}

		if rowCount != 1000 {
			b.Fatalf("expected 1000 rows, got %d", rowCount)
		}

		_ = mr.Close()
	}
}
