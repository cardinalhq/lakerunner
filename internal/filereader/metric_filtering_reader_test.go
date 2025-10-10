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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

type mockFilterReader struct {
	batches []*Batch
	index   int
	closed  bool
}

func (m *mockFilterReader) Next(ctx context.Context) (*Batch, error) {
	if m.index >= len(m.batches) {
		return nil, io.EOF
	}
	batch := m.batches[m.index]
	m.index++
	return batch, nil
}

func (m *mockFilterReader) Close() error {
	m.closed = true
	return nil
}

func (m *mockFilterReader) TotalRowsReturned() int64 {
	var total int64
	for i := 0; i < m.index; i++ {
		total += int64(m.batches[i].Len())
	}
	return total
}

func TestMetricFilteringReader_FiltersSingleMetric(t *testing.T) {
	// Create test data with multiple metrics
	batch1 := pipeline.GetBatch()
	row1 := batch1.AddRow()
	row1[wkk.RowKeyCName] = "api.requests"
	row1[wkk.RowKeyCTID] = "tid1"
	row1[wkk.RowKeyCTimestamp] = int64(1000)
	row1[wkk.RowKeyCValue] = 100.0

	row2 := batch1.AddRow()
	row2[wkk.RowKeyCName] = "api.requests"
	row2[wkk.RowKeyCTID] = "tid2"
	row2[wkk.RowKeyCTimestamp] = int64(2000)
	row2[wkk.RowKeyCValue] = 200.0

	row3 := batch1.AddRow()
	row3[wkk.RowKeyCName] = "cpu.usage"
	row3[wkk.RowKeyCTID] = "tid1"
	row3[wkk.RowKeyCTimestamp] = int64(3000)
	row3[wkk.RowKeyCValue] = 50.0

	mockReader := &mockFilterReader{
		batches: []*Batch{batch1},
	}

	// Create filtering reader for "api.requests"
	reader := NewMetricFilteringReader(mockReader, "api.requests")
	defer func() {
		_ = reader.Close()
	}()

	ctx := context.Background()

	// Read filtered data
	batch, err := reader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 2, batch.Len())

	// Verify first row
	row := batch.Get(0)
	assert.Equal(t, "api.requests", row[wkk.RowKeyCName])
	assert.Equal(t, "tid1", row[wkk.RowKeyCTID])

	// Verify second row
	row = batch.Get(1)
	assert.Equal(t, "api.requests", row[wkk.RowKeyCName])
	assert.Equal(t, "tid2", row[wkk.RowKeyCTID])

	pipeline.ReturnBatch(batch)

	// Next read should return EOF
	_, err = reader.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestMetricFilteringReader_EmptySource(t *testing.T) {
	mockReader := &mockFilterReader{
		batches: []*Batch{},
	}

	reader := NewMetricFilteringReader(mockReader, "api.requests")
	defer func() {
		_ = reader.Close()
	}()

	ctx := context.Background()

	// Should return EOF immediately
	_, err := reader.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestMetricFilteringReader_MetricNotFound(t *testing.T) {
	// Create test data without the target metric
	batch1 := pipeline.GetBatch()
	row1 := batch1.AddRow()
	row1[wkk.RowKeyCName] = "cpu.usage"
	row1[wkk.RowKeyCTID] = "tid1"
	row1[wkk.RowKeyCTimestamp] = int64(1000)
	row1[wkk.RowKeyCValue] = 50.0

	mockReader := &mockFilterReader{
		batches: []*Batch{batch1},
	}

	reader := NewMetricFilteringReader(mockReader, "api.requests")
	defer func() {
		_ = reader.Close()
	}()

	ctx := context.Background()

	// Should return EOF when metric not found
	_, err := reader.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestMetricFilteringReader_StopsAfterMetric(t *testing.T) {
	// Create sorted data where target metric is in the middle
	batch1 := pipeline.GetBatch()

	// Before target metric alphabetically
	row1 := batch1.AddRow()
	row1[wkk.RowKeyCName] = "aaa.metric"
	row1[wkk.RowKeyCTID] = "tid1"
	row1[wkk.RowKeyCTimestamp] = int64(1000)

	// Target metric
	row2 := batch1.AddRow()
	row2[wkk.RowKeyCName] = "bbb.metric"
	row2[wkk.RowKeyCTID] = "tid1"
	row2[wkk.RowKeyCTimestamp] = int64(2000)

	row3 := batch1.AddRow()
	row3[wkk.RowKeyCName] = "bbb.metric"
	row3[wkk.RowKeyCTID] = "tid2"
	row3[wkk.RowKeyCTimestamp] = int64(3000)

	// After target metric alphabetically
	row4 := batch1.AddRow()
	row4[wkk.RowKeyCName] = "ccc.metric"
	row4[wkk.RowKeyCTID] = "tid1"
	row4[wkk.RowKeyCTimestamp] = int64(4000)

	mockReader := &mockFilterReader{
		batches: []*Batch{batch1},
	}

	reader := NewMetricFilteringReader(mockReader, "bbb.metric")
	defer func() {
		_ = reader.Close()
	}()

	ctx := context.Background()

	// Should get only the target metric rows
	batch, err := reader.Next(ctx)
	require.NoError(t, err)
	require.NotNil(t, batch)
	assert.Equal(t, 2, batch.Len())

	// Verify we got the right rows
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		assert.Equal(t, "bbb.metric", row[wkk.RowKeyCName])
	}

	pipeline.ReturnBatch(batch)

	// Next read should return EOF
	_, err = reader.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestMetricFilteringReader_MultipleBatches(t *testing.T) {
	// Create multiple batches with target metric spread across them
	batch1 := pipeline.GetBatch()
	row1 := batch1.AddRow()
	row1[wkk.RowKeyCName] = "api.requests"
	row1[wkk.RowKeyCTID] = "tid1"
	row1[wkk.RowKeyCTimestamp] = int64(1000)

	batch2 := pipeline.GetBatch()
	row2 := batch2.AddRow()
	row2[wkk.RowKeyCName] = "api.requests"
	row2[wkk.RowKeyCTID] = "tid2"
	row2[wkk.RowKeyCTimestamp] = int64(2000)

	batch3 := pipeline.GetBatch()
	row3 := batch3.AddRow()
	row3[wkk.RowKeyCName] = "api.requests"
	row3[wkk.RowKeyCTID] = "tid3"
	row3[wkk.RowKeyCTimestamp] = int64(3000)

	// Add a different metric to stop
	row4 := batch3.AddRow()
	row4[wkk.RowKeyCName] = "cpu.usage"
	row4[wkk.RowKeyCTID] = "tid1"
	row4[wkk.RowKeyCTimestamp] = int64(4000)

	mockReader := &mockFilterReader{
		batches: []*Batch{batch1, batch2, batch3},
	}

	reader := NewMetricFilteringReader(mockReader, "api.requests")
	defer func() {
		_ = reader.Close()
	}()

	ctx := context.Background()

	totalRows := 0

	// Read all filtered data
	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		totalRows += batch.Len()

		// Verify all rows are the target metric
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			assert.Equal(t, "api.requests", row[wkk.RowKeyCName])
		}

		pipeline.ReturnBatch(batch)
	}

	assert.Equal(t, 3, totalRows)
	assert.Equal(t, int64(3), reader.TotalRowsReturned())
}
