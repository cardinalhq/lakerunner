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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// runAggregation simulates reading all rows from an AggregatingMetricsReader.
func runAggregation(t *testing.T) {
	t.Helper()

	rows := make([]Row, 200)
	baseTs := int64(1700000000000)
	for i := range rows {
		rows[i] = Row{
			wkk.RowKeyCName:       "metric",
			wkk.RowKeyCTID:        int64(1),
			wkk.RowKeyCTimestamp:  baseTs + int64(i*1000),
			wkk.RowKeyRollupSum:   float64(i),
			wkk.RowKeyRollupCount: int64(1),
			wkk.RowKeyRollupMin:   float64(i),
			wkk.RowKeyRollupMax:   float64(i),
			wkk.RowKeyCMetricType: "counter",
		}
	}

	mock := newMockReader("test", rows)
	reader, err := NewAggregatingMetricsReader(mock, 10000, 50)
	require.NoError(t, err)

	for {
		batch, err := reader.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		pipeline.ReturnBatch(batch)
	}
	require.NoError(t, reader.Close())
}

func TestAggregatingMetricsReader_BatchPoolStable(t *testing.T) {
	before := pipeline.GlobalBatchPoolStats()
	runAggregation(t)
	afterFirst := pipeline.GlobalBatchPoolStats()
	require.Equal(t, afterFirst.Gets-before.Gets, afterFirst.Puts-before.Puts, "all batches returned in first run")
	firstAllocs := afterFirst.Allocations - before.Allocations
	require.Greater(t, firstAllocs, uint64(0), "first run should allocate batches")

	runAggregation(t)
	afterSecond := pipeline.GlobalBatchPoolStats()
	require.Equal(t, afterSecond.Gets-afterFirst.Gets, afterSecond.Puts-afterFirst.Puts, "all batches returned in second run")
	secondAllocs := afterSecond.Allocations - afterFirst.Allocations

	// Allow for some variance in allocations due to global pool state and concurrent access.
	// The key requirement is that we don't have excessive growth in allocations.
	maxAllowedAllocs := firstAllocs + 2 // Allow up to 2 extra allocations for pool management overhead
	require.LessOrEqual(t, secondAllocs, maxAllowedAllocs, "second run should not allocate significantly more batches than first run")
}
