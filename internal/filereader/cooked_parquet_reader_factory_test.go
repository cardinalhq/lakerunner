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
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestNewCookedMetricParquetReader(t *testing.T) {
	filename := "../../testdata/metrics/compact-test-0001/tbl_299476441865651503.parquet"
	file, err := os.Open(filename)
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewCookedMetricParquetReader(file, stat.Size(), 1000)
	require.NoError(t, err)
	defer reader.Close()

	var count int64
	for {
		batch, err := reader.Next(context.Background())
		if batch != nil {
			count += int64(batch.Len())
		}
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// One NaN row should be dropped by the translating reader
	assert.Equal(t, int64(226), count)
}

func TestNewCookedLogParquetReader(t *testing.T) {
	filename := "../../testdata/logs/logs-cooked-0001.parquet"
	file, err := os.Open(filename)
	require.NoError(t, err)
	defer file.Close()

	stat, err := file.Stat()
	require.NoError(t, err)

	reader, err := NewCookedLogParquetReader(file, stat.Size(), 1000)
	require.NoError(t, err)
	defer reader.Close()

	batch, err := reader.Next(context.Background())
	require.NoError(t, err)
	require.NotNil(t, batch)

	found := false
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		if tidVal, ok := row[wkk.RowKeyCTID]; ok {
			_, isInt64 := tidVal.(int64)
			assert.True(t, isInt64)
			found = true
			break
		}
	}
	assert.True(t, found, "expected row with _cardinalhq.tid")
}
