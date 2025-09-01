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
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TestArrowCookedReaderWithRealFile verifies the reader can stream rows from a parquet file.
func TestArrowCookedReaderWithRealFile(t *testing.T) {
	file, err := os.Open("../../testdata/metrics/metrics-cooked-0001.parquet")
	require.NoError(t, err)
	defer file.Close()

	reader, err := NewArrowCookedReader(context.TODO(), file, 1000)
	require.NoError(t, err)
	defer reader.Close()

	var count int64
	checkedTimestamp := false
	for {
		batch, err := reader.Next(context.TODO())
		if batch != nil {
			if !checkedTimestamp && batch.Len() > 0 {
				_, ok := batch.Get(0)[wkk.RowKeyCTimestamp].(int64)
				assert.True(t, ok, "_cardinalhq.timestamp should be int64")
				checkedTimestamp = true
			}
			count += int64(batch.Len())
		}
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
	}

	assert.Equal(t, int64(211), count)
}
