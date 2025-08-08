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

package rawparquet

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/fileconv/translate"
)

func TestGetRow_ActualParquetFile(t *testing.T) {
	filePath := "testdata/logs_1752872650000_326740161.parquet"

	mapper := translate.NewMapper()

	reader, err := NewRawParquetReader(filePath, mapper, nil)
	assert.NoError(t, err)
	defer reader.Close()

	count := 0
	for {
		row, done, err := reader.GetRow()
		if err != nil {
			t.Fatalf("Error reading row: %v", err)
		}
		if done {
			break
		}
		assert.NotNil(t, row)
		count++
	}
	assert.Equal(t, 1026, count, "Expected to read 1026 rows from the parquet file")
}
