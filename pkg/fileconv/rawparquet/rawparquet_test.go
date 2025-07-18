// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rawparquet

import (
	"testing"

	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
	"github.com/stretchr/testify/assert"
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
	assert.Equal(t, 1025, count, "Expected to read 1000 rows from the parquet file")
}
