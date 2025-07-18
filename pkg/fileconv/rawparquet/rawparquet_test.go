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
