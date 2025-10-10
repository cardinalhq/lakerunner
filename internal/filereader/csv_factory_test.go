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
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestReaderForFileWithOptions_CSV(t *testing.T) {
	ctx := context.Background()

	// Create temp directory
	tempDir := t.TempDir()

	// Test plain CSV
	t.Run("plain CSV", func(t *testing.T) {
		csvPath := filepath.Join(tempDir, "test.csv")
		csvContent := `timestamp,level,data
1758397185000,INFO,Test message 1
1758397186000,ERROR,Test message 2`

		err := os.WriteFile(csvPath, []byte(csvContent), 0644)
		require.NoError(t, err)

		opts := ReaderOptions{
			SignalType: SignalTypeLogs,
			BatchSize:  10,
			OrgID:      "test-org",
			Bucket:     "test-bucket",
			ObjectID:   "test.csv",
		}

		reader, err := ReaderForFileWithOptions(csvPath, opts)
		require.NoError(t, err)
		defer func() {
			_ = reader.Close()
		}()

		// Read batch
		batch, err := reader.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, batch)
		assert.Equal(t, 2, batch.Len())

		// Check first row was translated properly for logs
		row := batch.Get(0)
		assert.Equal(t, "Test message 1", row[wkk.RowKeyCMessage])
		assert.Equal(t, int64(1758397185000), row[wkk.RowKeyCTimestamp])
		assert.Equal(t, "INFO", row[wkk.NewRowKey("log_level")])
	})

	// Test gzipped CSV
	t.Run("gzipped CSV", func(t *testing.T) {
		csvGzPath := filepath.Join(tempDir, "test.csv.gz")
		csvContent := `id,value,name
1,100,Alice
2,200,Bob`

		// Create gzipped file
		file, err := os.Create(csvGzPath)
		require.NoError(t, err)
		gzWriter := gzip.NewWriter(file)
		_, err = gzWriter.Write([]byte(csvContent))
		require.NoError(t, err)
		require.NoError(t, gzWriter.Close())
		require.NoError(t, file.Close())

		opts := ReaderOptions{
			SignalType: SignalTypeMetrics, // Test non-logs type
			BatchSize:  10,
		}

		reader, err := ReaderForFileWithOptions(csvGzPath, opts)
		require.NoError(t, err)
		defer func() {
			_ = reader.Close()
		}()

		// Read batch
		batch, err := reader.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, batch)
		assert.Equal(t, 2, batch.Len())

		// Check data was read correctly (no translation for metrics)
		row := batch.Get(0)
		assert.Equal(t, int64(1), row[wkk.NewRowKey("id")])
		assert.Equal(t, int64(100), row[wkk.NewRowKey("value")])
		assert.Equal(t, "Alice", row[wkk.NewRowKey("name")])
	})

	// Test CSV with logs translation
	t.Run("CSV with logs translation", func(t *testing.T) {
		csvPath := filepath.Join(tempDir, "logs.csv")
		csvContent := `subscription_name,message_id,publish_time,data,attributes
projects/test/sub,123,2025-09-20T19:39:45.000Z,"{""event"":""test"",""value"":42}",{}`

		err := os.WriteFile(csvPath, []byte(csvContent), 0644)
		require.NoError(t, err)

		opts := ReaderOptions{
			SignalType: SignalTypeLogs,
			BatchSize:  10,
			OrgID:      "test-org",
			Bucket:     "test-bucket",
			ObjectID:   "logs.csv",
		}

		reader, err := ReaderForFileWithOptions(csvPath, opts)
		require.NoError(t, err)
		defer func() {
			_ = reader.Close()
		}()

		// Read batch
		batch, err := reader.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, batch)
		assert.Equal(t, 1, batch.Len())

		// Check translation
		row := batch.Get(0)
		// data field should become message
		assert.Equal(t, `{"event":"test","value":42}`, row[wkk.RowKeyCMessage])
		// Fields should be mapped to log.* namespace
		assert.Equal(t, "projects/test/sub", row[wkk.NewRowKey("log_subscription_name")])
		assert.Equal(t, int64(123), row[wkk.NewRowKey("log_message_id")])
		// Resource fields
		assert.Equal(t, "test-bucket", row[wkk.RowKeyResourceBucketName])
		assert.Equal(t, "./logs.csv", row[wkk.RowKeyResourceFileName])
		assert.Equal(t, "logs", row[wkk.RowKeyResourceFileType]) // GetFileType returns filename without extension
	})
}

func TestReaderForFileWithOptions_UnsupportedFile(t *testing.T) {
	opts := ReaderOptions{
		SignalType: SignalTypeLogs,
		BatchSize:  10,
	}

	reader, err := ReaderForFileWithOptions("test.xyz", opts)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported file type")
	assert.Nil(t, reader)
}
