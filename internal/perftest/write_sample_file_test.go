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

package perftest

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/testdata"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// TestWriteSampleFiles writes sample Parquet files with different chunk sizes
// to /tmp for manual inspection using synthetic data
func TestWriteSampleFiles(t *testing.T) {
	ctx := context.Background()

	// Generate 400K rows of synthetic data
	numRows := 400000
	batchSize := 1000
	numBatches := numRows / batchSize

	var batches []*pipeline.Batch
	for i := 0; i < numBatches; i++ {
		batch := testdata.GenerateLogBatch(batchSize, i*batchSize)
		batches = append(batches, batch)
	}

	totalRows := int64(numRows)
	t.Logf("Generated %d batches (%d rows) for sample file generation", len(batches), totalRows)

	// Write files with different chunk sizes
	chunkSizes := []int64{10000, 25000, 50000}

	for _, chunkSize := range chunkSizes {
		tmpDir := t.TempDir()

		config := parquetwriter.BackendConfig{
			Type:      parquetwriter.BackendArrow,
			TmpDir:    tmpDir,
			ChunkSize: chunkSize,
			StringConversionPrefixes: []string{
				"resource_",
				"scope_",
				"attr_",
			},
		}

		backend, err := parquetwriter.NewArrowBackend(config)
		if err != nil {
			t.Fatal(err)
		}

		for _, batch := range batches {
			if err := backend.WriteBatch(ctx, batch); err != nil {
				backend.Abort()
				t.Fatal(err)
			}
		}

		outputPath := filepath.Join(tmpDir, "output.parquet")
		outputFile, err := os.Create(outputPath)
		if err != nil {
			backend.Abort()
			t.Fatal(err)
		}

		_, err = backend.Close(ctx, outputFile)
		if err != nil {
			_ = outputFile.Close()
			t.Fatal(err)
		}
		_ = outputFile.Close()

		// Copy to /tmp with descriptive name
		destPath := fmt.Sprintf("/tmp/arrow-chunk-%dk.parquet", chunkSize/1000)
		src, _ := os.Open(outputPath)
		dst, _ := os.Create(destPath)
		_, _ = io.Copy(dst, src)
		_ = src.Close()
		_ = dst.Close()

		stat, _ := os.Stat(destPath)
		t.Logf("Wrote %s (%d bytes, %d rows)", destPath, stat.Size(), totalRows)
	}

	t.Logf("\nSample files written to /tmp/arrow-chunk-*.parquet")
	t.Logf("Inspect with: parquet-tools meta /tmp/arrow-chunk-50k.parquet")
}
