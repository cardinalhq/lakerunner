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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
)

// TestMergesortReader_WithRealParquetFiles tests the mergesort reader with real parquet files.
// This test uses parquet files from /tmp/av2 if they exist, otherwise skips.
func TestMergesortReader_WithRealParquetFiles(t *testing.T) {
	parquetDir := "/tmp/av2"

	// Check if directory exists
	if _, err := os.Stat(parquetDir); os.IsNotExist(err) {
		t.Skipf("Test parquet directory %s does not exist, skipping", parquetDir)
	}

	// Find all parquet files
	files, err := filepath.Glob(filepath.Join(parquetDir, "*.parquet"))
	require.NoError(t, err)
	if len(files) == 0 {
		t.Skipf("No parquet files found in %s, skipping", parquetDir)
	}

	ctx := context.Background()

	// Create readers for each file
	var readers []Reader
	var openFiles []*os.File
	for _, f := range files {
		file, err := os.Open(f)
		require.NoError(t, err, "Failed to open %s", f)
		openFiles = append(openFiles, file)

		reader, err := NewIngestLogParquetReader(ctx, file, 1000)
		require.NoError(t, err, "Failed to create reader for %s", f)
		readers = append(readers, reader)
	}
	defer func() {
		for _, f := range openFiles {
			_ = f.Close()
		}
	}()

	// Create mergesort reader
	keyProvider := NewTimeOrderedSortKeyProvider("timestamp")
	mergeReader, err := NewMergesortReader(ctx, readers, keyProvider, 1000)
	require.NoError(t, err, "Failed to create mergesort reader")
	defer func() { _ = mergeReader.Close() }()

	// Read all rows
	var totalRows int64
	for {
		batch, err := mergeReader.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "Error reading batch")
		totalRows += int64(batch.Len())
		pipeline.ReturnBatch(batch)
	}

	t.Logf("Successfully read %d rows from %d files", totalRows, len(files))
}

// TestMergesortReader_WithRealParquetFiles_SingleFile tests reading a single parquet file
// to verify the reader works before testing mergesort.
func TestMergesortReader_WithRealParquetFiles_SingleFile(t *testing.T) {
	parquetDir := "/tmp/av2"

	// Check if directory exists
	if _, err := os.Stat(parquetDir); os.IsNotExist(err) {
		t.Skipf("Test parquet directory %s does not exist, skipping", parquetDir)
	}

	// Find all parquet files
	files, err := filepath.Glob(filepath.Join(parquetDir, "*.parquet"))
	require.NoError(t, err)
	if len(files) == 0 {
		t.Skipf("No parquet files found in %s, skipping", parquetDir)
	}

	ctx := context.Background()

	// Test each file individually
	for _, f := range files {
		t.Run(filepath.Base(f), func(t *testing.T) {
			file, err := os.Open(f)
			require.NoError(t, err, "Failed to open %s", f)
			defer func() { _ = file.Close() }()

			reader, err := NewIngestLogParquetReader(ctx, file, 1000)
			require.NoError(t, err, "Failed to create reader for %s", f)
			defer func() { _ = reader.Close() }()

			var totalRows int64
			for {
				batch, err := reader.Next(ctx)
				if err == io.EOF {
					break
				}
				require.NoError(t, err, "Error reading batch from %s", f)
				totalRows += int64(batch.Len())
				pipeline.ReturnBatch(batch)
			}

			t.Logf("Read %d rows from %s", totalRows, filepath.Base(f))
		})
	}
}
