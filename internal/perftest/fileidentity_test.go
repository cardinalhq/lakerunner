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
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// TestChunkSizeFileIdentity verifies that different chunk sizes produce
// identical Parquet files (same bytes, same compression).
func TestChunkSizeFileIdentity(t *testing.T) {
	// Find raw OTEL files
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) == 0 {
		t.Skip("No raw test data found. Run: ./scripts/download-perf-testdata.sh raw 10")
	}

	testFile := files[0]
	ctx := context.Background()

	// Pre-load batches
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      "test-org",
	}

	reader, err := filereader.ReaderForFileWithOptions(testFile, options)
	if err != nil {
		t.Fatal(err)
	}

	var batches []*pipeline.Batch
	for {
		batch, err := reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = reader.Close()
			t.Fatal(err)
		}
		batches = append(batches, batch)
	}

	// Get schema from reader before closing
	schema := reader.GetSchema()
	_ = reader.Close()

	t.Logf("Loaded %d batches for identity test", len(batches))

	// Define string conversion prefixes (must match backend config)
	stringConversionPrefixes := []string{"resource_", "scope_", "attr_"}

	// Adjust schema to convert matching columns to strings (match backend behavior)
	adjustedSchema := adjustSchemaForStringConversion(schema, stringConversionPrefixes)

	// Test different chunk sizes
	chunkSizes := []int64{10000, 25000, 50000}
	hashes := make(map[int64]string)
	sizes := make(map[int64]int64)

	for _, chunkSize := range chunkSizes {
		tmpDir := t.TempDir()

		config := parquetwriter.BackendConfig{
			Type:                     parquetwriter.BackendArrow,
			TmpDir:                   tmpDir,
			Schema:                   adjustedSchema,
			ChunkSize:                chunkSize,
			StringConversionPrefixes: stringConversionPrefixes,
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

		// Calculate SHA256
		f, err := os.Open(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		_, _ = io.Copy(h, f)
		_ = f.Close()
		hash := fmt.Sprintf("%x", h.Sum(nil))
		hashes[chunkSize] = hash

		stat, _ := os.Stat(outputPath)
		sizes[chunkSize] = stat.Size()

		t.Logf("Chunk %5d: %6d bytes, SHA256: %s...", chunkSize, stat.Size(), hash[:16])
	}

	// Verify all files are identical
	if hashes[10000] != hashes[25000] {
		t.Errorf("Files with chunk 10K and 25K differ:\n  10K: %s\n  25K: %s",
			hashes[10000][:32], hashes[25000][:32])
	}
	if hashes[25000] != hashes[50000] {
		t.Errorf("Files with chunk 25K and 50K differ:\n  25K: %s\n  50K: %s",
			hashes[25000][:32], hashes[50000][:32])
	}
	if hashes[10000] != hashes[50000] {
		t.Errorf("Files with chunk 10K and 50K differ:\n  10K: %s\n  50K: %s",
			hashes[10000][:32], hashes[50000][:32])
	}

	// Verify all sizes are identical
	if sizes[10000] != sizes[25000] || sizes[25000] != sizes[50000] {
		t.Errorf("File sizes differ: 10K=%d, 25K=%d, 50K=%d",
			sizes[10000], sizes[25000], sizes[50000])
	}

	if hashes[10000] == hashes[25000] && hashes[25000] == hashes[50000] {
		t.Logf("✓ All files are byte-for-byte IDENTICAL (same SHA256, same size)")
		t.Logf("  This confirms chunk size only affects in-memory buffering, not output")
	}
}

// adjustSchemaForStringConversion converts columns matching the given prefixes to string type.
// This ensures the schema matches what the backend will actually write after string conversion.
func adjustSchemaForStringConversion(schema *filereader.ReaderSchema, prefixes []string) *filereader.ReaderSchema {
	newSchema := filereader.NewReaderSchema()

	for _, col := range schema.Columns() {
		dataType := col.DataType
		hasNonNull := col.HasNonNull
		fieldName := string(col.Name.Value())

		// Check if this column name matches any prefix
		shouldConvert := false
		for _, prefix := range prefixes {
			if strings.HasPrefix(fieldName, prefix) {
				shouldConvert = true
				break
			}
		}

		// Convert to string if it matches a prefix
		if shouldConvert {
			dataType = filereader.DataTypeString
		}

		// Add column to new schema - preserve original name mapping
		originalName := schema.GetOriginalName(col.Name)
		newSchema.AddColumn(col.Name, originalName, dataType, hasNonNull)
	}

	return newSchema
}
