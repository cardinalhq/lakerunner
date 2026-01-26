// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package parquetwriter

import (
	"bytes"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPooledZstdCodec_EncodeDecodeRoundtrip(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("hello world, this is a test of zstd compression")

	// Test Encode/Decode
	compressed := codec.Encode(nil, testData)
	assert.NotEmpty(t, compressed)
	assert.NotEqual(t, testData, compressed)

	decompressed := codec.Decode(nil, compressed)
	assert.Equal(t, testData, decompressed)
}

func TestPooledZstdCodec_EncodeLevelRoundtrip(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("hello world, this is a test of zstd compression with levels")

	levels := []int{
		compress.DefaultCompressionLevel,
		1, // fastest
		3, // default zstd level
		5,
	}

	for _, level := range levels {
		t.Run("level_"+strconv.Itoa(level), func(t *testing.T) {
			compressed := codec.EncodeLevel(nil, testData, level)
			assert.NotEmpty(t, compressed)

			decompressed := codec.Decode(nil, compressed)
			assert.Equal(t, testData, decompressed)
		})
	}
}

func TestPooledZstdCodec_StreamingRoundtrip(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("streaming test data that should compress and decompress correctly")

	// Compress via streaming writer
	var compressed bytes.Buffer
	writer := codec.NewWriter(&compressed)
	_, err := writer.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	// Decompress via streaming reader
	reader := codec.NewReader(&compressed)
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.Equal(t, testData, decompressed)
}

func TestPooledZstdCodec_StreamingWithLevel(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("streaming test with compression level")

	var compressed bytes.Buffer
	writer, err := codec.NewWriterLevel(&compressed, 3)
	require.NoError(t, err)

	_, err = writer.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	reader := codec.NewReader(&compressed)
	decompressed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())

	assert.Equal(t, testData, decompressed)
}

func TestPooledZstdCodec_EncoderReuse(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("test data for encoder reuse verification")

	// Run multiple encode/decode cycles to verify pooling works
	for i := 0; i < 10; i++ {
		compressed := codec.EncodeLevel(nil, testData, 3)
		decompressed := codec.Decode(nil, compressed)
		assert.Equal(t, testData, decompressed, "iteration %d failed", i)
	}
}

func TestPooledZstdCodec_ConcurrentAccess(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("concurrent access test data that needs to work correctly under load")

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// Run concurrent encode/decode operations
	for i := range 100 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Mix of different operations
			switch id % 3 {
			case 0:
				// Encode/Decode
				compressed := codec.Encode(nil, testData)
				decompressed := codec.Decode(nil, compressed)
				if !bytes.Equal(testData, decompressed) {
					errChan <- assert.AnError
				}
			case 1:
				// EncodeLevel/Decode
				compressed := codec.EncodeLevel(nil, testData, 3)
				decompressed := codec.Decode(nil, compressed)
				if !bytes.Equal(testData, decompressed) {
					errChan <- assert.AnError
				}
			case 2:
				// Streaming
				var buf bytes.Buffer
				w := codec.NewWriter(&buf)
				_, _ = w.Write(testData)
				_ = w.Close()

				r := codec.NewReader(&buf)
				decompressed, _ := io.ReadAll(r)
				_ = r.Close()
				if !bytes.Equal(testData, decompressed) {
					errChan <- assert.AnError
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		require.NoError(t, err)
	}
}

func TestPooledZstdCodec_CompressBound(t *testing.T) {
	codec := &pooledZstdCodec{}

	tests := []struct {
		inputLen int64
		name     string
	}{
		{100, "small"},
		{1024, "1KB"},
		{1024 * 1024, "1MB"},
		{10 * 1024 * 1024, "10MB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bound := codec.CompressBound(tt.inputLen)
			assert.Greater(t, bound, tt.inputLen, "compress bound should be >= input length")
		})
	}
}

func TestPooledZstdCodec_EmptyInput(t *testing.T) {
	codec := &pooledZstdCodec{}

	// Empty slice should work
	compressed := codec.Encode(nil, []byte{})
	decompressed := codec.Decode(nil, compressed)
	assert.Empty(t, decompressed)
}

func TestPooledZstdCodec_LargeData(t *testing.T) {
	codec := &pooledZstdCodec{}

	// Create 1MB of compressible data
	largeData := make([]byte, 1024*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	compressed := codec.Encode(nil, largeData)
	assert.Less(t, len(compressed), len(largeData), "compressed should be smaller")

	decompressed := codec.Decode(nil, compressed)
	assert.Equal(t, largeData, decompressed)
}

func TestPooledZstdCodec_WriterReturnsToPool(t *testing.T) {
	codec := &pooledZstdCodec{}

	testData := []byte("test data")

	// Create and close multiple writers - they should be returned to pool
	for range 5 {
		var buf bytes.Buffer
		writer, err := codec.NewWriterLevel(&buf, 3)
		require.NoError(t, err)

		_, err = writer.Write(testData)
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		// Verify data is correct
		reader := codec.NewReader(&buf)
		decompressed, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.NoError(t, reader.Close())
		assert.Equal(t, testData, decompressed)
	}
}

func BenchmarkPooledZstdCodec_Encode(b *testing.B) {
	codec := &pooledZstdCodec{}
	data := make([]byte, 10*1024) // 10KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	for b.Loop() {
		_ = codec.Encode(nil, data)
	}
}

func BenchmarkPooledZstdCodec_EncodeLevel(b *testing.B) {
	codec := &pooledZstdCodec{}
	data := make([]byte, 10*1024) // 10KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	for b.Loop() {
		_ = codec.EncodeLevel(nil, data, 3)
	}
}

func BenchmarkPooledZstdCodec_StreamingWrite(b *testing.B) {
	codec := &pooledZstdCodec{}
	data := make([]byte, 10*1024) // 10KB
	for i := range data {
		data[i] = byte(i % 256)
	}

	for b.Loop() {
		var buf bytes.Buffer
		w := codec.NewWriter(&buf)
		_, _ = w.Write(data)
		_ = w.Close()
	}
}
