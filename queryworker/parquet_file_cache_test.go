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

package queryworker

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestCache creates a ParquetFileCache with an isolated test directory.
func newTestCache(t *testing.T) *ParquetFileCache {
	t.Helper()
	baseDir := t.TempDir()
	pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 0, 0)
	require.NoError(t, err)
	t.Cleanup(func() { pfc.Close() })
	return pfc
}

func TestNewParquetFileCache(t *testing.T) {
	t.Parallel()

	t.Run("creates cache with default TTLs", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		assert.Equal(t, DefaultParquetFileTTL, pfc.fileTTL)
		assert.Equal(t, DefaultCleanupInterval, pfc.cleanupInterval)
		assert.NotEmpty(t, pfc.baseDir)
	})

	t.Run("creates cache with custom TTLs", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 10*time.Minute, 2*time.Minute)
		require.NoError(t, err)
		defer pfc.Close()

		assert.Equal(t, 10*time.Minute, pfc.fileTTL)
		assert.Equal(t, 2*time.Minute, pfc.cleanupInterval)
	})

	t.Run("base directory is under temp dir", func(t *testing.T) {
		t.Parallel()
		pfc, err := NewParquetFileCache(0, 0)
		require.NoError(t, err)
		defer pfc.Close()

		assert.Contains(t, pfc.baseDir, "parquet-cache")
	})
}

func TestParquetFileCache_GetOrPrepare(t *testing.T) {
	t.Parallel()

	t.Run("returns path and exists=false for new file", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		localPath, exists, err := pfc.GetOrPrepare("us-east-1", "my-bucket", "db/org/collector/date/file.parquet")
		require.NoError(t, err)
		assert.False(t, exists)
		assert.NotEmpty(t, localPath)
		assert.Contains(t, localPath, "us-east-1")
		assert.Contains(t, localPath, "my-bucket")
		assert.Contains(t, localPath, "file.parquet")
	})

	t.Run("creates parent directories", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		localPath, _, err := pfc.GetOrPrepare("us-west-2", "test-bucket", "deep/nested/path/file.parquet")
		require.NoError(t, err)

		dir := filepath.Dir(localPath)
		stat, err := os.Stat(dir)
		require.NoError(t, err)
		assert.True(t, stat.IsDir())
	})

	t.Run("returns exists=true after file is tracked", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "eu-west-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// First call - not cached
		localPath, exists, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		assert.False(t, exists)

		// Create the file
		err = os.WriteFile(localPath, []byte("test content"), 0644)
		require.NoError(t, err)

		// Track the file
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		// Second call - should be cached
		localPath2, exists2, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		assert.True(t, exists2)
		assert.Equal(t, localPath, localPath2)
	})

	t.Run("path without region omits region from path", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		localPath, _, err := pfc.GetOrPrepare("", "my-bucket", "file.parquet")
		require.NoError(t, err)

		// Should have bucket but not region component
		assert.Contains(t, localPath, "my-bucket")
		// Path structure should be baseDir/bucket/objectID (no region)
		expected := filepath.Join(pfc.baseDir, "my-bucket", "file.parquet")
		assert.Equal(t, expected, localPath)
	})

	t.Run("path with region includes region in path", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		localPath, _, err := pfc.GetOrPrepare("us-east-1", "my-bucket", "file.parquet")
		require.NoError(t, err)

		// Path structure should be baseDir/region/bucket/objectID
		expected := filepath.Join(pfc.baseDir, "us-east-1", "my-bucket", "file.parquet")
		assert.Equal(t, expected, localPath)
	})
}

func TestParquetFileCache_TrackFile(t *testing.T) {
	t.Parallel()

	t.Run("tracks new file", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// Prepare and create file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test content"), 0644)
		require.NoError(t, err)

		// Track
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		assert.Equal(t, int64(1), pfc.FileCount())
		assert.Equal(t, int64(12), pfc.TotalBytes()) // "test content" is 12 bytes
	})

	t.Run("updates existing tracked file", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// Prepare and create file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("short"), 0644)
		require.NoError(t, err)

		// Track first time
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)
		assert.Equal(t, int64(5), pfc.TotalBytes())

		// Update file with more content
		err = os.WriteFile(localPath, []byte("much longer content"), 0644)
		require.NoError(t, err)

		// Track again
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		assert.Equal(t, int64(1), pfc.FileCount()) // Still just 1 file
		assert.Equal(t, int64(19), pfc.TotalBytes())
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		err := pfc.TrackFile("us-east-1", "bucket", "nonexistent/file.parquet")
		require.Error(t, err)
	})
}

func TestParquetFileCache_MarkForDeletion(t *testing.T) {
	t.Parallel()

	t.Run("marked file returns exists=false from GetOrPrepare", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// Prepare, create, and track file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		// Verify it's cached
		_, exists, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		assert.True(t, exists)

		// Mark for deletion
		pfc.MarkForDeletion(region, bucket, objectID)

		// Should now return exists=false (will trigger re-download)
		_, exists, err = pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestParquetFileCache_Close(t *testing.T) {
	t.Parallel()

	t.Run("cleans up all files on close", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 0, 0)
		require.NoError(t, err)

		region := "us-east-1"
		bucket := "test-bucket"

		// Create and track multiple files
		for i := 0; i < 3; i++ {
			objectID := filepath.Join("test", "file"+string(rune('0'+i))+".parquet")
			localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
			require.NoError(t, err)
			err = os.WriteFile(localPath, []byte("test"), 0644)
			require.NoError(t, err)
			err = pfc.TrackFile(region, bucket, objectID)
			require.NoError(t, err)
		}

		assert.Equal(t, int64(3), pfc.FileCount())

		// Close should clean up
		pfc.Close()

		assert.Equal(t, int64(0), pfc.FileCount())
		assert.Equal(t, int64(0), pfc.TotalBytes())
	})
}

func TestParquetFileCache_DifferentBucketsAndRegions(t *testing.T) {
	t.Parallel()

	t.Run("same objectID in different buckets are separate files", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		objectID := "same/path/file.parquet"

		path1, _, err := pfc.GetOrPrepare(region, "bucket-1", objectID)
		require.NoError(t, err)

		path2, _, err := pfc.GetOrPrepare(region, "bucket-2", objectID)
		require.NoError(t, err)

		assert.NotEqual(t, path1, path2)
	})

	t.Run("same objectID in different regions are separate files", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		bucket := "my-bucket"
		objectID := "same/path/file.parquet"

		path1, _, err := pfc.GetOrPrepare("us-east-1", bucket, objectID)
		require.NoError(t, err)

		path2, _, err := pfc.GetOrPrepare("eu-west-1", bucket, objectID)
		require.NoError(t, err)

		assert.NotEqual(t, path1, path2)
	})
}

func TestCacheKey(t *testing.T) {
	t.Parallel()

	t.Run("CacheKey equality", func(t *testing.T) {
		t.Parallel()

		key1 := CacheKey{Region: "us-east-1", Bucket: "bucket", ObjectID: "obj"}
		key2 := CacheKey{Region: "us-east-1", Bucket: "bucket", ObjectID: "obj"}
		key3 := CacheKey{Region: "us-west-2", Bucket: "bucket", ObjectID: "obj"}

		assert.Equal(t, key1, key2)
		assert.NotEqual(t, key1, key3)
	})
}
