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
	"fmt"
	"os"
	"path/filepath"
	"sync"
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

func TestParquetFileCache_ConcurrentGetOrPrepare(t *testing.T) {
	t.Parallel()

	t.Run("concurrent access to same key", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := filepath.Join("db", "org", "collector", "file.parquet")

		// Prepare path and create file once
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test content"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		// Launch many goroutines hitting the same key
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, exists, err := pfc.GetOrPrepare(region, bucket, objectID)
				assert.NoError(t, err)
				assert.True(t, exists)
			}()
		}
		wg.Wait()

		// Should still only track one file
		assert.Equal(t, int64(1), pfc.FileCount())
		assert.Equal(t, int64(12), pfc.TotalBytes())
	})

	t.Run("concurrent access to different keys", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"

		// Launch many goroutines with different keys
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				objectID := fmt.Sprintf("db/org/collector/file%d.parquet", idx)
				localPath, exists, err := pfc.GetOrPrepare(region, bucket, objectID)
				assert.NoError(t, err)
				assert.False(t, exists)

				// Create and track the file
				err = os.WriteFile(localPath, []byte("test"), 0644)
				assert.NoError(t, err)
				err = pfc.TrackFile(region, bucket, objectID)
				assert.NoError(t, err)
			}(i)
		}
		wg.Wait()

		// Should track all 50 files
		assert.Equal(t, int64(50), pfc.FileCount())
		assert.Equal(t, int64(50*4), pfc.TotalBytes()) // 4 bytes each
	})

	t.Run("concurrent GetOrPrepare and TrackFile", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := filepath.Join("db", "org", "collector", "concurrent.parquet")

		// Create the file first
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("data"), 0644)
		require.NoError(t, err)

		// Concurrent GetOrPrepare and TrackFile calls
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(2)
			go func() {
				defer wg.Done()
				_, _, _ = pfc.GetOrPrepare(region, bucket, objectID)
			}()
			go func() {
				defer wg.Done()
				_ = pfc.TrackFile(region, bucket, objectID)
			}()
		}
		wg.Wait()

		// Should still only track one file with correct size
		assert.Equal(t, int64(1), pfc.FileCount())
		assert.Equal(t, int64(4), pfc.TotalBytes())
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
			objectID := filepath.Join("test", fmt.Sprintf("file%d.parquet", i))
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

func TestParquetFileCache_keyFromPath(t *testing.T) {
	t.Parallel()

	t.Run("parses path with region", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		path := filepath.Join(pfc.baseDir, "us-east-1", "my-bucket", "db", "org", "collector", "file.parquet")
		key, ok := pfc.keyFromPath(path)

		assert.True(t, ok)
		assert.Equal(t, "us-east-1", key.Region)
		assert.Equal(t, "my-bucket", key.Bucket)
		assert.Equal(t, filepath.Join("db", "org", "collector", "file.parquet"), key.ObjectID)
	})

	t.Run("parses path without region", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		path := filepath.Join(pfc.baseDir, "my-bucket", "db", "org", "collector", "file.parquet")
		key, ok := pfc.keyFromPath(path)

		assert.True(t, ok)
		assert.Equal(t, "", key.Region)
		assert.Equal(t, "my-bucket", key.Bucket)
		assert.Equal(t, filepath.Join("db", "org", "collector", "file.parquet"), key.ObjectID)
	})

	t.Run("returns false for path without db marker", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		path := filepath.Join(pfc.baseDir, "bucket", "some", "path", "file.parquet")
		_, ok := pfc.keyFromPath(path)

		assert.False(t, ok)
	})

	t.Run("returns false for too short path", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		path := filepath.Join(pfc.baseDir, "file.parquet")
		_, ok := pfc.keyFromPath(path)

		assert.False(t, ok)
	})

	t.Run("roundtrip with pathForKey", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		original := CacheKey{
			Region:   "eu-west-1",
			Bucket:   "test-bucket",
			ObjectID: filepath.Join("db", "org123", "collector", "20241215", "logs", "seg1", "tbl_123.parquet"),
		}

		path := pfc.pathForKey(original)
		recovered, ok := pfc.keyFromPath(path)

		assert.True(t, ok)
		assert.Equal(t, original, recovered)
	})

	t.Run("roundtrip without region", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		original := CacheKey{
			Region:   "",
			Bucket:   "test-bucket",
			ObjectID: filepath.Join("db", "org123", "collector", "tbl_123.parquet"),
		}

		path := pfc.pathForKey(original)
		recovered, ok := pfc.keyFromPath(path)

		assert.True(t, ok)
		assert.Equal(t, original, recovered)
	})
}

func TestParquetFileCache_removeFile(t *testing.T) {
	t.Parallel()

	t.Run("removes tracked file from cache and disk", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// Create and track a file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test content"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		assert.Equal(t, int64(1), pfc.FileCount())
		assert.Equal(t, int64(12), pfc.TotalBytes())

		// Remove the file
		pfc.removeFile(localPath)

		// Verify tracking is cleared
		assert.Equal(t, int64(0), pfc.FileCount())
		assert.Equal(t, int64(0), pfc.TotalBytes())

		// Verify file is deleted from disk
		_, err = os.Stat(localPath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("handles non-existent file gracefully", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		// Should not panic when removing non-tracked file
		pfc.removeFile("/nonexistent/path/file.parquet")

		assert.Equal(t, int64(0), pfc.FileCount())
	})
}

func TestParquetFileCache_cleanupExpiredFiles(t *testing.T) {
	t.Parallel()

	t.Run("removes files past TTL", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		// Use very short TTL for testing
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 1*time.Millisecond, 1*time.Hour)
		require.NoError(t, err)
		defer pfc.Close()

		region := "us-east-1"
		bucket := "test-bucket"

		// Create and track a file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, "test/file.parquet")
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, "test/file.parquet")
		require.NoError(t, err)

		assert.Equal(t, int64(1), pfc.FileCount())

		// Wait for TTL to expire
		time.Sleep(10 * time.Millisecond)

		// Trigger cleanup
		pfc.cleanupExpiredFiles()

		// File should be removed
		assert.Equal(t, int64(0), pfc.FileCount())
		_, err = os.Stat(localPath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("removes files marked for deletion after delay", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		// Use long TTL but we'll mark for deletion
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 1*time.Hour, 1*time.Hour)
		require.NoError(t, err)
		defer pfc.Close()

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// Create and track a file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		// Mark for deletion
		pfc.MarkForDeletion(region, bucket, objectID)

		// Manually set deleteAfter to past time to simulate delay passing
		pfc.mu.Lock()
		key := CacheKey{Region: region, Bucket: bucket, ObjectID: objectID}
		if path, ok := pfc.keyToPath[key]; ok {
			if info, exists := pfc.files[path]; exists {
				info.deleteAfter = time.Now().Add(-1 * time.Second)
			}
		}
		pfc.mu.Unlock()

		// Trigger cleanup
		pfc.cleanupExpiredFiles()

		// File should be removed
		assert.Equal(t, int64(0), pfc.FileCount())
	})

	t.Run("keeps files within TTL", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 1*time.Hour, 1*time.Hour)
		require.NoError(t, err)
		defer pfc.Close()

		region := "us-east-1"
		bucket := "test-bucket"

		// Create and track a file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, "test/file.parquet")
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, "test/file.parquet")
		require.NoError(t, err)

		// Trigger cleanup immediately
		pfc.cleanupExpiredFiles()

		// File should still be there
		assert.Equal(t, int64(1), pfc.FileCount())
		_, err = os.Stat(localPath)
		assert.NoError(t, err)
	})
}

func TestParquetFileCache_cleanupEmptyDirs(t *testing.T) {
	t.Parallel()

	t.Run("removes empty directories", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "deep/nested/path/file.parquet"

		// Create and track a file in a nested directory
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		// Remove the file directly (leaving empty directories)
		err = os.Remove(localPath)
		require.NoError(t, err)

		// Verify parent directory exists before cleanup
		parentDir := filepath.Dir(localPath)
		_, err = os.Stat(parentDir)
		require.NoError(t, err)

		// Cleanup empty directories
		pfc.cleanupEmptyDirs()

		// Parent directory should be removed (it's empty)
		_, err = os.Stat(parentDir)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("keeps non-empty directories", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"

		// Create and track a file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, "test/file.parquet")
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, "test/file.parquet")
		require.NoError(t, err)

		// Cleanup empty directories
		pfc.cleanupEmptyDirs()

		// Directory should still exist (has file in it)
		parentDir := filepath.Dir(localPath)
		_, err = os.Stat(parentDir)
		assert.NoError(t, err)
	})

	t.Run("does not remove base directory", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		// Cleanup on empty cache
		pfc.cleanupEmptyDirs()

		// Base directory should still exist
		_, err := os.Stat(pfc.baseDir)
		assert.NoError(t, err)
	})
}

func TestParquetFileCache_ScanExistingFiles(t *testing.T) {
	t.Parallel()

	t.Run("discovers existing parquet files", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()

		// Create some files before creating the cache
		nestedDir := filepath.Join(baseDir, "us-east-1", "bucket", "path")
		err := os.MkdirAll(nestedDir, 0755)
		require.NoError(t, err)

		file1 := filepath.Join(nestedDir, "file1.parquet")
		file2 := filepath.Join(nestedDir, "file2.parquet")
		err = os.WriteFile(file1, []byte("content1"), 0644)
		require.NoError(t, err)
		err = os.WriteFile(file2, []byte("content2content2"), 0644)
		require.NoError(t, err)

		// Create cache
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 0, 0)
		require.NoError(t, err)
		defer pfc.Close()

		// Initially no files tracked
		assert.Equal(t, int64(0), pfc.FileCount())

		// Scan for existing files
		err = pfc.ScanExistingFiles()
		require.NoError(t, err)

		// Should find both files
		assert.Equal(t, int64(2), pfc.FileCount())
		assert.Equal(t, int64(8+16), pfc.TotalBytes()) // "content1" (8) + "content2content2" (16)
	})

	t.Run("ignores non-parquet files", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()

		// Create mixed files
		err := os.MkdirAll(filepath.Join(baseDir, "data"), 0755)
		require.NoError(t, err)

		err = os.WriteFile(filepath.Join(baseDir, "data", "file.parquet"), []byte("parquet"), 0644)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(baseDir, "data", "file.txt"), []byte("text"), 0644)
		require.NoError(t, err)
		err = os.WriteFile(filepath.Join(baseDir, "data", "file.json"), []byte("json"), 0644)
		require.NoError(t, err)

		// Create cache and scan
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 0, 0)
		require.NoError(t, err)
		defer pfc.Close()

		err = pfc.ScanExistingFiles()
		require.NoError(t, err)

		// Should only find the parquet file
		assert.Equal(t, int64(1), pfc.FileCount())
		assert.Equal(t, int64(7), pfc.TotalBytes()) // "parquet" is 7 bytes
	})

	t.Run("does not duplicate already tracked files", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		region := "us-east-1"
		bucket := "test-bucket"
		objectID := "test/file.parquet"

		// Create and track a file
		localPath, _, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		err = os.WriteFile(localPath, []byte("test"), 0644)
		require.NoError(t, err)
		err = pfc.TrackFile(region, bucket, objectID)
		require.NoError(t, err)

		assert.Equal(t, int64(1), pfc.FileCount())

		// Scan should not duplicate
		err = pfc.ScanExistingFiles()
		require.NoError(t, err)

		assert.Equal(t, int64(1), pfc.FileCount())
		assert.Equal(t, int64(4), pfc.TotalBytes())
	})

	t.Run("handles empty directory", func(t *testing.T) {
		t.Parallel()
		pfc := newTestCache(t)

		err := pfc.ScanExistingFiles()
		require.NoError(t, err)

		assert.Equal(t, int64(0), pfc.FileCount())
	})

	t.Run("reconstructs CacheKey for files with db marker", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()

		// Create a file with proper db/ structure
		region := "us-east-1"
		bucket := "my-bucket"
		objectID := filepath.Join("db", "org123", "collector", "tbl_1.parquet")

		filePath := filepath.Join(baseDir, region, bucket, objectID)
		err := os.MkdirAll(filepath.Dir(filePath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(filePath, []byte("test data"), 0644)
		require.NoError(t, err)

		// Create cache and scan
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 0, 0)
		require.NoError(t, err)
		defer pfc.Close()

		err = pfc.ScanExistingFiles()
		require.NoError(t, err)

		// File should be findable via GetOrPrepare
		localPath, exists, err := pfc.GetOrPrepare(region, bucket, objectID)
		require.NoError(t, err)
		assert.True(t, exists, "scanned file should be found via GetOrPrepare")
		assert.Equal(t, filePath, localPath)
	})

	t.Run("scanned files without db marker tracked for cleanup only", func(t *testing.T) {
		t.Parallel()
		baseDir := t.TempDir()

		// Create a file without db/ structure (non-standard path)
		filePath := filepath.Join(baseDir, "some", "random", "file.parquet")
		err := os.MkdirAll(filepath.Dir(filePath), 0755)
		require.NoError(t, err)
		err = os.WriteFile(filePath, []byte("test data"), 0644)
		require.NoError(t, err)

		// Create cache and scan
		pfc, err := NewParquetFileCacheWithBaseDir(baseDir, 0, 0)
		require.NoError(t, err)
		defer pfc.Close()

		err = pfc.ScanExistingFiles()
		require.NoError(t, err)

		// File should be counted
		assert.Equal(t, int64(1), pfc.FileCount())

		// But won't be findable via GetOrPrepare since we can't reconstruct the key
		// (would need to know what region/bucket it belongs to)
	})
}
