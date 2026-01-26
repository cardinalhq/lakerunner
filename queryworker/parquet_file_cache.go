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

package queryworker

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	// DefaultCleanupInterval is how often the cleanup goroutine runs.
	DefaultCleanupInterval = 5 * time.Minute

	// DefaultDeletionDelay is how long to wait before actually deleting a file
	// after it's been marked for deletion. This allows concurrent queries to
	// finish using the file before it's removed.
	DefaultDeletionDelay = 1 * time.Minute

	// DiskUsageHighWatermark is the disk utilization level that triggers eviction.
	// When disk usage exceeds this fraction, the cache will start evicting files.
	DiskUsageHighWatermark = 0.80

	// DiskUsageLowWatermark is the target disk utilization after eviction.
	// Eviction continues until disk usage drops below this level.
	DiskUsageLowWatermark = 0.70
)

// CacheKey uniquely identifies a cached file by its cloud storage location.
type CacheKey struct {
	Region   string // cloud region (e.g., "us-east-1"), can be empty
	Bucket   string // bucket name
	ObjectID string // object key/path within the bucket
}

// DiskUsageFunc is a function that returns disk usage statistics.
type DiskUsageFunc func(path string) (usedBytes, totalBytes uint64, err error)

// ParquetFileCache manages downloaded parquet files with disk pressure-based cleanup.
// It tracks file sizes and access times for metrics and LRU eviction purposes.
// The cache owns its storage directory and provides a domain-aware API
// for callers to work with (bucket, region, objectId) rather than file paths.
type ParquetFileCache struct {
	mu sync.RWMutex

	// baseDir is the root directory for all cached files
	baseDir string

	// files tracks metadata for each cached file, keyed by local path
	files map[string]*cachedFileInfo

	// keyToPath maps CacheKey to local file path for lookups
	keyToPath map[CacheKey]string

	// config
	cleanupInterval time.Duration

	// getDiskUsage is the function to check disk usage (injectable for testing)
	getDiskUsage DiskUsageFunc

	// metrics
	fileCount  int64
	totalBytes int64

	// cleanup control
	stopCleanup context.CancelFunc
	cleanupWG   sync.WaitGroup
}

type cachedFileInfo struct {
	key         CacheKey
	path        string
	size        int64
	lastAccess  time.Time
	deleteAfter time.Time // if non-zero, file is marked for deletion after this time
}

// NewParquetFileCache creates a new parquet file cache with disk pressure-based cleanup.
// The cache directory is created under os.TempDir(), which respects the TMPDIR
// environment variable (typically set by helpers.SetupTempDir()).
func NewParquetFileCache(cleanupInterval time.Duration) (*ParquetFileCache, error) {
	baseDir := filepath.Join(os.TempDir(), "parquet-cache")
	return NewParquetFileCacheWithBaseDir(baseDir, cleanupInterval)
}

// NewParquetFileCacheWithBaseDir creates a new parquet file cache with a custom base directory.
// This is useful for testing where each test needs an isolated cache directory.
func NewParquetFileCacheWithBaseDir(baseDir string, cleanupInterval time.Duration) (*ParquetFileCache, error) {
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultCleanupInterval
	}

	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	pfc := &ParquetFileCache{
		baseDir:         baseDir,
		files:           make(map[string]*cachedFileInfo),
		keyToPath:       make(map[CacheKey]string),
		cleanupInterval: cleanupInterval,
		getDiskUsage:    defaultGetDiskUsage,
	}

	ctx, cancel := context.WithCancel(context.Background())
	pfc.stopCleanup = cancel
	pfc.cleanupWG.Add(1)
	go pfc.cleanupLoop(ctx)

	slog.Info("ParquetFileCache initialized",
		slog.String("baseDir", baseDir),
		slog.Duration("cleanupInterval", cleanupInterval),
		slog.Float64("diskHighWatermark", DiskUsageHighWatermark),
		slog.Float64("diskLowWatermark", DiskUsageLowWatermark))

	return pfc, nil
}

// Close stops the cleanup goroutine and removes all cached files.
func (pfc *ParquetFileCache) Close() {
	if pfc.stopCleanup != nil {
		pfc.stopCleanup()
	}
	pfc.cleanupWG.Wait()

	// Clean up all files in the cache directory
	pfc.mu.Lock()
	for path := range pfc.files {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			slog.Warn("Failed to remove cached file during Close",
				slog.String("path", path),
				slog.Any("error", err))
		}
	}
	pfc.files = make(map[string]*cachedFileInfo)
	pfc.keyToPath = make(map[CacheKey]string)
	pfc.fileCount = 0
	pfc.totalBytes = 0
	pfc.mu.Unlock()

	// Remove any remaining empty directories
	pfc.cleanupEmptyDirs()
}

// RegisterMetrics registers OTEL metrics for the parquet file cache.
func (pfc *ParquetFileCache) RegisterMetrics() error {
	meter := otel.Meter("lakerunner.querycache")

	_, err := meter.Int64ObservableGauge(
		"lakerunner.parquet_cache.file_count",
		metric.WithDescription("Number of parquet files cached on disk"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			pfc.mu.RLock()
			defer pfc.mu.RUnlock()
			o.Observe(pfc.fileCount)
			return nil
		}),
	)
	if err != nil {
		return err
	}

	_, err = meter.Int64ObservableGauge(
		"lakerunner.parquet_cache.bytes",
		metric.WithDescription("Total bytes of parquet files cached on disk"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			pfc.mu.RLock()
			defer pfc.mu.RUnlock()
			o.Observe(pfc.totalBytes)
			return nil
		}),
	)
	if err != nil {
		return err
	}

	return nil
}

// GetOrPrepare checks if a file is cached and returns its local path.
// If cached and valid, returns (localPath, true, nil).
// If not cached, prepares the directory structure and returns (localPath, false, nil).
// The caller should download to the returned path and then call TrackFile.
func (pfc *ParquetFileCache) GetOrPrepare(region, bucket, objectID string) (localPath string, exists bool, err error) {
	key := CacheKey{Region: region, Bucket: bucket, ObjectID: objectID}

	// Check if already cached
	pfc.mu.RLock()
	if path, ok := pfc.keyToPath[key]; ok {
		info := pfc.files[path]
		// Check if marked for deletion - treat as not cached
		if info != nil && !info.deleteAfter.IsZero() {
			pfc.mu.RUnlock()
			// File is marked for deletion, return path but exists=false
			return path, false, nil
		}
		if info != nil {
			pfc.mu.RUnlock()
			// Touch to keep alive
			pfc.touchFile(path)
			return path, true, nil
		}
	}
	pfc.mu.RUnlock()

	// Not cached - prepare the path
	localPath = pfc.pathForKey(key)
	dir := filepath.Dir(localPath)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", false, err
	}

	// Check if file exists on disk but not tracked (e.g., from previous run)
	if stat, err := os.Stat(localPath); err == nil {
		pfc.mu.Lock()
		defer pfc.mu.Unlock()

		// Double-check after acquiring write lock - also check for deletion marker
		if existingPath, tracked := pfc.keyToPath[key]; tracked {
			info := pfc.files[existingPath]
			if info != nil && !info.deleteAfter.IsZero() {
				// Marked for deletion, don't count as existing
				return localPath, false, nil
			}
			// Already tracked and not marked for deletion
			return localPath, true, nil
		}

		// Not tracked, add it
		pfc.files[localPath] = &cachedFileInfo{
			key:        key,
			path:       localPath,
			size:       stat.Size(),
			lastAccess: time.Now(),
		}
		pfc.keyToPath[key] = localPath
		pfc.fileCount++
		pfc.totalBytes += stat.Size()
		return localPath, true, nil
	}

	return localPath, false, nil
}

// TrackFile marks a file as successfully downloaded and starts tracking it.
// Should be called after a file is downloaded to the path returned by GetOrPrepare.
func (pfc *ParquetFileCache) TrackFile(region, bucket, objectID string) error {
	key := CacheKey{Region: region, Bucket: bucket, ObjectID: objectID}
	localPath := pfc.pathForKey(key)

	info, err := os.Stat(localPath)
	if err != nil {
		return err
	}

	pfc.mu.Lock()
	defer pfc.mu.Unlock()

	existing, exists := pfc.files[localPath]
	if exists {
		// Update existing entry
		pfc.totalBytes -= existing.size
		existing.size = info.Size()
		existing.lastAccess = time.Now()
		existing.deleteAfter = time.Time{} // Clear any deletion mark
		pfc.totalBytes += existing.size
	} else {
		// Add new entry
		pfc.files[localPath] = &cachedFileInfo{
			key:        key,
			path:       localPath,
			size:       info.Size(),
			lastAccess: time.Now(),
		}
		pfc.keyToPath[key] = localPath
		pfc.fileCount++
		pfc.totalBytes += info.Size()
	}

	return nil
}

// MarkForDeletion marks a file for delayed deletion. The file will be deleted
// after DefaultDeletionDelay has passed. Until then, the file remains on disk
// but GetOrPrepare will return exists=false, causing re-downloads if needed.
func (pfc *ParquetFileCache) MarkForDeletion(region, bucket, objectID string) {
	key := CacheKey{Region: region, Bucket: bucket, ObjectID: objectID}

	pfc.mu.Lock()
	defer pfc.mu.Unlock()

	if path, ok := pfc.keyToPath[key]; ok {
		if info, exists := pfc.files[path]; exists {
			info.deleteAfter = time.Now().Add(DefaultDeletionDelay)
		}
	}
}

// MarkForDeletionWithCompanion marks a tbl_ file and its corresponding agg_ file
// for delayed deletion. This ensures tbl_ and agg_ files are treated as a unit.
func (pfc *ParquetFileCache) MarkForDeletionWithCompanion(region, bucket, objectID string) {
	// Mark the primary file
	pfc.MarkForDeletion(region, bucket, objectID)

	// If this is a tbl_ file, also mark the corresponding agg_ file
	if strings.Contains(objectID, "/tbl_") {
		aggObjectID := strings.Replace(objectID, "/tbl_", "/agg_", 1)
		pfc.MarkForDeletion(region, bucket, aggObjectID)
	}
}

// FileCount returns the current number of tracked files.
func (pfc *ParquetFileCache) FileCount() int64 {
	pfc.mu.RLock()
	defer pfc.mu.RUnlock()
	return pfc.fileCount
}

// TotalBytes returns the total size of tracked files.
func (pfc *ParquetFileCache) TotalBytes() int64 {
	pfc.mu.RLock()
	defer pfc.mu.RUnlock()
	return pfc.totalBytes
}

// pathForKey computes the local file path for a cache key.
// Structure: <baseDir>/<region>/<bucket>/<objectID>
// If region is empty, it's omitted from the path.
func (pfc *ParquetFileCache) pathForKey(key CacheKey) string {
	if key.Region != "" {
		return filepath.Join(pfc.baseDir, key.Region, key.Bucket, key.ObjectID)
	}
	return filepath.Join(pfc.baseDir, key.Bucket, key.ObjectID)
}

// keyFromPath attempts to reconstruct a CacheKey from a file path.
// The objectID is expected to start with "db/" which serves as a marker.
// Returns (key, true) if successful, (CacheKey{}, false) if the path
// structure cannot be parsed (e.g., files from incompatible versions).
func (pfc *ParquetFileCache) keyFromPath(path string) (CacheKey, bool) {
	// Remove baseDir prefix
	relPath := strings.TrimPrefix(path, pfc.baseDir)
	relPath = strings.TrimPrefix(relPath, string(filepath.Separator))

	// Split into components
	parts := strings.Split(relPath, string(filepath.Separator))
	if len(parts) < 2 {
		return CacheKey{}, false
	}

	// Find the "db" component which starts the objectID
	dbIndex := -1
	for i, part := range parts {
		if part == "db" {
			dbIndex = i
			break
		}
	}

	if dbIndex < 0 || dbIndex < 1 {
		// No "db" marker found, or it's at position 0 (no bucket)
		return CacheKey{}, false
	}

	// Reconstruct based on how many components precede "db"
	// - 1 component before "db" = bucket only (no region)
	// - 2 components before "db" = region + bucket
	objectID := strings.Join(parts[dbIndex:], string(filepath.Separator))

	var region, bucket string
	if dbIndex == 1 {
		// Path: <bucket>/db/...
		bucket = parts[0]
	} else if dbIndex == 2 {
		// Path: <region>/<bucket>/db/...
		region = parts[0]
		bucket = parts[1]
	} else {
		// Unexpected path structure
		return CacheKey{}, false
	}

	return CacheKey{
		Region:   region,
		Bucket:   bucket,
		ObjectID: objectID,
	}, true
}

// touchFile updates the last access time for a file.
func (pfc *ParquetFileCache) touchFile(path string) {
	pfc.mu.Lock()
	defer pfc.mu.Unlock()

	if info, exists := pfc.files[path]; exists {
		info.lastAccess = time.Now()
	}
}

// removeFile removes a file from tracking and deletes it from disk.
func (pfc *ParquetFileCache) removeFile(path string) {
	pfc.mu.Lock()
	if info, exists := pfc.files[path]; exists {
		pfc.totalBytes -= info.size
		pfc.fileCount--
		delete(pfc.keyToPath, info.key)
		delete(pfc.files, path)
	}
	pfc.mu.Unlock()

	// Best-effort delete from disk
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		slog.Warn("Failed to remove cached file from disk",
			slog.String("path", path),
			slog.Any("error", err))
	}
}

// cleanupLoop periodically checks for disk pressure and removes marked files.
func (pfc *ParquetFileCache) cleanupLoop(ctx context.Context) {
	defer pfc.cleanupWG.Done()

	ticker := time.NewTicker(pfc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pfc.cleanupMarkedFiles()
		}
	}
}

// cleanupMarkedFiles removes files that have been marked for deletion and
// whose deletion delay has passed. Also triggers disk-pressure eviction if needed.
func (pfc *ParquetFileCache) cleanupMarkedFiles() {
	now := time.Now()

	var toRemove []string

	pfc.mu.RLock()
	for path, info := range pfc.files {
		// Remove if marked for deletion and delay has passed
		if !info.deleteAfter.IsZero() && now.After(info.deleteAfter) {
			toRemove = append(toRemove, path)
		}
	}
	pfc.mu.RUnlock()

	if len(toRemove) > 0 {
		slog.Info("Cleaning up marked-for-deletion parquet files",
			slog.Int("count", len(toRemove)))

		for _, path := range toRemove {
			pfc.removeFile(path)
		}
	}

	// Check disk pressure and evict if needed
	pfc.evictForDiskPressure()

	// Try to remove empty directories
	pfc.cleanupEmptyDirs()
}

// evictForDiskPressure removes files LRU-style when disk usage exceeds the high watermark.
// Eviction continues until disk usage drops below the low watermark.
func (pfc *ParquetFileCache) evictForDiskPressure() {
	usedBytes, totalBytes, err := pfc.getDiskUsage(pfc.baseDir)
	if err != nil {
		slog.Warn("Failed to get disk usage for eviction check",
			slog.String("baseDir", pfc.baseDir),
			slog.Any("error", err))
		return
	}

	diskUtilization := float64(usedBytes) / float64(totalBytes)
	if diskUtilization < DiskUsageHighWatermark {
		return // No pressure, nothing to do
	}

	slog.Info("Disk pressure detected, starting LRU eviction",
		slog.Float64("utilization", diskUtilization),
		slog.Float64("highWatermark", DiskUsageHighWatermark),
		slog.Float64("lowWatermark", DiskUsageLowWatermark),
		slog.Uint64("usedBytes", usedBytes),
		slog.Uint64("totalBytes", totalBytes))

	// Build LRU list of files (oldest access first)
	type fileEntry struct {
		path       string
		lastAccess time.Time
	}

	pfc.mu.RLock()
	entries := make([]fileEntry, 0, len(pfc.files))
	for path, info := range pfc.files {
		entries = append(entries, fileEntry{path: path, lastAccess: info.lastAccess})
	}
	pfc.mu.RUnlock()

	// Sort by last access time (oldest first)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].lastAccess.Before(entries[j].lastAccess)
	})

	// Evict until we're below the low watermark
	evictedCount := 0
	for _, entry := range entries {
		// Re-check disk usage
		usedBytes, totalBytes, err = pfc.getDiskUsage(pfc.baseDir)
		if err != nil {
			break
		}
		diskUtilization = float64(usedBytes) / float64(totalBytes)
		if diskUtilization <= DiskUsageLowWatermark {
			break // We've evicted enough
		}

		pfc.removeFile(entry.path)
		evictedCount++
	}

	if evictedCount > 0 {
		// Final disk usage check for logging
		usedBytes, totalBytes, _ = pfc.getDiskUsage(pfc.baseDir)
		finalUtilization := float64(usedBytes) / float64(totalBytes)
		slog.Info("Disk pressure eviction complete",
			slog.Int("evictedCount", evictedCount),
			slog.Float64("finalUtilization", finalUtilization))
	}
}

// defaultGetDiskUsage returns current disk usage statistics for the filesystem containing
// the given path. Returns used bytes and total bytes.
func defaultGetDiskUsage(path string) (usedBytes, totalBytes uint64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}

	totalBytes = stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	usedBytes = totalBytes - freeBytes

	return usedBytes, totalBytes, nil
}

// cleanupEmptyDirs removes empty directories under the cache base dir.
func (pfc *ParquetFileCache) cleanupEmptyDirs() {
	_ = filepath.Walk(pfc.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if !info.IsDir() {
			return nil
		}
		if path == pfc.baseDir {
			return nil // don't remove base dir
		}

		// Try to remove - will fail if not empty
		_ = os.Remove(path)
		return nil
	})
}

// ScanExistingFiles scans the cache directory and tracks any existing files.
// This is useful on startup to recover state from previous runs.
// Files with parseable paths (containing "db/" marker) are fully tracked
// with CacheKey reconstruction. Other files are tracked for cleanup only.
func (pfc *ParquetFileCache) ScanExistingFiles() error {
	return filepath.Walk(pfc.baseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".parquet" {
			return nil
		}

		pfc.mu.Lock()
		if _, exists := pfc.files[path]; !exists {
			fileInfo := &cachedFileInfo{
				path:       path,
				size:       info.Size(),
				lastAccess: info.ModTime(),
			}

			// Try to reconstruct the CacheKey from the path
			if key, ok := pfc.keyFromPath(path); ok {
				fileInfo.key = key
				pfc.keyToPath[key] = path
			}

			pfc.files[path] = fileInfo
			pfc.fileCount++
			pfc.totalBytes += info.Size()
		}
		pfc.mu.Unlock()

		return nil
	})
}
