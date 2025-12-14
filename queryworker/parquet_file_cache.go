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
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	// DefaultParquetFileTTL is how long downloaded parquet files are kept before cleanup.
	DefaultParquetFileTTL = 30 * time.Minute

	// DefaultCleanupInterval is how often the cleanup goroutine runs.
	DefaultCleanupInterval = 5 * time.Minute

	// ParquetCacheBaseDir is the base directory for cached parquet files.
	ParquetCacheBaseDir = "db"

	// DefaultDeletionDelay is how long to wait before actually deleting a file
	// after it's been marked for deletion. This allows concurrent queries to
	// finish using the file before it's removed.
	DefaultDeletionDelay = 1 * time.Minute
)

// ParquetFileCache manages downloaded parquet files with TTL-based cleanup.
// It tracks file sizes and access times for metrics and cleanup purposes.
type ParquetFileCache struct {
	mu sync.RWMutex

	// files tracks metadata for each cached file
	files map[string]*cachedFileInfo

	// config
	fileTTL         time.Duration
	cleanupInterval time.Duration

	// metrics
	fileCount  int64
	totalBytes int64

	// cleanup control
	stopCleanup context.CancelFunc
	cleanupWG   sync.WaitGroup
}

type cachedFileInfo struct {
	path        string
	size        int64
	lastAccess  time.Time
	deleteAfter time.Time // if non-zero, file is marked for deletion after this time
}

// NewParquetFileCache creates a new parquet file cache with TTL-based cleanup.
func NewParquetFileCache(fileTTL, cleanupInterval time.Duration) *ParquetFileCache {
	if fileTTL <= 0 {
		fileTTL = DefaultParquetFileTTL
	}
	if cleanupInterval <= 0 {
		cleanupInterval = DefaultCleanupInterval
	}

	pfc := &ParquetFileCache{
		files:           make(map[string]*cachedFileInfo),
		fileTTL:         fileTTL,
		cleanupInterval: cleanupInterval,
	}

	ctx, cancel := context.WithCancel(context.Background())
	pfc.stopCleanup = cancel
	pfc.cleanupWG.Add(1)
	go pfc.cleanupLoop(ctx)

	slog.Info("ParquetFileCache initialized",
		slog.Duration("fileTTL", fileTTL),
		slog.Duration("cleanupInterval", cleanupInterval))

	return pfc
}

// Close stops the cleanup goroutine.
func (pfc *ParquetFileCache) Close() {
	if pfc.stopCleanup != nil {
		pfc.stopCleanup()
	}
	pfc.cleanupWG.Wait()
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

// TrackFile adds or updates a file in the cache tracking.
// Should be called after a file is downloaded.
func (pfc *ParquetFileCache) TrackFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	pfc.mu.Lock()
	defer pfc.mu.Unlock()

	existing, exists := pfc.files[path]
	if exists {
		// Update existing entry
		pfc.totalBytes -= existing.size
		existing.size = info.Size()
		existing.lastAccess = time.Now()
		pfc.totalBytes += existing.size
	} else {
		// Add new entry
		pfc.files[path] = &cachedFileInfo{
			path:       path,
			size:       info.Size(),
			lastAccess: time.Now(),
		}
		pfc.fileCount++
		pfc.totalBytes += info.Size()
	}

	return nil
}

// TouchFile updates the last access time for a file, keeping it alive.
// Returns true if the file exists in the cache.
func (pfc *ParquetFileCache) TouchFile(path string) bool {
	pfc.mu.Lock()
	defer pfc.mu.Unlock()

	if info, exists := pfc.files[path]; exists {
		info.lastAccess = time.Now()
		return true
	}
	return false
}

// HasFile checks if a file exists in the cache and touches it if found.
// This is used by the downloader to check before downloading.
// Returns false for files marked for deletion (even if still on disk).
func (pfc *ParquetFileCache) HasFile(path string) bool {
	// First check our tracking
	pfc.mu.RLock()
	info, tracked := pfc.files[path]
	markedForDeletion := tracked && !info.deleteAfter.IsZero()
	pfc.mu.RUnlock()

	// If marked for deletion, treat as not present (will be re-downloaded)
	if markedForDeletion {
		return false
	}

	if tracked {
		// Touch to keep alive
		pfc.TouchFile(path)
		return true
	}

	// File might exist but not be tracked (e.g., from previous run)
	// Check filesystem and track if found
	if stat, err := os.Stat(path); err == nil {
		pfc.mu.Lock()
		defer pfc.mu.Unlock()

		// Double-check after acquiring write lock
		if _, exists := pfc.files[path]; !exists {
			pfc.files[path] = &cachedFileInfo{
				path:       path,
				size:       stat.Size(),
				lastAccess: time.Now(),
			}
			pfc.fileCount++
			pfc.totalBytes += stat.Size()
		}
		return true
	}

	return false
}

// RemoveFile removes a file from tracking and deletes it from disk.
func (pfc *ParquetFileCache) RemoveFile(path string) {
	pfc.mu.Lock()
	if info, exists := pfc.files[path]; exists {
		pfc.totalBytes -= info.size
		pfc.fileCount--
		delete(pfc.files, path)
	}
	pfc.mu.Unlock()

	// Best-effort delete from disk
	_ = os.Remove(path)
}

// MarkForDeletion marks a file for delayed deletion. The file will be deleted
// after DefaultDeletionDelay has passed. Until then, the file remains on disk
// but HasFile will return false, causing re-downloads if needed.
func (pfc *ParquetFileCache) MarkForDeletion(path string) {
	pfc.mu.Lock()
	defer pfc.mu.Unlock()

	if info, exists := pfc.files[path]; exists {
		info.deleteAfter = time.Now().Add(DefaultDeletionDelay)
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

// cleanupLoop periodically removes expired files.
func (pfc *ParquetFileCache) cleanupLoop(ctx context.Context) {
	defer pfc.cleanupWG.Done()

	ticker := time.NewTicker(pfc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pfc.cleanupExpiredFiles()
		}
	}
}

// cleanupExpiredFiles removes files that haven't been accessed within the TTL
// or have been marked for deletion and their deletion time has passed.
func (pfc *ParquetFileCache) cleanupExpiredFiles() {
	now := time.Now()
	cutoff := now.Add(-pfc.fileTTL)

	var toRemove []string

	pfc.mu.RLock()
	for path, info := range pfc.files {
		// Remove if TTL expired
		if info.lastAccess.Before(cutoff) {
			toRemove = append(toRemove, path)
			continue
		}
		// Remove if marked for deletion and delay has passed
		if !info.deleteAfter.IsZero() && now.After(info.deleteAfter) {
			toRemove = append(toRemove, path)
		}
	}
	pfc.mu.RUnlock()

	if len(toRemove) == 0 {
		return
	}

	slog.Info("Cleaning up expired parquet files",
		slog.Int("count", len(toRemove)),
		slog.Duration("ttl", pfc.fileTTL))

	for _, path := range toRemove {
		pfc.RemoveFile(path)
	}

	// Try to remove empty directories
	pfc.cleanupEmptyDirs()
}

// cleanupEmptyDirs removes empty directories under the cache base dir.
func (pfc *ParquetFileCache) cleanupEmptyDirs() {
	_ = filepath.Walk(ParquetCacheBaseDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if !info.IsDir() {
			return nil
		}
		if path == ParquetCacheBaseDir {
			return nil // don't remove base dir
		}

		// Try to remove - will fail if not empty
		_ = os.Remove(path)
		return nil
	})
}

// ScanExistingFiles scans the cache directory and tracks any existing files.
// This is useful on startup to recover state from previous runs.
func (pfc *ParquetFileCache) ScanExistingFiles() error {
	return filepath.Walk(ParquetCacheBaseDir, func(path string, info os.FileInfo, err error) error {
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
			pfc.files[path] = &cachedFileInfo{
				path:       path,
				size:       info.Size(),
				lastAccess: info.ModTime(), // Use file mtime as last access
			}
			pfc.fileCount++
			pfc.totalBytes += info.Size()
		}
		pfc.mu.Unlock()

		return nil
	})
}
