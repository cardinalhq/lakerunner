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
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type ParquetCache struct {
	cacheDir        string
	maxSizeMB       int64
	awsManager      *awsclient.Manager
	storageProfiler storageprofile.StorageProfileProvider
	enableCache     bool

	mu          sync.RWMutex
	cacheIndex  map[string]*CacheEntry
	totalSizeMB int64
}

type CacheEntry struct {
	LocalPath  string
	Size       int64
	AccessTime time.Time
}

type ParquetCacheConfig struct {
	CacheDir        string
	MaxSizeMB       int64
	AWSManager      *awsclient.Manager
	StorageProfiler storageprofile.StorageProfileProvider
	EnableCache     bool
}

func NewParquetCache(cfg ParquetCacheConfig) (*ParquetCache, error) {
	if cfg.EnableCache {
		if err := os.MkdirAll(cfg.CacheDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create cache directory: %w", err)
		}
	}

	cache := &ParquetCache{
		cacheDir:        cfg.CacheDir,
		maxSizeMB:       cfg.MaxSizeMB,
		awsManager:      cfg.AWSManager,
		storageProfiler: cfg.StorageProfiler,
		enableCache:     cfg.EnableCache,
		cacheIndex:      make(map[string]*CacheEntry),
	}

	if cfg.EnableCache {
		if err := cache.loadCacheIndex(); err != nil {
			slog.Warn("Failed to load cache index", "error", err)
		}
	}

	return cache, nil
}

func (c *ParquetCache) GetFile(ctx context.Context, organizationID uuid.UUID, s3Key string) (string, error) {
	if !c.enableCache {
		return c.downloadToTemp(ctx, organizationID, s3Key)
	}

	c.mu.RLock()
	entry, exists := c.cacheIndex[s3Key]
	c.mu.RUnlock()

	if exists {
		// Check if file still exists (immutable files never expire)
		if _, err := os.Stat(entry.LocalPath); err == nil {
			// Update access time for LRU tracking
			c.mu.Lock()
			entry.AccessTime = time.Now()
			c.mu.Unlock()
			slog.Debug("Cache hit", "s3Key", s3Key, "localPath", entry.LocalPath)
			return entry.LocalPath, nil
		}
		// Remove stale entry if file was deleted externally
		c.mu.Lock()
		delete(c.cacheIndex, s3Key)
		c.totalSizeMB -= entry.Size / (1024 * 1024)
		c.mu.Unlock()
	}

	// Cache miss - download file
	slog.Debug("Cache miss", "s3Key", s3Key)
	return c.downloadAndCache(ctx, organizationID, s3Key)
}

func (c *ParquetCache) downloadAndCache(ctx context.Context, organizationID uuid.UUID, s3Key string) (string, error) {
	// Check if we need to make space
	c.evictIfNeeded()

	localPath := filepath.Join(c.cacheDir, filepath.Base(s3Key))
	if err := c.downloadFile(ctx, organizationID, s3Key, localPath); err != nil {
		return "", err
	}

	stat, err := os.Stat(localPath)
	if err != nil {
		return "", fmt.Errorf("failed to stat downloaded file: %w", err)
	}

	entry := &CacheEntry{
		LocalPath:  localPath,
		Size:       stat.Size(),
		AccessTime: time.Now(),
	}

	c.mu.Lock()
	c.cacheIndex[s3Key] = entry
	c.totalSizeMB += stat.Size() / (1024 * 1024)
	c.mu.Unlock()

	slog.Debug("File cached", "s3Key", s3Key, "localPath", localPath, "sizeMB", stat.Size()/(1024*1024))
	return localPath, nil
}

func (c *ParquetCache) downloadToTemp(ctx context.Context, organizationID uuid.UUID, s3Key string) (string, error) {
	tempFile, err := os.CreateTemp("", "query-worker-*.parquet")
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	tempPath := tempFile.Name()
	tempFile.Close()

	if err := c.downloadFile(ctx, organizationID, s3Key, tempPath); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	return tempPath, nil
}

// getStorageProfile gets the storage profile for an organization
func (c *ParquetCache) getStorageProfile(ctx context.Context, organizationID uuid.UUID) (storageprofile.StorageProfile, error) {
	return c.storageProfiler.GetStorageProfileForOrganization(ctx, organizationID)
}

func (c *ParquetCache) downloadFile(ctx context.Context, organizationID uuid.UUID, s3Key, localPath string) error {
	slog.Debug("Downloading file", "s3Key", s3Key, "localPath", localPath)

	// Get storage profile for this organization
	profile, err := c.getStorageProfile(ctx, organizationID)
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Get S3 client for this profile
	s3Client, err := c.awsManager.GetS3ForProfile(ctx, profile)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	resp, err := s3Client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(profile.Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to get S3 object: %w", err)
	}
	defer resp.Body.Close()

	file, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return fmt.Errorf("failed to copy S3 object to local file: %w", err)
	}

	return nil
}

func (c *ParquetCache) evictIfNeeded() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.totalSizeMB <= c.maxSizeMB {
		return
	}

	slog.Info("Cache size exceeded, evicting files", "totalSizeMB", c.totalSizeMB, "maxSizeMB", c.maxSizeMB)

	// Sort entries by access time (LRU eviction)
	type entryWithKey struct {
		key   string
		entry *CacheEntry
	}

	var entries []entryWithKey
	for key, entry := range c.cacheIndex {
		entries = append(entries, entryWithKey{key: key, entry: entry})
	}

	// Sort by access time (oldest first)
	for i := 0; i < len(entries)-1; i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[i].entry.AccessTime.After(entries[j].entry.AccessTime) {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}

	// Evict files until we're under the limit
	for _, e := range entries {
		if c.totalSizeMB <= c.maxSizeMB*8/10 { // Evict to 80% of max
			break
		}

		if err := os.Remove(e.entry.LocalPath); err != nil {
			slog.Warn("Failed to remove cached file", "path", e.entry.LocalPath, "error", err)
		} else {
			slog.Debug("Evicted cached file", "s3Key", e.key, "localPath", e.entry.LocalPath)
		}

		c.totalSizeMB -= e.entry.Size / (1024 * 1024)
		delete(c.cacheIndex, e.key)
	}
}

func (c *ParquetCache) loadCacheIndex() error {
	entries, err := os.ReadDir(c.cacheDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		path := filepath.Join(c.cacheDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Use filename as S3 key (simplified)
		cacheEntry := &CacheEntry{
			LocalPath:  path,
			Size:       info.Size(),
			AccessTime: info.ModTime(), // Use last modified as access time on startup
		}

		c.cacheIndex[entry.Name()] = cacheEntry
		c.totalSizeMB += info.Size() / (1024 * 1024)
	}

	slog.Info("Loaded cache index", "files", len(c.cacheIndex), "totalSizeMB", c.totalSizeMB)
	return nil
}

func (c *ParquetCache) Close() error {
	if !c.enableCache {
		return nil
	}

	slog.Info("Closing parquet cache", "totalFiles", len(c.cacheIndex), "totalSizeMB", c.totalSizeMB)
	return nil
}
