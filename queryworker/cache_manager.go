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
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// isMissingFingerprintError checks if the error is about a missing chq_fingerprint column.
// Only explicit column not found errors are treated as missing fingerprint issues.
// Timeouts and other errors are not classified as fingerprint-related to avoid wasting
// time on retries that won't fix the underlying issue.
func isMissingFingerprintError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()

	// Explicit column not found error
	if strings.Contains(errStr, "chq_fingerprint") &&
		(strings.Contains(errStr, "not found") || strings.Contains(errStr, "Binder Error")) {
		return true
	}

	return false
}

// removeFingerprintNormalization removes the fingerprint normalization stage from SQL.
// This handles cases where older Parquet files don't have the chq_fingerprint column.
func removeFingerprintNormalization(sql string) string {
	// The fingerprint normalization appears as:
	// s1 AS (SELECT s0.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint") FROM s0)
	// We replace the REPLACE clause with a simple SELECT *
	modified := strings.ReplaceAll(sql,
		`.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint")`,
		`.*`)

	return modified
}

// DownloadBatchFunc downloads ALL given paths to their target local paths.
type DownloadBatchFunc func(ctx context.Context, storageProfile storageprofile.StorageProfile, keys []string) error

// CacheManager coordinates downloads and queries over local parquet files.
type CacheManager struct {
	pool                   *duckdbx.DB // shared global pool
	downloader             DownloadBatchFunc
	storageProfileProvider storageprofile.StorageProfileProvider
	dataset                string
	parquetCache           *ParquetFileCache // shared parquet file cache for cleanup coordination

	profilesByOrgInstanceNum map[uuid.UUID]map[int16]storageprofile.StorageProfile
	profilesMu               sync.RWMutex
}

func NewCacheManager(dl DownloadBatchFunc, dataset string, storageProfileProvider storageprofile.StorageProfileProvider, pool *duckdbx.DB, parquetCache *ParquetFileCache) *CacheManager {
	if parquetCache == nil {
		slog.Error("parquetCache is required but was nil")
		return nil
	}

	w := &CacheManager{
		pool:                     pool,
		dataset:                  dataset,
		storageProfileProvider:   storageProfileProvider,
		profilesByOrgInstanceNum: make(map[uuid.UUID]map[int16]storageprofile.StorageProfile),
		downloader:               dl,
		parquetCache:             parquetCache,
	}

	slog.Info("CacheManager initialized",
		slog.String("dataset", dataset))

	return w
}

func (w *CacheManager) Close() {
	// No-op - resources are managed externally
}

func (w *CacheManager) getProfile(ctx context.Context, orgID uuid.UUID, inst int16) (storageprofile.StorageProfile, error) {
	// Fast path: read under RLock
	w.profilesMu.RLock()
	if byInst, ok := w.profilesByOrgInstanceNum[orgID]; ok {
		if p, ok2 := byInst[inst]; ok2 {
			w.profilesMu.RUnlock()
			return p, nil
		}
	}
	w.profilesMu.RUnlock()

	// Fetch outside locks
	p, err := w.storageProfileProvider.GetStorageProfileForOrganizationAndInstance(ctx, orgID, inst)
	if err != nil {
		return storageprofile.StorageProfile{}, fmt.Errorf(
			"get storage profile for org %s instance %d: %w", orgID.String(), inst, err)
	}

	// Write path with double-check
	w.profilesMu.Lock()
	defer w.profilesMu.Unlock()

	if _, ok := w.profilesByOrgInstanceNum[orgID]; !ok {
		w.profilesByOrgInstanceNum[orgID] = make(map[int16]storageprofile.StorageProfile)
	}
	if existing, ok := w.profilesByOrgInstanceNum[orgID][inst]; ok {
		return existing, nil
	}
	w.profilesByOrgInstanceNum[orgID][inst] = p
	return p, nil
}

// downloadForQuery downloads all files from S3 in parallel before querying.
func (w *CacheManager) downloadForQuery(ctx context.Context, profile storageprofile.StorageProfile, localPaths []string) error {
	if w.downloader == nil || len(localPaths) == 0 {
		return nil
	}

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
	ctx, downloadSpan := tracer.Start(ctx, "query.worker.download_for_query")
	defer downloadSpan.End()

	downloadSpan.SetAttributes(
		attribute.Int("file_count", len(localPaths)),
		attribute.String("bucket", profile.Bucket),
		attribute.String("dataset", w.dataset),
	)

	if err := w.downloader(ctx, profile, localPaths); err != nil {
		downloadSpan.RecordError(err)
		downloadSpan.SetStatus(codes.Error, "download failed")
		return fmt.Errorf("download files for query: %w", err)
	}

	return nil
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
}
