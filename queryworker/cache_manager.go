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
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
)

const (
	ChannelBufferSize = 4096
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

// executeQueryWithRetry executes a DuckDB query with automatic retry logic for missing fingerprint errors.
// If the query fails due to a missing chq_fingerprint column, it retries with fingerprint normalization removed.
// NOTE: This function uses the parent context directly (no separate timeout) because the returned *sql.Rows
// object continues to use the context during row scanning. Creating a local timeout context and cancelling it
// on function return causes "context canceled" errors during rows.Next()/rows.Scan().
func executeQueryWithRetry(ctx context.Context, conn *sql.Conn, query string, queryType string, numItems int) (*sql.Rows, error) {
	slog.Info("Calling DuckDB QueryContext", slog.String("queryType", queryType), slog.Int("numItems", numItems))
	queryStart := time.Now()
	rows, err := conn.QueryContext(ctx, query)
	queryDuration := time.Since(queryStart)
	slog.Info("DuckDB QueryContext returned",
		slog.String("queryType", queryType),
		slog.Duration("duration", queryDuration),
		slog.Bool("hasError", err != nil))

	if err != nil {
		if isMissingFingerprintError(err) {
			slog.Warn("Segments missing chq_fingerprint column, retrying without normalization",
				slog.String("queryType", queryType),
				slog.Any("error", err),
				slog.Int("numItems", numItems))

			modifiedSQL := removeFingerprintNormalization(query)
			rows, err = conn.QueryContext(ctx, modifiedSQL)
			if err != nil {
				slog.Error("Query failed even without fingerprint normalization",
					slog.String("queryType", queryType),
					slog.Any("error", err),
					slog.String("sql", modifiedSQL))
				return nil, err
			}
			return rows, nil
		}

		slog.Error("Query failed",
			slog.String("queryType", queryType),
			slog.Any("error", err),
			slog.String("sql", query))
		return nil, err
	}

	return rows, nil
}

// DownloadBatchFunc downloads ALL given paths to their target local paths.
type DownloadBatchFunc func(ctx context.Context, storageProfile storageprofile.StorageProfile, keys []string) error

// RowMapper turns the current row into a T.
type RowMapper[T promql.Timestamped] func(queryapi.PushDownRequest, []string, *sql.Rows) (T, error)

type ingestJob struct {
	profile   storageprofile.StorageProfile
	objectIDs []string
	ids       []int64
}

// CacheManager coordinates downloads, batch-ingest, queries, and LRU evictions.
type CacheManager struct {
	sink                   *DDBSink
	s3Pool                 *duckdbx.S3DB // shared global pool
	maxRows                int64
	downloader             DownloadBatchFunc
	storageProfileProvider storageprofile.StorageProfileProvider
	dataset                string
	disableTableCache      bool              // when true, skip DuckDB cache entirely
	parquetCache           *ParquetFileCache // shared parquet file cache for cleanup coordination

	// in-memory presence + LRU tracking
	mu         sync.RWMutex
	present    map[int64]struct{}  // segmentID -> exists in cache
	lastAccess map[int64]time.Time // segmentID -> last access
	inflight   map[int64]*struct{} // singleflight per segmentID

	profilesByOrgInstanceNum map[uuid.UUID]map[int16]storageprofile.StorageProfile
	profilesMu               sync.RWMutex // <--- add this

	ingestQ    chan ingestJob
	stopIngest context.CancelFunc
	ingestWG   sync.WaitGroup

	// metrics
	evictionDurationHist metric.Float64Histogram
}

const (
	MaxRowsDefault = 1000000000 // 1 billion rows (approx 10GB on disk)

	// DiskUsageHighWatermark is the disk utilization level that triggers eviction.
	// When disk usage exceeds this fraction, CacheManagers will start evicting.
	DiskUsageHighWatermark = 0.80

	// DiskUsageLowWatermark is the target disk utilization after eviction.
	// Eviction continues until disk usage drops below this level.
	DiskUsageLowWatermark = 0.70

	// MinDiskUsageBytes is the minimum disk usage limit (1GB).
	// This prevents unreasonably small limits on tiny test volumes.
	MinDiskUsageBytes = 1 << 30 // 1 GB

	// DefaultDiskUsageBytes is the fallback if disk size detection fails (8GB).
	DefaultDiskUsageBytes = 8 << 30 // 8 GB
)

func NewCacheManager(dl DownloadBatchFunc, dataset string, storageProfileProvider storageprofile.StorageProfileProvider, s3Pool *duckdbx.S3DB, disableTableCache bool, parquetCache *ParquetFileCache) *CacheManager {
	var ddb *DDBSink
	var err error

	// Only create DuckDB sink if table cache is enabled
	if !disableTableCache {
		ddb, err = NewDDBSink(dataset, context.Background(), s3Pool)
		if err != nil {
			slog.Error("Failed to create DuckDB sink", slog.Any("error", err))
			return nil
		}
	}

	w := &CacheManager{
		sink:                     ddb,
		s3Pool:                   s3Pool,
		dataset:                  dataset,
		storageProfileProvider:   storageProfileProvider,
		profilesByOrgInstanceNum: make(map[uuid.UUID]map[int16]storageprofile.StorageProfile),
		maxRows:                  MaxRowsDefault,
		downloader:               dl,
		disableTableCache:        disableTableCache,
		parquetCache:             parquetCache,
		present:                  make(map[int64]struct{}, ChannelBufferSize),
		lastAccess:               make(map[int64]time.Time, ChannelBufferSize),
		inflight:                 make(map[int64]*struct{}, ChannelBufferSize),
		ingestQ:                  make(chan ingestJob, 64),
	}

	slog.Info("CacheManager initialized",
		slog.String("dataset", dataset),
		slog.Int64("maxRows", w.maxRows),
		slog.Bool("disableTableCache", disableTableCache),
		slog.Float64("diskHighWatermark", DiskUsageHighWatermark),
		slog.Float64("diskLowWatermark", DiskUsageLowWatermark))

	// Only start ingest loop if table cache is enabled
	if !disableTableCache {
		ingCtx, ingCancel := context.WithCancel(context.Background())
		w.stopIngest = ingCancel
		w.ingestWG.Add(1)
		go w.ingestLoop(ingCtx)
	}
	return w
}

func (w *CacheManager) Close() {
	if w.stopIngest != nil {
		w.stopIngest()
	}
	w.ingestWG.Wait()
}

// RegisterCacheMetrics registers OTEL metrics for multiple CacheManagers.
// This must be called once with all CacheManagers to avoid callback conflicts.
func RegisterCacheMetrics(managers ...*CacheManager) error {
	meter := otel.Meter("lakerunner.querycache")

	// Row count metrics
	_, err := meter.Int64ObservableGauge(
		"lakerunner.querycache.rows",
		metric.WithDescription("Current number of rows in the query cache"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for _, w := range managers {
				if w != nil && w.sink != nil {
					o.Observe(w.sink.RowCount(), metric.WithAttributes(attribute.String("signal", w.dataset)))
				}
			}
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create rows gauge: %w", err)
	}

	_, err = meter.Int64ObservableGauge(
		"lakerunner.querycache.rows_limit",
		metric.WithDescription("Configured row limit for the query cache"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for _, w := range managers {
				if w != nil {
					o.Observe(w.maxRows, metric.WithAttributes(attribute.String("signal", w.dataset)))
				}
			}
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create rows_limit gauge: %w", err)
	}

	_, err = meter.Float64ObservableGauge(
		"lakerunner.querycache.rows_utilization",
		metric.WithDescription("Utilization of max rows in the query cache (0-1, may exceed 1 if over limit)"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			for _, w := range managers {
				if w != nil && w.sink != nil && w.maxRows > 0 {
					o.Observe(float64(w.sink.RowCount())/float64(w.maxRows), metric.WithAttributes(attribute.String("signal", w.dataset)))
				}
			}
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create rows_utilization gauge: %w", err)
	}

	// Disk usage metrics
	_, err = meter.Int64ObservableGauge(
		"lakerunner.querycache.disk_bytes",
		metric.WithDescription("Current disk usage in bytes"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for _, w := range managers {
				if w != nil {
					dbPath := w.s3Pool.GetDatabasePath()
					usedBytes, _, err := getDiskUsage(dbPath)
					if err == nil {
						o.Observe(int64(usedBytes), metric.WithAttributes(attribute.String("signal", w.dataset)))
					}
				}
			}
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create disk_bytes gauge: %w", err)
	}

	// Report disk utilization - shared across all signal types (not per-signal)
	// Only report once since it's the same filesystem for all managers
	_, err = meter.Float64ObservableGauge(
		"lakerunner.querycache.disk_utilization",
		metric.WithDescription("Disk utilization (0-1), shared across all signal types"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			// Only need to check once since all managers share the same disk
			for _, w := range managers {
				if w != nil {
					dbPath := w.s3Pool.GetDatabasePath()
					usedBytes, totalBytes, err := getDiskUsage(dbPath)
					if err == nil && totalBytes > 0 {
						o.Observe(float64(usedBytes) / float64(totalBytes))
					}
					break // Only report once
				}
			}
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create disk_utilization gauge: %w", err)
	}

	_, err = meter.Float64ObservableGauge(
		"lakerunner.querycache.disk_high_watermark",
		metric.WithDescription("Disk utilization threshold that triggers eviction"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(DiskUsageHighWatermark)
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create disk_high_watermark gauge: %w", err)
	}

	// LRU depth (number of segments tracked)
	_, err = meter.Int64ObservableGauge(
		"lakerunner.querycache.lru_depth",
		metric.WithDescription("Number of segments tracked in the LRU cache"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			for _, w := range managers {
				if w != nil {
					w.mu.RLock()
					depth := len(w.present)
					w.mu.RUnlock()
					o.Observe(int64(depth), metric.WithAttributes(attribute.String("signal", w.dataset)))
				}
			}
			return nil
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to create lru_depth gauge: %w", err)
	}

	// Eviction duration histogram - register once, each manager records with its own signal attribute
	hist, err := meter.Float64Histogram(
		"lakerunner.querycache.eviction_duration_seconds",
		metric.WithDescription("Duration of eviction cycles in seconds"),
	)
	if err != nil {
		return fmt.Errorf("failed to create eviction_duration histogram: %w", err)
	}
	for _, w := range managers {
		if w != nil {
			w.evictionDurationHist = hist
		}
	}

	return nil
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

func EvaluatePushDown[T promql.Timestamped](
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	userSQL string,
	s3GlobSize int,
	mapper RowMapper[T],
) (<-chan T, error) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
	ctx, evalSpan := tracer.Start(ctx, "query.worker.evaluate_pushdown")
	defer evalSpan.End()

	evalSpan.SetAttributes(
		attribute.String("dataset", w.dataset),
		attribute.Int("segment_count", len(request.Segments)),
		attribute.Int("s3_glob_size", s3GlobSize),
	)

	if len(request.Segments) == 0 {
		evalSpan.SetStatus(codes.Error, "no segment paths")
		return nil, errors.New("no segment paths")
	}
	if mapper == nil {
		evalSpan.SetStatus(codes.Error, "nil RowMapper")
		return nil, errors.New("nil RowMapper")
	}
	if !strings.Contains(userSQL, "{table}") {
		evalSpan.SetStatus(codes.Error, "invalid SQL template")
		return nil, errors.New(`userSQL must contain "{table}" placeholder`)
	}

	// Group segments by orgId/instanceNum (no profile map mutation here)
	ctx, groupSpan := tracer.Start(ctx, "query.worker.group_segments")
	segmentsByOrg := make(map[uuid.UUID]map[int16][]queryapi.SegmentInfo)
	start := time.Now()
	for _, seg := range request.Segments {
		if _, ok := segmentsByOrg[seg.OrganizationID]; !ok {
			segmentsByOrg[seg.OrganizationID] = make(map[int16][]queryapi.SegmentInfo)
		}
		segmentsByOrg[seg.OrganizationID][seg.InstanceNum] =
			append(segmentsByOrg[seg.OrganizationID][seg.InstanceNum], seg)
	}
	metadataCreationTime := time.Since(start)
	groupSpan.SetAttributes(attribute.Int("org_instance_groups", len(segmentsByOrg)))
	groupSpan.End()
	slog.Info("Metadata Creation Time", "duration", metadataCreationTime.String())
	start = time.Now()

	outs := make([]<-chan T, 0)
	var totalS3Segments, totalCachedSegments int

	// Build channels per (org, instance)
	for orgId, instances := range segmentsByOrg {
		for instanceNum, segments := range instances {
			profile, err := w.getProfile(ctx, orgId, instanceNum)
			if err != nil {
				evalSpan.RecordError(err)
				evalSpan.SetStatus(codes.Error, "failed to get profile")
				return nil, err
			}

			// Split into cached vs S3 (when table cache is disabled, treat all as S3)
			var objectIDs []string
			var s3IDs []int64
			var cachedIDs []int64

			for _, seg := range segments {
				objectID := fmt.Sprintf("db/%s/%s/%d/%s/%s/tbl_%d.parquet",
					orgId.String(),
					profile.CollectorName,
					seg.DateInt,
					w.dataset,
					seg.Hour,
					seg.SegmentID)

				inCache := false
				if !w.disableTableCache {
					w.mu.RLock()
					_, inCache = w.present[seg.SegmentID]
					w.mu.RUnlock()
				}

				if inCache {
					cachedIDs = append(cachedIDs, seg.SegmentID)
				} else {
					objectIDs = append(objectIDs, objectID)
					s3IDs = append(s3IDs, seg.SegmentID)
				}
			}

			totalS3Segments += len(objectIDs)
			totalCachedSegments += len(cachedIDs)

			// Safe logging (len(w.present) under RLock)
			w.mu.RLock()
			numPresent := len(w.present)
			w.mu.RUnlock()
			slog.Info("Segment Stats",
				"numS3", len(objectIDs),
				"numCached", len(cachedIDs),
				"numPresent", numPresent)

			// Download files from S3 before querying (new flow: download-then-query)
			if len(objectIDs) > 0 {
				if err := w.downloadForQuery(ctx, profile, objectIDs); err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to download from S3")
					return nil, fmt.Errorf("download from S3: %w", err)
				}
			}

			// Convert object IDs to local paths for streaming (cache has already downloaded them)
			localPaths := make([]string, len(objectIDs))
			for i, objID := range objectIDs {
				localPath, exists, err := w.parquetCache.GetOrPrepare(profile.Region, profile.Bucket, objID)
				if err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to get local path")
					return nil, fmt.Errorf("get local path for %s: %w", objID, err)
				}
				if !exists {
					slog.Warn("Expected file to exist after download", "objectID", objID, "localPath", localPath)
				}
				localPaths[i] = localPath
			}

			// Stream from downloaded local files (not S3 directly).
			s3Channels, err := streamFromLocalFiles(ctx, w, request,
				localPaths,
				userSQL,
				mapper)
			if err != nil {
				evalSpan.RecordError(err)
				evalSpan.SetStatus(codes.Error, "failed to stream from local files")
				return nil, fmt.Errorf("stream from local files: %w", err)
			}
			outs = append(outs, s3Channels...)

			// Enqueue uncached segments for background ingest (skip if cache disabled).
			if len(objectIDs) > 0 && !w.disableTableCache {
				w.enqueueIngest(profile, objectIDs, s3IDs)
			}

			// Stream cached segments from the cache.
			cachedChannels := streamCached(ctx, w, request, cachedIDs, userSQL, mapper)
			outs = append(outs, cachedChannels...)
		}
	}
	channelCreationTime := time.Since(start)
	slog.Info("Channel Creation Time", "duration", channelCreationTime.String())

	evalSpan.SetAttributes(
		attribute.Int("s3_segments", totalS3Segments),
		attribute.Int("cached_segments", totalCachedSegments),
		attribute.Int("output_channels", len(outs)),
	)

	// Merge all sources (cached + S3 batches) in timestamp order.
	return promql.MergeSorted(ctx, ChannelBufferSize, request.Reverse, request.Limit, outs...), nil
}

func streamCached[T promql.Timestamped](ctx context.Context, w *CacheManager,
	request queryapi.PushDownRequest,
	cachedIDs []int64,
	userSQL string, mapper RowMapper[T]) []<-chan T {
	outs := make([]<-chan T, 0)

	if len(cachedIDs) > 0 {
		out := make(chan T, ChannelBufferSize)
		outs = append(outs, out)

		// Touch lastAccess for cached segments
		w.mu.Lock()
		now := time.Now()
		for _, id := range cachedIDs {
			w.lastAccess[id] = now
		}
		w.mu.Unlock()

		go func(ids []int64, out chan<- T) {
			defer close(out)

			tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
			_, cacheSpan := tracer.Start(ctx, "query.worker.stream_from_cache")
			defer cacheSpan.End()

			cacheSpan.SetAttributes(
				attribute.Int("segment_count", len(ids)),
				attribute.String("table", w.sink.table),
			)

			idLits := make([]string, len(ids))
			for i, id := range ids {
				idLits[i] = strconv.FormatInt(id, 10)
			}
			inList := strings.Join(idLits, ",")

			// Replace {table} with cached table; replace sentinel "AND true" with segment filter.
			cacheSQL := ""
			if request.LogLeaf != nil || (request.BaseExpr != nil && request.BaseExpr.LogLeaf != nil) {
				cacheBase := fmt.Sprintf("(SELECT * FROM %s WHERE segment_id IN (%s))", w.sink.table, inList)
				cacheSQL = strings.Replace(userSQL, "{table}", cacheBase, 1)
			} else {
				cacheSQL = strings.Replace(userSQL, "{table}", w.sink.table, 1)
				cacheSQL = strings.Replace(cacheSQL, "AND true", "AND segment_id IN ("+inList+")", 1)
			}

			// Get connection from shared pool for local queries
			_, connSpan := tracer.Start(ctx, "query.worker.cache_connection_acquire")
			conn, release, err := w.sink.s3Pool.GetConnection(ctx)
			connSpan.End()
			if err != nil {
				cacheSpan.RecordError(err)
				cacheSpan.SetStatus(codes.Error, "connection acquire failed")
				slog.Error("Failed to get connection", slog.Any("error", err))
				return
			}
			defer release()

			_, querySpan := tracer.Start(ctx, "query.worker.cache_query_execute")
			rows, err := executeQueryWithRetry(ctx, conn, cacheSQL, "cached segments", len(ids))
			if err != nil {
				querySpan.RecordError(err)
				querySpan.SetStatus(codes.Error, "query failed")
				querySpan.End()
				cacheSpan.RecordError(err)
				cacheSpan.SetStatus(codes.Error, "query failed")
				return
			}
			querySpan.End()
			defer func(rows *sql.Rows) {
				err := rows.Close()
				if err != nil {
					slog.Error("Error closing rows", slog.Any("error", err))
				}
			}(rows)

			cols, err := rows.Columns()
			if err != nil {
				cacheSpan.RecordError(err)
				cacheSpan.SetStatus(codes.Error, "failed to get columns")
				slog.Error("failed to get columns", "err", err)
				return
			}

			rowCount := 0
			for rows.Next() {
				select {
				case <-ctx.Done():
					cacheSpan.SetAttributes(attribute.Int("rows_returned", rowCount))
					cacheSpan.SetStatus(codes.Error, "context cancelled")
					slog.Warn("Context cancelled during cached row iteration", slog.Int("numSegments", len(ids)), slog.Int("rowsProcessed", rowCount))
					return
				default:
				}
				v, mErr := mapper(request, cols, rows)
				if mErr != nil {
					cacheSpan.RecordError(mErr)
					cacheSpan.SetStatus(codes.Error, "row mapping failed")
					slog.Error("Cached row mapping failed", slog.Any("error", mErr))
					return
				}
				out <- v
				rowCount++
			}
			if err := rows.Err(); err != nil {
				cacheSpan.RecordError(err)
				cacheSpan.SetStatus(codes.Error, "rows iteration error")
				slog.Error("Cached rows iteration error", slog.Any("error", err))
				return
			}
			cacheSpan.SetAttributes(attribute.Int("rows_returned", rowCount))
			slog.Info("Cached query completed", slog.Int("numSegments", len(ids)), slog.Int("rowsReturned", rowCount))
		}(cachedIDs, out)
	}
	return outs
}

func streamFromLocalFiles[T promql.Timestamped](
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	localPaths []string,
	userSQL string,
	mapper RowMapper[T],
) ([]<-chan T, error) {
	if len(localPaths) == 0 {
		return []<-chan T{}, nil
	}

	// Single batch per worker: one channel, one goroutine
	out := make(chan T, ChannelBufferSize)
	outs := []<-chan T{out}

	pathsCopy := append([]string(nil), localPaths...) // capture

	go func(out chan<- T) {
		defer close(out)

		tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
		_, localSpan := tracer.Start(ctx, "query.worker.stream_from_local_files")
		defer localSpan.End()

		localSpan.SetAttributes(
			attribute.Int("file_count", len(pathsCopy)),
			attribute.String("dataset", w.dataset),
		)

		// Build read_parquet source from local paths
		quoted := make([]string, len(pathsCopy))
		for i := range pathsCopy {
			quoted[i] = "'" + escapeSQL(pathsCopy[i]) + "'"
		}
		array := "[" + strings.Join(quoted, ", ") + "]"
		src := fmt.Sprintf(`read_parquet(%s, union_by_name=true)`, array)

		sqlReplaced := strings.Replace(userSQL, "{table}", src, 1)

		// Get a local connection (no S3 credentials needed for local files)
		_, connSpan := tracer.Start(ctx, "query.worker.local_connection_acquire")
		start := time.Now()
		conn, release, err := w.s3Pool.GetConnection(ctx)
		connectionAcquireTime := time.Since(start)
		connSpan.SetAttributes(attribute.Int64("duration_ms", connectionAcquireTime.Milliseconds()))
		connSpan.End()
		slog.Info("Local Connection Acquire Time", "duration", connectionAcquireTime.String())

		if err != nil {
			localSpan.RecordError(err)
			localSpan.SetStatus(codes.Error, "connection acquire failed")
			slog.Error("GetConnection failed", slog.Any("error", err))
			return
		}
		defer release()

		_, querySpan := tracer.Start(ctx, "query.worker.local_query_execute")
		rows, err := executeQueryWithRetry(ctx, conn, sqlReplaced, "local files", len(pathsCopy))
		if err != nil {
			querySpan.RecordError(err)
			querySpan.SetStatus(codes.Error, "query failed")
			querySpan.End()
			localSpan.RecordError(err)
			localSpan.SetStatus(codes.Error, "query failed")
			return
		}
		querySpan.End()
		defer func() {
			if err := rows.Close(); err != nil {
				slog.Error("Error closing rows", slog.Any("error", err))
			}
		}()

		cols, err := rows.Columns()
		if err != nil {
			localSpan.RecordError(err)
			localSpan.SetStatus(codes.Error, "failed to get columns")
			slog.Error("failed to get columns", slog.Any("error", err))
			return
		}

		rowCount := 0
		for rows.Next() {
			select {
			case <-ctx.Done():
				localSpan.SetAttributes(attribute.Int("rows_returned", rowCount))
				localSpan.SetStatus(codes.Error, "context cancelled")
				slog.Warn("Context cancelled during local file row iteration", slog.Int("numFiles", len(pathsCopy)), slog.Int("rowsProcessed", rowCount))
				return
			default:
			}
			v, mErr := mapper(request, cols, rows)
			if mErr != nil {
				localSpan.RecordError(mErr)
				localSpan.SetStatus(codes.Error, "row mapping failed")
				slog.Error("Row mapping failed", slog.Any("error", mErr))
				return
			}
			out <- v
			rowCount++
		}
		if err := rows.Err(); err != nil {
			localSpan.RecordError(err)
			localSpan.SetStatus(codes.Error, "rows iteration error")
			slog.Error("Rows iteration error", slog.Any("error", err))
			return
		}
		localSpan.SetAttributes(attribute.Int("rows_returned", rowCount))
		slog.Info("Local file query completed", slog.Int("numFiles", len(pathsCopy)), slog.Int("rowsReturned", rowCount))
	}(out)

	return outs, nil
}

// downloadForQuery downloads all files from S3 in parallel before querying.
// Returns the local paths of successfully downloaded files and their corresponding IDs.
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

// enqueueIngest filters out present/in-flight, marks new IDs in-flight, and queues one job.
func (w *CacheManager) enqueueIngest(storageProfile storageprofile.StorageProfile, objectIDs []string, ids []int64) {
	if len(objectIDs) == 0 || len(objectIDs) != len(ids) {
		return
	}

	// Filter + mark in-flight under lock
	var todoObjectIDs []string
	var todoIDs []int64
	w.mu.Lock()
	for i, id := range ids {
		if _, ok := w.present[id]; ok {
			continue
		}
		if _, in := w.inflight[id]; in {
			continue // someone else is already ingesting
		}
		w.inflight[id] = &struct{}{}
		todoObjectIDs = append(todoObjectIDs, objectIDs[i])
		todoIDs = append(todoIDs, id)
		w.lastAccess[id] = time.Now()
	}
	w.mu.Unlock()

	if len(todoObjectIDs) == 0 {
		return
	}

	// Non-blocking enqueue
	w.ingestQ <- ingestJob{profile: storageProfile, objectIDs: todoObjectIDs, ids: todoIDs}
}

func (w *CacheManager) ingestLoop(ctx context.Context) {
	defer w.ingestWG.Done()

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.ingestQ:
			if len(job.objectIDs) == 0 {
				continue
			}

			region := job.profile.Region
			bucket := job.profile.Bucket

			// Create a span for the entire cache ingest operation
			_, ingestSpan := tracer.Start(ctx, "query.worker.cache_ingest_batch")
			ingestSpan.SetAttributes(
				attribute.Int("file_count", len(job.objectIDs)),
				attribute.String("bucket", bucket),
				attribute.String("dataset", w.dataset),
			)

			// Convert object IDs to local paths and filter out ones that need download
			localPaths := make([]string, len(job.objectIDs))
			var objectIDsToDownload []string
			var prepareErr error
			for i, objID := range job.objectIDs {
				localPath, exists, err := w.parquetCache.GetOrPrepare(region, bucket, objID)
				if err != nil {
					slog.Error("Failed to get local path for ingest", "objectID", objID, "error", err)
					prepareErr = err
					break
				}
				localPaths[i] = localPath
				if !exists {
					objectIDsToDownload = append(objectIDsToDownload, objID)
				}
			}
			if prepareErr != nil {
				ingestSpan.RecordError(prepareErr)
				ingestSpan.SetStatus(codes.Error, "failed to prepare local paths")
				ingestSpan.End()
				w.mu.Lock()
				for _, id := range job.ids {
					delete(w.inflight, id)
				}
				w.mu.Unlock()
				continue
			}

			if w.downloader != nil && len(objectIDsToDownload) > 0 {
				_, downloadSpan := tracer.Start(ctx, "query.worker.cache_download_files")
				downloadSpan.SetAttributes(
					attribute.Int("file_count", len(objectIDsToDownload)),
					attribute.Int("skipped_existing", len(job.objectIDs)-len(objectIDsToDownload)),
				)
				if err := w.downloader(ctx, job.profile, objectIDsToDownload); err != nil {
					downloadSpan.RecordError(err)
					downloadSpan.SetStatus(codes.Error, "download failed")
					downloadSpan.End()
					ingestSpan.RecordError(err)
					ingestSpan.SetStatus(codes.Error, "download failed")
					ingestSpan.End()
					slog.Error("Failed to download S3 objects", "error", err.Error())
					w.mu.Lock()
					for _, id := range job.ids {
						if wg := w.inflight[id]; wg != nil {
							delete(w.inflight, id)
						}
					}
					w.mu.Unlock()
					continue
				}
				downloadSpan.End()
			} else if len(objectIDsToDownload) == 0 {
				slog.Info("Skipping download, all files already exist locally",
					slog.Int("file_count", len(job.objectIDs)))
			}

			_, parquetSpan := tracer.Start(ctx, "query.worker.cache_ingest_parquet")
			parquetSpan.SetAttributes(attribute.Int("file_count", len(localPaths)))
			if err := w.sink.IngestParquetBatch(ctx, localPaths, job.ids); err != nil {
				parquetSpan.RecordError(err)
				parquetSpan.SetStatus(codes.Error, "ingest failed")
				parquetSpan.End()
				ingestSpan.RecordError(err)
				ingestSpan.SetStatus(codes.Error, "ingest failed")
				ingestSpan.End()
				slog.Error("Failed to ingest Parquet batch", "error", err.Error())
				// release inflight on failure
				w.mu.Lock()
				for _, id := range job.ids {
					if wg := w.inflight[id]; wg != nil {
						delete(w.inflight, id)
					}
				}
				w.mu.Unlock()
				continue
			}
			parquetSpan.End()

			// Mark present, update lastAccess, release inflight, and mark files for delayed deletion
			now := time.Now()
			w.mu.Lock()
			for _, id := range job.ids {
				w.present[id] = struct{}{}
				w.lastAccess[id] = now
				if wg := w.inflight[id]; wg != nil {
					delete(w.inflight, id)
				}
			}
			w.mu.Unlock()

			// Mark files for delayed deletion instead of immediate removal.
			// This allows concurrent queries to finish using the files.
			for _, objID := range job.objectIDs {
				if w.parquetCache != nil {
					w.parquetCache.MarkForDeletion(region, bucket, objID)
				}
			}

			ingestSpan.SetAttributes(attribute.Int("segments_cached", len(job.ids)))
			ingestSpan.End()

			w.maybeEvictOnce(ctx)
		}
	}
}

const batchSize = 300

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
}

// evictedEnough returns true if we've evicted enough to be below the low watermark.
// This provides a buffer to avoid thrashing at the boundary.
func (w *CacheManager) evictedEnough() bool {
	// Check row count limit
	targetRows := int64(float64(w.maxRows) * 0.9)
	if w.sink.RowCount() > targetRows {
		return false
	}

	// Check disk usage against low watermark (shared across all signal types)
	dbPath := w.s3Pool.GetDatabasePath()
	usedBytes, totalBytes, err := getDiskUsage(dbPath)
	if err != nil {
		// If we can't check disk, assume we haven't evicted enough
		return false
	}

	diskUtilization := float64(usedBytes) / float64(totalBytes)
	return diskUtilization <= DiskUsageLowWatermark
}

func (w *CacheManager) maybeEvictOnce(ctx context.Context) {
	if w.maxRows <= 0 {
		return
	}

	over := w.sink.RowCount() - w.maxRows

	// Get disk usage for the filesystem where the database resides
	dbPath := w.s3Pool.GetDatabasePath()
	usedBytes, totalBytes, err := getDiskUsage(dbPath)
	if err != nil {
		slog.Error("Failed to get disk usage", slog.Any("error", err), slog.String("path", dbPath))
		return
	}

	diskUtilization := float64(usedBytes) / float64(totalBytes)
	usedGB := float64(usedBytes) / (1 << 30)
	totalGB := float64(totalBytes) / (1 << 30)

	slog.Info("Cache Status",
		slog.String("dataset", w.dataset),
		slog.Int64("rowCount", w.sink.RowCount()),
		slog.Int64("maxRows", w.maxRows),
		slog.Int64("overRows", over),
		slog.Float64("usedDiskGB", usedGB),
		slog.Float64("totalDiskGB", totalGB),
		slog.Float64("diskUtilization", diskUtilization),
		slog.Float64("highWatermark", DiskUsageHighWatermark))

	// Evict if row count exceeded OR disk usage above high watermark
	shouldEvict := over > 0 || diskUtilization >= DiskUsageHighWatermark

	if shouldEvict {
		evictStart := time.Now()
		defer func() {
			if w.evictionDurationHist != nil {
				w.evictionDurationHist.Record(ctx, time.Since(evictStart).Seconds(),
					metric.WithAttributes(attribute.String("signal", w.dataset)))
			}
		}()

		type ent struct {
			id int64
			at time.Time
		}
		w.mu.Lock()
		lru := make([]ent, 0, len(w.present))
		for id := range w.present {
			lru = append(lru, ent{id: id, at: w.lastAccess[id]})
		}
		w.mu.Unlock()
		if len(lru) == 0 {
			return
		}

		sort.Slice(lru, func(i, j int) bool { return lru[i].at.Before(lru[j].at) })

		batch := make([]int64, 0, batchSize)

		for _, e := range lru {
			if w.evictedEnough() {
				break
			}
			batch = append(batch, e.id)
			if len(batch) == batchSize {
				w.dropSegments(ctx, batch)
				batch = batch[:0]
			}
		}
		if len(batch) > 0 && !w.evictedEnough() {
			w.dropSegments(ctx, batch)
		}
	}
}

func (w *CacheManager) dropSegments(ctx context.Context, segIDs []int64) {
	slog.Info("Evicting segments from cache", slog.Int("count", len(segIDs)))
	_, _ = w.sink.DeleteSegments(ctx, segIDs)

	w.mu.Lock()
	for _, id := range segIDs {
		delete(w.present, id)
		delete(w.lastAccess, id)
	}
	w.mu.Unlock()
}

// getDiskUsage returns current disk usage statistics for the filesystem containing
// the given path. Returns used bytes and total bytes.
func getDiskUsage(path string) (usedBytes, totalBytes uint64, err error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}

	totalBytes = stat.Blocks * uint64(stat.Bsize)
	freeBytes := stat.Bfree * uint64(stat.Bsize)
	usedBytes = totalBytes - freeBytes

	return usedBytes, totalBytes, nil
}
