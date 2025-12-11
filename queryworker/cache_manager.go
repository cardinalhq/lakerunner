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
	"os"
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
	profile storageprofile.StorageProfile
	paths   []string
	ids     []int64
}

// CacheManager coordinates downloads, batch-ingest, queries, and LRU evictions.
type CacheManager struct {
	sink                   *DDBSink
	s3Pool                 *duckdbx.S3DB // shared global pool
	maxRows                int64
	maxDiskUsageBytes      uint64 // dynamically calculated from disk size
	downloader             DownloadBatchFunc
	storageProfileProvider storageprofile.StorageProfileProvider
	dataset                string

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
}

const (
	MaxRowsDefault = 1000000000 // 1 billion rows (approx 10GB on disk)

	// DiskUsageHeadroomFraction is the fraction of disk space to reserve as headroom.
	// We use 80% of total disk space as the max, leaving 20% for overhead, temp files,
	// and measurement noise.
	DiskUsageHeadroomFraction = 0.80

	// MinDiskUsageBytes is the minimum disk usage limit (1GB).
	// This prevents unreasonably small limits on tiny test volumes.
	MinDiskUsageBytes = 1 << 30 // 1 GB

	// DefaultDiskUsageBytes is the fallback if disk size detection fails (8GB).
	DefaultDiskUsageBytes = 8 << 30 // 8 GB
)

func NewCacheManager(dl DownloadBatchFunc, dataset string, storageProfileProvider storageprofile.StorageProfileProvider, s3Pool *duckdbx.S3DB) *CacheManager {
	ddb, err := NewDDBSink(dataset, context.Background(), s3Pool)
	if err != nil {
		slog.Error("Failed to create DuckDB sink", slog.Any("error", err))
		return nil
	}

	// Calculate max disk usage based on the filesystem where the database resides
	maxDiskUsage := calculateMaxDiskUsage(s3Pool.GetDatabasePath())

	w := &CacheManager{
		sink:                     ddb,
		s3Pool:                   s3Pool,
		dataset:                  dataset,
		storageProfileProvider:   storageProfileProvider,
		profilesByOrgInstanceNum: make(map[uuid.UUID]map[int16]storageprofile.StorageProfile),
		maxRows:                  MaxRowsDefault,
		maxDiskUsageBytes:        maxDiskUsage,
		downloader:               dl,
		present:                  make(map[int64]struct{}, ChannelBufferSize),
		lastAccess:               make(map[int64]time.Time, ChannelBufferSize),
		inflight:                 make(map[int64]*struct{}, ChannelBufferSize),
		ingestQ:                  make(chan ingestJob, 64),
	}

	slog.Info("CacheManager initialized",
		slog.String("dataset", dataset),
		slog.Int64("maxRows", w.maxRows),
		slog.Uint64("maxDiskUsageBytes", w.maxDiskUsageBytes),
		slog.Float64("maxDiskUsageGB", float64(w.maxDiskUsageBytes)/(1<<30)))

	ingCtx, ingCancel := context.WithCancel(context.Background())
	w.stopIngest = ingCancel
	w.ingestWG.Add(1)
	go w.ingestLoop(ingCtx)
	return w
}

func (w *CacheManager) Close() {
	if w.stopIngest != nil {
		w.stopIngest()
	}
	w.ingestWG.Wait()
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

			// Split into cached vs S3
			var s3URIs []string
			var s3LocalPaths []string
			var s3IDs []int64
			var cachedIDs []int64

			for _, seg := range segments {
				objectId := fmt.Sprintf("db/%s/%s/%d/%s/%s/tbl_%d.parquet",
					orgId.String(),
					profile.CollectorName,
					seg.DateInt,
					w.dataset,
					seg.Hour,
					seg.SegmentID)

				w.mu.RLock()
				_, inCache := w.present[seg.SegmentID]
				w.mu.RUnlock()

				if inCache {
					cachedIDs = append(cachedIDs, seg.SegmentID)
				} else {
					bucket := profile.Bucket
					var prefix string
					if profile.CloudProvider == "azure" {
						prefix = "azure://" + bucket + "/"
					} else {
						prefix = "s3://" + bucket + "/"
					}
					s3URIs = append(s3URIs, prefix+objectId)
					s3LocalPaths = append(s3LocalPaths, objectId)
					s3IDs = append(s3IDs, seg.SegmentID)
				}
			}

			totalS3Segments += len(s3URIs)
			totalCachedSegments += len(cachedIDs)

			// Safe logging (len(w.present) under RLock)
			w.mu.RLock()
			numPresent := len(w.present)
			w.mu.RUnlock()
			slog.Info("Segment Stats",
				"numS3", len(s3URIs),
				"numCached", len(cachedIDs),
				"numPresent", numPresent)

			// Stream uncached segments directly from S3 (one channel per glob).
			s3Channels, err := streamFromS3(ctx, w, request,
				profile,
				s3URIs,
				s3GlobSize,
				userSQL,
				mapper)
			if err != nil {
				evalSpan.RecordError(err)
				evalSpan.SetStatus(codes.Error, "failed to stream from S3")
				return nil, fmt.Errorf("stream from S3: %w", err)
			}
			outs = append(outs, s3Channels...)

			// Enqueue uncached segments for background ingest.
			if len(s3LocalPaths) > 0 {
				w.enqueueIngest(profile, s3LocalPaths, s3IDs)
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

func streamFromS3[T promql.Timestamped](
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	profile storageprofile.StorageProfile,
	s3URIs []string,
	_ int, // s3GlobSize now unused: single-batch S3
	userSQL string,
	mapper RowMapper[T],
) ([]<-chan T, error) {
	if len(s3URIs) == 0 {
		return []<-chan T{}, nil
	}

	// Single S3 batch per worker: one channel, one goroutine
	out := make(chan T, ChannelBufferSize)
	outs := []<-chan T{out}

	urisCopy := append([]string(nil), s3URIs...) // capture
	batchNum := 0                                // always 0; single batch

	go func(out chan<- T) {
		defer close(out)

		tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
		_, s3Span := tracer.Start(ctx, "query.worker.stream_from_s3")
		defer s3Span.End()

		s3Span.SetAttributes(
			attribute.Int("batch_index", batchNum),
			attribute.Int("file_count", len(urisCopy)),
			attribute.String("bucket", profile.Bucket),
			attribute.String("cloud_provider", profile.CloudProvider),
		)

		// Build read_parquet source
		quoted := make([]string, len(urisCopy))
		for i := range urisCopy {
			quoted[i] = "'" + escapeSQL(urisCopy[i]) + "'"
		}
		array := "[" + strings.Join(quoted, ", ") + "]"
		src := fmt.Sprintf(`read_parquet(%s, union_by_name=true)`, array)

		sqlReplaced := strings.Replace(userSQL, "{table}", src, 1)

		// Lease a per-bucket connection (creates/refreshes S3 secret under the hood)
		_, connSpan := tracer.Start(ctx, "query.worker.s3_connection_acquire")
		start := time.Now()
		conn, release, err := w.s3Pool.GetConnectionForBucket(ctx, profile)
		connectionAcquireTime := time.Since(start)
		connSpan.SetAttributes(attribute.Int64("duration_ms", connectionAcquireTime.Milliseconds()))
		connSpan.End()
		slog.Info("S3 Connection Acquire Time", "duration", connectionAcquireTime.String(), "bucket", profile.Bucket)

		if err != nil {
			s3Span.RecordError(err)
			s3Span.SetStatus(codes.Error, "connection acquire failed")
			slog.Error("GetConnection failed", slog.String("bucket", profile.Bucket), slog.Any("error", err))
			return
		}
		defer release()

		_, querySpan := tracer.Start(ctx, "query.worker.s3_query_execute")
		rows, err := executeQueryWithRetry(ctx, conn, sqlReplaced, "S3", len(urisCopy))
		if err != nil {
			querySpan.RecordError(err)
			querySpan.SetStatus(codes.Error, "query failed")
			querySpan.End()
			s3Span.RecordError(err)
			s3Span.SetStatus(codes.Error, "query failed")
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
			s3Span.RecordError(err)
			s3Span.SetStatus(codes.Error, "failed to get columns")
			slog.Error("failed to get columns", slog.Any("error", err))
			return
		}

		rowCount := 0
		for rows.Next() {
			select {
			case <-ctx.Done():
				s3Span.SetAttributes(attribute.Int("rows_returned", rowCount))
				s3Span.SetStatus(codes.Error, "context cancelled")
				slog.Warn("Context cancelled during S3 row iteration", slog.Int("numFiles", len(urisCopy)), slog.Int("rowsProcessed", rowCount))
				return
			default:
			}
			v, mErr := mapper(request, cols, rows)
			if mErr != nil {
				s3Span.RecordError(mErr)
				s3Span.SetStatus(codes.Error, "row mapping failed")
				slog.Error("Row mapping failed", slog.Any("error", mErr))
				return
			}
			out <- v
			rowCount++
		}
		if err := rows.Err(); err != nil {
			s3Span.RecordError(err)
			s3Span.SetStatus(codes.Error, "rows iteration error")
			slog.Error("Rows iteration error", slog.Any("error", err))
			return
		}
		s3Span.SetAttributes(attribute.Int("rows_returned", rowCount))
		slog.Info("S3 query completed", slog.Int("numFiles", len(urisCopy)), slog.Int("rowsReturned", rowCount))
	}(out)

	return outs, nil
}

//func streamFromS3[T promql.Timestamped](
//	ctx context.Context,
//	w *CacheManager,
//	request queryapi.PushDownRequest,
//	profile storageprofile.StorageProfile,
//	s3URIs []string,
//	s3GlobSize int,
//	userSQL string,
//	mapper RowMapper[T],
//) ([]<-chan T, error) {
//	if len(s3URIs) == 0 {
//		return []<-chan T{}, nil
//	}
//
//	batches := chunkStrings(s3URIs, s3GlobSize)
//	outs := make([]<-chan T, 0, len(batches))
//
//	for batchIdx, uris := range batches {
//		slog.Info("Streaming from S3", slog.Int("uris", len(uris)))
//		out := make(chan T, ChannelBufferSize)
//		outs = append(outs, out)
//
//		urisCopy := append([]string(nil), uris...) // capture loop var
//		batchNum := batchIdx                       // capture for goroutine
//
//		go func(out chan<- T) {
//			defer close(out)
//
//			tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
//			_, s3Span := tracer.Start(ctx, "query.worker.stream_from_s3")
//			defer s3Span.End()
//
//			s3Span.SetAttributes(
//				attribute.Int("batch_index", batchNum),
//				attribute.Int("file_count", len(urisCopy)),
//				attribute.String("bucket", profile.Bucket),
//				attribute.String("cloud_provider", profile.CloudProvider),
//			)
//
//			// Build read_parquet source
//			quoted := make([]string, len(urisCopy))
//			for i := range urisCopy {
//				quoted[i] = "'" + escapeSQL(urisCopy[i]) + "'"
//			}
//			array := "[" + strings.Join(quoted, ", ") + "]"
//			src := fmt.Sprintf(`read_parquet(%s, union_by_name=true)`, array)
//
//			sqlReplaced := strings.Replace(userSQL, "{table}", src, 1)
//
//			// Lease a per-bucket connection (creates/refreshes S3 secret under the hood)
//			_, connSpan := tracer.Start(ctx, "query.worker.s3_connection_acquire")
//			start := time.Now()
//			conn, release, err := w.s3Pool.GetConnectionForBucket(ctx, profile)
//			connectionAcquireTime := time.Since(start)
//			connSpan.SetAttributes(attribute.Int64("duration_ms", connectionAcquireTime.Milliseconds()))
//			connSpan.End()
//			slog.Info("S3 Connection Acquire Time", "duration", connectionAcquireTime.String(), "bucket", profile.Bucket)
//
//			if err != nil {
//				s3Span.RecordError(err)
//				s3Span.SetStatus(codes.Error, "connection acquire failed")
//				slog.Error("GetConnection failed", slog.String("bucket", profile.Bucket), slog.Any("error", err))
//				return
//			}
//			// Ensure rows close before releasing the connection
//			defer release()
//
//			_, querySpan := tracer.Start(ctx, "query.worker.s3_query_execute")
//			rows, err := executeQueryWithRetry(ctx, conn, sqlReplaced, "S3", len(urisCopy))
//			if err != nil {
//				querySpan.RecordError(err)
//				querySpan.SetStatus(codes.Error, "query failed")
//				querySpan.End()
//				s3Span.RecordError(err)
//				s3Span.SetStatus(codes.Error, "query failed")
//				return
//			}
//			querySpan.End()
//			defer func() {
//				if err := rows.Close(); err != nil {
//					slog.Error("Error closing rows", slog.Any("error", err))
//				}
//			}()
//
//			cols, err := rows.Columns()
//			if err != nil {
//				s3Span.RecordError(err)
//				s3Span.SetStatus(codes.Error, "failed to get columns")
//				slog.Error("failed to get columns", slog.Any("error", err))
//				return
//			}
//
//			rowCount := 0
//			for rows.Next() {
//				select {
//				case <-ctx.Done():
//					s3Span.SetAttributes(attribute.Int("rows_returned", rowCount))
//					s3Span.SetStatus(codes.Error, "context cancelled")
//					slog.Warn("Context cancelled during S3 row iteration", slog.Int("numFiles", len(urisCopy)), slog.Int("rowsProcessed", rowCount))
//					return
//				default:
//				}
//				v, mErr := mapper(request, cols, rows)
//				if mErr != nil {
//					s3Span.RecordError(mErr)
//					s3Span.SetStatus(codes.Error, "row mapping failed")
//					slog.Error("Row mapping failed", slog.Any("error", mErr))
//					return
//				}
//				out <- v
//				rowCount++
//			}
//			if err := rows.Err(); err != nil {
//				s3Span.RecordError(err)
//				s3Span.SetStatus(codes.Error, "rows iteration error")
//				slog.Error("Rows iteration error", slog.Any("error", err))
//				return
//			}
//			s3Span.SetAttributes(attribute.Int("rows_returned", rowCount))
//			slog.Info("S3 query completed", slog.Int("numFiles", len(urisCopy)), slog.Int("rowsReturned", rowCount))
//		}(out)
//	}
//
//	// enqueue is done in EvaluatePushDown(...) after ids/paths are known
//	return outs, nil
//}

// enqueueIngest filters out present/in-flight, marks new IDs in-flight, and queues one job.
func (w *CacheManager) enqueueIngest(storageProfile storageprofile.StorageProfile, paths []string, ids []int64) {
	if len(paths) == 0 || len(paths) != len(ids) {
		return
	}

	// Filter + mark in-flight under lock
	var todoPaths []string
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
		todoPaths = append(todoPaths, paths[i])
		todoIDs = append(todoIDs, id)
		w.lastAccess[id] = time.Now()
	}
	w.mu.Unlock()

	if len(todoPaths) == 0 {
		return
	}

	// Non-blocking enqueue
	w.ingestQ <- ingestJob{profile: storageProfile, paths: todoPaths, ids: todoIDs}
}

func (w *CacheManager) ingestLoop(ctx context.Context) {
	defer w.ingestWG.Done()

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.ingestQ:
			if len(job.paths) == 0 {
				continue
			}

			// Create a span for the entire cache ingest operation
			_, ingestSpan := tracer.Start(ctx, "query.worker.cache_ingest_batch")
			ingestSpan.SetAttributes(
				attribute.Int("file_count", len(job.paths)),
				attribute.String("bucket", job.profile.Bucket),
				attribute.String("dataset", w.dataset),
			)

			if w.downloader != nil {
				_, downloadSpan := tracer.Start(ctx, "query.worker.cache_download_files")
				downloadSpan.SetAttributes(attribute.Int("file_count", len(job.paths)))
				if err := w.downloader(ctx, job.profile, job.paths); err != nil {
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
			}

			_, parquetSpan := tracer.Start(ctx, "query.worker.cache_ingest_parquet")
			parquetSpan.SetAttributes(attribute.Int("file_count", len(job.paths)))
			if err := w.sink.IngestParquetBatch(ctx, job.paths, job.ids); err != nil {
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

			// Mark present, update lastAccess, release inflight, and delete local files
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

			for _, p := range job.paths {
				_ = os.Remove(p) // best-effort cleanup
			}

			ingestSpan.SetAttributes(attribute.Int("segments_cached", len(job.ids)))
			ingestSpan.End()

			w.maybeEvictOnce(ctx)
		}
	}
}

func chunkStrings(xs []string, size int) [][]string {
	if size <= 0 || len(xs) == 0 {
		return nil
	}
	var out [][]string
	for i := 0; i < len(xs); i += size {
		j := i + size
		if j > len(xs) {
			j = len(xs)
		}
		out = append(out, xs[i:j])
	}
	return out
}

const batchSize = 300

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
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

	usedGB := float64(usedBytes) / (1 << 30)
	maxUsageGB := float64(w.maxDiskUsageBytes) / (1 << 30)
	totalGB := float64(totalBytes) / (1 << 30)

	slog.Info("Cache Status",
		slog.Int64("rowCount", w.sink.RowCount()),
		slog.Int64("maxRows", w.maxRows),
		slog.Int64("overRows", over),
		slog.Float64("usedDiskGB", usedGB),
		slog.Float64("maxDiskUsageGB", maxUsageGB),
		slog.Float64("totalDiskGB", totalGB))

	shouldEvict := over > 0 || usedBytes >= w.maxDiskUsageBytes

	if shouldEvict {
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
			//if w.sink.RowCount() <= w.maxRows {
			//	break
			//}
			batch = append(batch, e.id)
			if len(batch) == batchSize {
				w.dropSegments(ctx, batch)
				batch = batch[:0]
			}
		}
		if len(batch) > 0 && w.sink.RowCount() > w.maxRows {
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

// calculateMaxDiskUsage determines the maximum disk usage for the cache based on
// the total size of the filesystem where the database file resides.
// It returns 80% of total disk space, with a minimum of 1GB and a fallback of 8GB
// if disk detection fails.
func calculateMaxDiskUsage(dbPath string) uint64 {
	if dbPath == "" {
		slog.Warn("No database path provided for disk size calculation, using default",
			slog.Uint64("defaultBytes", DefaultDiskUsageBytes))
		return DefaultDiskUsageBytes
	}

	var stat syscall.Statfs_t
	if err := syscall.Statfs(dbPath, &stat); err != nil {
		slog.Warn("Failed to stat filesystem for disk size calculation, using default",
			slog.String("path", dbPath),
			slog.Any("error", err),
			slog.Uint64("defaultBytes", DefaultDiskUsageBytes))
		return DefaultDiskUsageBytes
	}

	totalBytes := stat.Blocks * uint64(stat.Bsize)
	maxUsage := uint64(float64(totalBytes) * DiskUsageHeadroomFraction)

	// Enforce minimum to prevent unreasonably small limits
	if maxUsage < MinDiskUsageBytes {
		maxUsage = MinDiskUsageBytes
	}

	slog.Info("Calculated max disk usage for cache",
		slog.String("path", dbPath),
		slog.Uint64("totalBytes", totalBytes),
		slog.Float64("totalGB", float64(totalBytes)/(1<<30)),
		slog.Uint64("maxUsageBytes", maxUsage),
		slog.Float64("maxUsageGB", float64(maxUsage)/(1<<30)),
		slog.Float64("headroomFraction", DiskUsageHeadroomFraction))

	return maxUsage
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
