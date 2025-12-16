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
	"strings"
	"sync"
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

// tblToAggObjectID converts a tbl_ object ID to its corresponding agg_ object ID.
// e.g., db/org/col/123/logs/00/tbl_456.parquet -> db/org/col/123/logs/00/agg_456.parquet
func tblToAggObjectID(tblObjectID string) string {
	return strings.Replace(tblObjectID, "/tbl_", "/agg_", 1)
}

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

	// Build channels per (org, instance)
	for orgId, instances := range segmentsByOrg {
		for instanceNum, segments := range instances {
			profile, err := w.getProfile(ctx, orgId, instanceNum)
			if err != nil {
				evalSpan.RecordError(err)
				evalSpan.SetStatus(codes.Error, "failed to get profile")
				return nil, err
			}

			// Build object IDs for all segments (tbl_ files)
			var objectIDs []string
			var aggObjectIDs []string
			for _, seg := range segments {
				objectID := fmt.Sprintf("db/%s/%s/%d/%s/%s/tbl_%d.parquet",
					orgId.String(),
					profile.CollectorName,
					seg.DateInt,
					w.dataset,
					seg.Hour,
					seg.SegmentID)
				objectIDs = append(objectIDs, objectID)

				// Also download agg_ file if segment has agg_fields
				if len(seg.AggFields) > 0 {
					aggObjectIDs = append(aggObjectIDs, tblToAggObjectID(objectID))
				}
			}

			slog.Info("Segment Stats",
				"numSegments", len(objectIDs),
				"numAggSegments", len(aggObjectIDs))

			// Download tbl_ files from S3 before querying
			if len(objectIDs) > 0 {
				if err := w.downloadForQuery(ctx, profile, objectIDs); err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to download from S3")
					return nil, fmt.Errorf("download from S3: %w", err)
				}
			}

			// Download agg_ files (best-effort - don't fail if not found)
			if len(aggObjectIDs) > 0 {
				if err := w.downloadForQuery(ctx, profile, aggObjectIDs); err != nil {
					// Log but don't fail - we'll fall back to tbl_ queries
					slog.Warn("Failed to download some agg_ files, will use tbl_ fallback",
						"error", err,
						"numAggFiles", len(aggObjectIDs))
				}
			}

			// Convert object IDs to local paths for streaming
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

			// Stream from downloaded local files
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
		}
	}
	channelCreationTime := time.Since(start)
	slog.Info("Channel Creation Time", "duration", channelCreationTime.String())

	evalSpan.SetAttributes(
		attribute.Int("total_segments", len(request.Segments)),
		attribute.Int("output_channels", len(outs)),
	)

	// Merge all sources in timestamp order.
	return promql.MergeSorted(ctx, ChannelBufferSize, request.Reverse, request.Limit, outs...), nil
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
		conn, release, err := w.pool.GetConnection(ctx)
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

// EvaluatePushDownWithAggSplit evaluates a pushdown query with intelligent
// routing between agg_ and tbl_ files based on segment eligibility.
// For segments where agg_ files exist and support the query's GROUP BY and matchers,
// use aggSQL on agg_ files. Otherwise use tblSQL on tbl_ files.
func EvaluatePushDownWithAggSplit[T promql.Timestamped](
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	tblSQL string,
	aggSQL string,
	groupBy []string,
	matcherFields []string,
	s3GlobSize int,
	mapper RowMapper[T],
) (<-chan T, error) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
	ctx, evalSpan := tracer.Start(ctx, "query.worker.evaluate_pushdown_with_agg_split")
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
	if !strings.Contains(tblSQL, "{table}") {
		evalSpan.SetStatus(codes.Error, "invalid tblSQL template")
		return nil, errors.New(`tblSQL must contain "{table}" placeholder`)
	}
	if !strings.Contains(aggSQL, "{table}") {
		evalSpan.SetStatus(codes.Error, "invalid aggSQL template")
		return nil, errors.New(`aggSQL must contain "{table}" placeholder`)
	}

	// Group segments by orgId/instanceNum
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

	// Build channels per (org, instance)
	for orgId, instances := range segmentsByOrg {
		for instanceNum, segments := range instances {
			profile, err := w.getProfile(ctx, orgId, instanceNum)
			if err != nil {
				evalSpan.RecordError(err)
				evalSpan.SetStatus(codes.Error, "failed to get profile")
				return nil, err
			}

			// Build object IDs for all segments (tbl_ files)
			var objectIDs []string
			var aggObjectIDs []string
			segmentObjectIDMap := make(map[int64]string) // segmentID -> objectID

			for _, seg := range segments {
				objectID := fmt.Sprintf("db/%s/%s/%d/%s/%s/tbl_%d.parquet",
					orgId.String(),
					profile.CollectorName,
					seg.DateInt,
					w.dataset,
					seg.Hour,
					seg.SegmentID)
				objectIDs = append(objectIDs, objectID)
				segmentObjectIDMap[seg.SegmentID] = objectID

				// Also download agg_ file if segment has agg_fields
				if len(seg.AggFields) > 0 {
					aggObjectIDs = append(aggObjectIDs, tblToAggObjectID(objectID))
				}
			}

			slog.Info("Segment Stats",
				"numSegments", len(objectIDs),
				"numAggSegments", len(aggObjectIDs))

			// Download tbl_ files from S3 before querying
			if len(objectIDs) > 0 {
				if err := w.downloadForQuery(ctx, profile, objectIDs); err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to download from S3")
					return nil, fmt.Errorf("download from S3: %w", err)
				}
			}

			// Download agg_ files (best-effort - don't fail if not found)
			if len(aggObjectIDs) > 0 {
				if err := w.downloadForQuery(ctx, profile, aggObjectIDs); err != nil {
					// Log but don't fail - we'll fall back to tbl_ queries
					slog.Warn("Failed to download some agg_ files, will use tbl_ fallback",
						"error", err,
						"numAggFiles", len(aggObjectIDs))
				}
			}

			// Split segments into agg-eligible and tbl-only based on:
			// 1. agg_ file exists locally
			// 2. segment's agg_fields support the query's GROUP BY
			var aggLocalPaths []string
			var tblLocalPaths []string

			for _, seg := range segments {
				tblObjectID := segmentObjectIDMap[seg.SegmentID]
				tblLocalPath, tblExists, err := w.parquetCache.GetOrPrepare(profile.Region, profile.Bucket, tblObjectID)
				if err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to get local path")
					return nil, fmt.Errorf("get local path for %s: %w", tblObjectID, err)
				}
				if !tblExists {
					slog.Warn("Expected tbl_ file to exist after download", "objectID", tblObjectID, "localPath", tblLocalPath)
				}

				// Check if we can use agg_ file for this segment
				aggObjectID := tblToAggObjectID(tblObjectID)
				aggLocalPath, aggExists, _ := w.parquetCache.GetOrPrepare(profile.Region, profile.Bucket, aggObjectID)

				canUseAgg := promql.CanUseAggFile(seg.AggFields, groupBy, matcherFields)
				if aggExists && canUseAgg {
					aggLocalPaths = append(aggLocalPaths, aggLocalPath)
					promql.RecordLogAggregationSource(ctx, profile.OrganizationID.String(), "agg", profile.InstanceNum)
				} else {
					tblLocalPaths = append(tblLocalPaths, tblLocalPath)
					promql.RecordLogAggregationSource(ctx, profile.OrganizationID.String(), "tbl", profile.InstanceNum)
					slog.Info("Using tbl_ file instead of agg_",
						slog.Int64("segmentID", seg.SegmentID),
						slog.Bool("aggFileExists", aggExists),
						slog.Bool("canUseAggFile", canUseAgg),
						slog.Any("segAggFields", seg.AggFields),
						slog.Any("queryGroupBy", groupBy),
						slog.Any("queryMatcherFields", matcherFields))
				}
			}

			slog.Info("Segment Split",
				"aggEligible", len(aggLocalPaths),
				"tblOnly", len(tblLocalPaths))

			// Stream from agg_ files with agg SQL
			if len(aggLocalPaths) > 0 {
				aggChannels, err := streamFromLocalFiles(ctx, w, request, aggLocalPaths, aggSQL, mapper)
				if err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to stream from agg files")
					return nil, fmt.Errorf("stream from agg files: %w", err)
				}
				outs = append(outs, aggChannels...)
			}

			// Stream from tbl_ files with tbl SQL
			if len(tblLocalPaths) > 0 {
				tblChannels, err := streamFromLocalFiles(ctx, w, request, tblLocalPaths, tblSQL, mapper)
				if err != nil {
					evalSpan.RecordError(err)
					evalSpan.SetStatus(codes.Error, "failed to stream from tbl files")
					return nil, fmt.Errorf("stream from tbl files: %w", err)
				}
				outs = append(outs, tblChannels...)
			}
		}
	}
	channelCreationTime := time.Since(start)
	slog.Info("Channel Creation Time", "duration", channelCreationTime.String())

	evalSpan.SetAttributes(
		attribute.Int("total_segments", len(request.Segments)),
		attribute.Int("output_channels", len(outs)),
	)

	// Merge all sources in timestamp order.
	return promql.MergeSorted(ctx, ChannelBufferSize, request.Reverse, request.Limit, outs...), nil
}
