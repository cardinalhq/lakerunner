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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"golang.org/x/sync/errgroup"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
)

// WorkerService wires HTTP → CacheManager → SSE.
type WorkerService struct {
	MetricsCM            *CacheManager
	LogsCM               *CacheManager
	TracesCM             *CacheManager
	StorageProfilePoller storageprofile.StorageProfileProvider
	MetricsGlobSize      int
	LogsGlobSize         int
	TracesGlobSize       int
	pool                 *duckdbx.DB       // shared pool for all queries
	parquetCache         *ParquetFileCache // shared parquet file cache
}

type scanBuffers struct {
	vals []any
	ptrs []any
}

var scanBufferPool = sync.Pool{
	New: func() any {
		return &scanBuffers{
			vals: make([]any, 0, 32),
			ptrs: make([]any, 0, 32),
		}
	},
}

func acquireScanBuffers(n int) (*scanBuffers, func()) {
	buf := scanBufferPool.Get().(*scanBuffers)

	if cap(buf.vals) < n {
		buf.vals = make([]any, n)
	} else {
		buf.vals = buf.vals[:n]
	}

	if cap(buf.ptrs) < n {
		buf.ptrs = make([]any, n)
	} else {
		buf.ptrs = buf.ptrs[:n]
	}

	for i := range n {
		buf.vals[i] = nil
		buf.ptrs[i] = &buf.vals[i]
	}

	return buf, func() {
		for i := range buf.vals {
			buf.vals[i] = nil
		}
		buf.vals = buf.vals[:0]
		buf.ptrs = buf.ptrs[:0]
		scanBufferPool.Put(buf)
	}
}

func NewWorkerService(
	metricsGlobSize int,
	logsGlobSize int,
	tracesGlobSize int,
	maxParallelDownloads int,
	sp storageprofile.StorageProfileProvider,
	cloudManagers cloudstorage.ClientProvider,
	duckdbSettings duckdbx.DuckDBSettings,
) (*WorkerService, error) {
	// Create shared parquet file cache for downloaded files
	parquetCache, err := NewParquetFileCache(DefaultCleanupInterval)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet file cache: %w", err)
	}

	// Scan for existing files from previous runs
	if err := parquetCache.ScanExistingFiles(); err != nil {
		slog.Warn("Failed to scan existing parquet files", slog.Any("error", err))
	}

	// Register parquet cache metrics
	if err := parquetCache.RegisterMetrics(); err != nil {
		slog.Error("Failed to register parquet cache metrics", slog.Any("error", err))
	}

	downloader := func(ctx context.Context, profile storageprofile.StorageProfile, objectIDs []string) error {
		if len(objectIDs) == 0 {
			return nil
		}

		storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
		if err != nil {
			return fmt.Errorf("failed to create storage client for provider %s: %w", profile.CloudProvider, err)
		}

		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(maxParallelDownloads)

		region := profile.Region
		bucket := profile.Bucket

		for _, objectID := range objectIDs {
			g.Go(func() error {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				// Ask the cache where to put this file
				localPath, exists, err := parquetCache.GetOrPrepare(region, bucket, objectID)
				if err != nil {
					return fmt.Errorf("failed to prepare cache path for %s: %w", objectID, err)
				}

				if exists {
					slog.Debug("File already cached locally, skipping download",
						slog.String("objectID", objectID),
						slog.String("path", localPath))
					return nil
				}

				dir := filepath.Dir(localPath)

				// Download to temp file in target directory
				tmpfn, _, is404, err := storageClient.DownloadObject(gctx, dir, bucket, objectID)
				if err != nil {
					slog.Error("Failed to download object",
						slog.String("cloudProvider", profile.CloudProvider),
						slog.String("bucket", bucket),
						slog.String("objectID", objectID),
						slog.Any("error", err))
					return err
				}
				if is404 {
					// Non-fatal skip
					slog.Info("Object not found, skipping",
						slog.String("cloudProvider", profile.CloudProvider),
						slog.String("bucket", bucket),
						slog.String("objectID", objectID))
					return nil
				}

				// Atomically move tmp file into place as the final localPath.
				// Since tmp is in the same dir, os.Rename is atomic on POSIX.
				if err := os.Rename(tmpfn, localPath); err != nil {
					// Windows: need to remove existing file first
					_ = os.Remove(localPath)
					if err2 := os.Rename(tmpfn, localPath); err2 != nil {
						_ = os.Remove(tmpfn)
						return fmt.Errorf("rename %q -> %q: %w", tmpfn, localPath, err2)
					}
				}

				// Track the newly downloaded file
				if err := parquetCache.TrackFile(region, bucket, objectID); err != nil {
					slog.Error("Failed to track downloaded file - file exists but won't be managed by cache",
						slog.String("objectID", objectID),
						slog.String("path", localPath),
						slog.Any("error", err))
				}

				return nil
			})
		}

		// Wait for all downloads; return first error if any
		if err := g.Wait(); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			return fmt.Errorf("one or more downloads failed: %w", err)
		}
		return nil
	}

	// Create a single shared DB pool for all queries with metrics enabled
	pool, err := duckdbx.NewDB(
		duckdbx.WithMetrics(10*time.Second),
		duckdbx.WithConnectionMaxAge(30*time.Minute),
		duckdbx.WithDuckDBSettings(duckdbSettings),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared DB pool: %w", err)
	}

	metricsCM := NewCacheManager(downloader, "metrics", sp, pool, parquetCache)
	logsCM := NewCacheManager(downloader, "logs", sp, pool, parquetCache)
	tracesCM := NewCacheManager(downloader, "traces", sp, pool, parquetCache)

	return &WorkerService{
		MetricsCM:            metricsCM,
		LogsCM:               logsCM,
		TracesCM:             tracesCM,
		StorageProfilePoller: sp,
		MetricsGlobSize:      metricsGlobSize,
		LogsGlobSize:         logsGlobSize,
		TracesGlobSize:       tracesGlobSize,
		pool:                 pool,
		parquetCache:         parquetCache,
	}, nil
}

func sketchInputMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	buf, release := acquireScanBuffers(len(cols))
	defer release()

	if err := row.Scan(buf.ptrs...); err != nil {
		slog.Error("failed to scan row", "err", err)
		return promql.SketchInput{}, fmt.Errorf("failed to scan row: %w", err)
	}

	var ts int64
	var agg map[string]float64
	vals := buf.vals
	tags := make(map[string]any, len(request.BaseExpr.Matchers)+len(cols)+1)

	tags["name"] = request.BaseExpr.Metric
	for _, matcher := range request.BaseExpr.Matchers {
		if matcher.Op == promql.MatchEq {
			tags[matcher.Label] = matcher.Value
		}
	}

	for i, col := range cols {
		switch col {
		case "bucket_ts":
			switch v := vals[i].(type) {
			case int64:
				ts = v
			case int32:
				ts = int64(v)
			case int:
				ts = int64(v)
			default:
				slog.Error("unexpected type for bucket_ts", "value", vals[i])
				continue
			}
		case promql.SUM, promql.COUNT, promql.MIN, promql.MAX:
			if vals[i] == nil {
				continue
			}
			if f, ok := toFloat64(vals[i]); ok {
				if agg == nil {
					agg = make(map[string]float64, 4)
				}
				agg[col] = f
			}
		default:
			if vals[i] != nil {
				tags[col] = vals[i]
			}
		}
	}

	return promql.SketchInput{
		ExprID:         request.BaseExpr.ID,
		OrganizationID: request.OrganizationID.String(),
		Timestamp:      ts,
		Frequency:      int64(request.Step.Seconds()),
		SketchTags: promql.SketchTags{
			Tags:       tags,
			SketchType: promql.SketchMAP,
			Agg:        agg,
		},
	}, nil
}

// summarySketchMapper maps rows from summary queries (IsSummary=true) to SketchInput with DDSketch bytes.
// Summary rows contain group-by columns + merged chq_sketch, no timestamp (it's an aggregation across time).
func summarySketchMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	buf, release := acquireScanBuffers(len(cols))
	defer release()

	if err := row.Scan(buf.ptrs...); err != nil {
		slog.Error("failed to scan summary row", "err", err)
		return promql.SketchInput{}, fmt.Errorf("failed to scan row: %w", err)
	}

	vals := buf.vals
	tags := make(map[string]any, len(request.BaseExpr.Matchers)+len(cols)+1)

	tags["name"] = request.BaseExpr.Metric
	for _, matcher := range request.BaseExpr.Matchers {
		if matcher.Op == promql.MatchEq {
			tags[matcher.Label] = matcher.Value
		}
	}

	var sketchBytes []byte

	for i, col := range cols {
		switch col {
		case "chq_sketch":
			if vals[i] != nil {
				switch v := vals[i].(type) {
				case []byte:
					sketchBytes = v
				default:
					slog.Warn("unexpected type for chq_sketch", "type", fmt.Sprintf("%T", vals[i]))
				}
			}
		default:
			if vals[i] != nil {
				tags[col] = vals[i]
			}
		}
	}

	return promql.SketchInput{
		ExprID:         request.BaseExpr.ID,
		OrganizationID: request.OrganizationID.String(),
		Timestamp:      0, // Summary has no timestamp; it aggregates across all time
		Frequency:      int64(request.Step.Seconds()),
		SketchTags: promql.SketchTags{
			Tags:       tags,
			SketchType: promql.SketchDDS,
			Bytes:      sketchBytes,
		},
	}, nil
}

func exemplarMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	buf, release := acquireScanBuffers(len(cols))
	defer release()

	if err := row.Scan(buf.ptrs...); err != nil {
		slog.Error("failed to scan row", "err", err)
		return promql.Exemplar{}, fmt.Errorf("failed to scan row: %w", err)
	}

	tags := make(map[string]any, len(cols))
	exemplar := promql.Exemplar{}
	vals := buf.vals
	hasTimestamp := false

	for i, col := range cols {
		switch col {
		case "chq_timestamp":
			exemplar.Timestamp = vals[i].(int64)
			hasTimestamp = true
		case "chq_tsns":
			exemplar.TimestampNs = vals[i].(int64)
			tags["chq_tsns"] = vals[i]
		case "metric_name":
			// skip: metric_name is junk in log records
		default:
			if vals[i] != nil {
				tags[col] = vals[i]
			}
		}
	}

	// Derive timestamp from chq_tsns if chq_timestamp wasn't present
	if !hasTimestamp && exemplar.TimestampNs != 0 {
		exemplar.Timestamp = exemplar.TimestampNs / 1_000_000
	}

	exemplar.Tags = tags
	return exemplar, nil
}

func tagValuesMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	buf, release := acquireScanBuffers(len(cols))
	defer release()

	if err := row.Scan(buf.ptrs...); err != nil {
		slog.Error("failed to scan row", "err", err)
		return promql.TagValue{}, fmt.Errorf("failed to scan row: %w", err)
	}

	tagValue := promql.TagValue{}
	vals := buf.vals
	for i, col := range cols {
		switch col {
		case "tag_value":
			if vals[i] != nil {
				tagValue.Value = asString(vals[i])
			}
		}
	}

	return tagValue, nil
}

func asString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}

// ServeHttp serves SSE with merged, sorted points from cache+S3.
func (ws *WorkerService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Extract trace context from incoming request headers
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/queryworker")
	ctx, requestSpan := tracer.Start(ctx, "query.worker.handle_request")
	defer requestSpan.End()

	var req queryapi.PushDownRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		requestSpan.RecordError(err)
		requestSpan.SetStatus(codes.Error, "bad json")
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Segments) == 0 {
		requestSpan.SetStatus(codes.Error, "no segments")
		http.Error(w, "no segments", http.StatusBadRequest)
		return
	}

	// Set common request attributes
	requestSpan.SetAttributes(
		attribute.String("organization_id", req.OrganizationID.String()),
		attribute.Int("segment_count", len(req.Segments)),
		attribute.Int64("start_ts", req.StartTs),
		attribute.Int64("end_ts", req.EndTs),
		attribute.Int("limit", req.Limit),
		attribute.Bool("reverse", req.Reverse),
	)

	var workerSql string
	var cacheManager *CacheManager
	var globSize int
	var isTagValuesQuery = false
	var isSummaryQuery = false // true for summary queries that return DDSketch per series

	if req.BaseExpr != nil {
		if req.TagNames {
			// Return distinct tag names (column names) instead of values
			workerSql = req.BaseExpr.ToWorkerSQLForTagNames()
			cacheManager = ws.MetricsCM
			globSize = ws.MetricsGlobSize
			isTagValuesQuery = true // Use same string-based streaming handler for tag names
		} else if req.TagName != "" {
			workerSql = req.BaseExpr.ToWorkerSQLForTagValues(req.Step, req.TagName)
			cacheManager = ws.MetricsCM
			globSize = ws.MetricsGlobSize
			isTagValuesQuery = true
		} else if req.IsSummary {
			// Summary query: return merged DDSketch per series
			workerSql = req.BaseExpr.ToWorkerSummarySQL()
			cacheManager = ws.MetricsCM
			globSize = ws.MetricsGlobSize
			isSummaryQuery = true
		} else {
			workerSql = req.BaseExpr.ToWorkerSQL(req.Step)
			if req.BaseExpr.LogLeaf != nil {
				cacheManager = ws.LogsCM
				globSize = ws.LogsGlobSize
			} else {
				cacheManager = ws.MetricsCM
				globSize = ws.MetricsGlobSize
			}
		}
	} else if req.LogLeaf != nil {
		if req.TagNames {
			// Return distinct tag names (column names) instead of values
			// Use different SQL generators for logs vs spans since they have different schemas
			if req.IsSpans {
				workerSql = req.LogLeaf.ToSpansWorkerSQLForTagNames()
				cacheManager = ws.TracesCM
				globSize = ws.TracesGlobSize
			} else {
				workerSql = req.LogLeaf.ToWorkerSQLForTagNames()
				cacheManager = ws.LogsCM
				globSize = ws.LogsGlobSize
			}
			isTagValuesQuery = true // Use same string-based streaming handler for tag names
		} else if req.TagName != "" {
			workerSql = req.LogLeaf.ToWorkerSQLForTagValues(req.TagName)
			if req.IsSpans {
				cacheManager = ws.TracesCM
				globSize = ws.TracesGlobSize
			} else {
				cacheManager = ws.LogsCM
				globSize = ws.LogsGlobSize
			}
			isTagValuesQuery = true
		} else {
			if req.IsSpans {
				workerSql = req.LogLeaf.ToSpansWorkerSQLWithLimit(req.Limit, req.ToOrderString(), req.Fields)
				cacheManager = ws.TracesCM
				globSize = ws.TracesGlobSize
			} else {
				workerSql = req.LogLeaf.ToWorkerSQLWithLimit(req.Limit, req.ToOrderString(), req.Fields)
				cacheManager = ws.LogsCM
				globSize = ws.LogsGlobSize
			}
		}
	} else {
		requestSpan.SetStatus(codes.Error, "no leaf to evaluate")
		http.Error(w, "no leaf to evaluate", http.StatusBadRequest)
		return
	}

	// Determine query type for span attributes
	var queryType string
	switch {
	case isTagValuesQuery:
		queryType = "tag_values"
	case isSummaryQuery:
		queryType = "metrics_summary"
	case req.BaseExpr != nil && req.BaseExpr.LogLeaf != nil:
		queryType = "metrics_over_logs"
	case req.BaseExpr != nil:
		queryType = "metrics"
	case req.IsSpans:
		queryType = "spans"
	case req.LogLeaf != nil:
		queryType = "logs"
	default:
		queryType = "unknown"
	}
	requestSpan.SetAttributes(attribute.String("query_type", queryType))

	// group request.segments by organizationId and instanceNum
	segmentsByOrg := make(map[uuid.UUID]map[int16][]queryapi.SegmentInfo)
	for _, seg := range req.Segments {
		if _, ok := segmentsByOrg[seg.OrganizationID]; !ok {
			segmentsByOrg[seg.OrganizationID] = make(map[int16][]queryapi.SegmentInfo)
		}
		if _, ok := segmentsByOrg[seg.OrganizationID][seg.InstanceNum]; !ok {
			segmentsByOrg[seg.OrganizationID][seg.InstanceNum] = []queryapi.SegmentInfo{}
		}
		segmentsByOrg[seg.OrganizationID][seg.InstanceNum] = append(segmentsByOrg[seg.OrganizationID][seg.InstanceNum], seg)
	}

	// Fill time placeholders. {table} stays in; CacheManager.EvaluatePushDown will replace it appropriately.
	workerSql = strings.ReplaceAll(workerSql, "{start}", fmt.Sprintf("%d", req.StartTs))
	workerSql = strings.ReplaceAll(workerSql, "{end}", fmt.Sprintf("%d", req.EndTs))

	if cacheManager == nil {
		requestSpan.SetStatus(codes.Error, "cache manager not initialized")
		http.Error(w, "cache manager not initialized for this query type", http.StatusInternalServerError)
		return
	}

	if isTagValuesQuery {
		// Handle tag values query
		tagValuesChannel, err := EvaluatePushDown(ctx, cacheManager, req, workerSql, globSize, tagValuesMapper)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "query failed")
			slog.Error("failed to query cache", "error", err)
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Convert TagValue channel to Timestamped channel
		responseChannel := make(chan promql.Timestamped, 100)
		go func() {
			defer close(responseChannel)
			for tv := range tagValuesChannel {
				responseChannel <- tv
			}
		}()

		// Process the response
		ws.processResponse(w, responseChannel, ctx)
		return

	} else if isSummaryQuery {
		// Handle summary query (DDSketch per series)
		sketchChannel, err := EvaluatePushDown(ctx, cacheManager, req, workerSql, globSize, summarySketchMapper)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "summary query failed")
			slog.Error("failed to query cache for summary", "error", err)
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Convert SketchInput channel to Timestamped channel
		responseChannel := make(chan promql.Timestamped, 100)
		go func() {
			defer close(responseChannel)
			for si := range sketchChannel {
				responseChannel <- si
			}
		}()

		// Process the response
		ws.processResponse(w, responseChannel, ctx)
		return

	} else if req.BaseExpr != nil {
		// Handle metrics query
		sketchChannel, err := EvaluatePushDown(ctx, cacheManager, req, workerSql, globSize, sketchInputMapper)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "query failed")
			slog.Error("failed to query cache", "error", err)
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Convert SketchInput channel to Timestamped channel
		responseChannel := make(chan promql.Timestamped, 100)
		go func() {
			defer close(responseChannel)
			for si := range sketchChannel {
				responseChannel <- si
			}
		}()

		// Process the response
		ws.processResponse(w, responseChannel, ctx)
		return

	} else if req.LogLeaf != nil {
		// Handle logs query
		exemplarChannel, err := EvaluatePushDown(ctx, cacheManager, req, workerSql, globSize, exemplarMapper)
		if err != nil {
			requestSpan.RecordError(err)
			requestSpan.SetStatus(codes.Error, "query failed")
			slog.Error("failed to query cache", "error", err)
			http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
			return
		}

		// Convert Exemplar channel to Timestamped channel
		responseChannel := make(chan promql.Timestamped, 100)
		go func() {
			defer close(responseChannel)
			for ex := range exemplarChannel {
				responseChannel <- ex
			}
		}()

		// Process the response
		ws.processResponse(w, responseChannel, ctx)
		return
	}
}

func (ws *WorkerService) processResponse(w http.ResponseWriter, responseChannel <-chan promql.Timestamped, ctx context.Context) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	writeSSE := func(event string, v any) error {
		type envelope struct {
			Type string `json:"type"`
			Data any    `json:"data,omitempty"`
		}
		env := envelope{Type: event, Data: v}

		data, err := json.Marshal(env)
		if err != nil {
			return err
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// Stream until channel closes or client disconnects.
	for {
		select {
		case <-ctx.Done():
			slog.Info("client disconnected; stopping stream")
			return

		case res, ok := <-responseChannel:
			if !ok {
				_ = writeSSE("done", map[string]string{"status": "ok"})
				return
			}
			if err := writeSSE("result", res); err != nil {
				slog.Error("write SSE failed", "error", err)
				return
			}
		}
	}
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	case uint64:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint:
		return float64(n), true
	case *big.Int:
		f, _ := new(big.Float).SetInt(n).Float64()
		return f, true
	default:
		slog.Error("unexpected type for numeric value", "value", v, "type", fmt.Sprintf("%T", v))
		return 0, false
	}
}

func (ws *WorkerService) Run(doneCtx context.Context) error {
	slog.Info("Starting query-worker service")

	mux := http.NewServeMux()
	mux.Handle("/api/v1/pushDown", ws) // supports GET + POST

	srv := &http.Server{
		Addr:    ":8081",
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HTTP server", slog.Any("error", err))
		}
	}()

	<-doneCtx.Done()

	slog.Info("Shutting down query-worker service")
	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("Failed to shutdown HTTP server", slog.Any("error", err))
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	// Clean up resources
	ws.Close()

	return nil
}

func (ws *WorkerService) Close() {
	if ws.MetricsCM != nil {
		ws.MetricsCM.Close()
	}
	if ws.LogsCM != nil {
		ws.LogsCM.Close()
	}
	if ws.TracesCM != nil {
		ws.TracesCM.Close()
	}
	if ws.parquetCache != nil {
		ws.parquetCache.Close()
	}
	if ws.pool != nil {
		_ = ws.pool.Close()
	}
}
