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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/google/uuid"
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
	s3Pool               *duckdbx.S3DB // shared pool for all queries
}

func NewWorkerService(
	metricsGlobSize int,
	logsGlobSize int,
	tracesGlobSize int,
	maxConcurrency int,
	sp storageprofile.StorageProfileProvider,
	cloudManagers cloudstorage.ClientProvider,
) (*WorkerService, error) {
	downloader := func(ctx context.Context, profile storageprofile.StorageProfile, keys []string) error {
		if len(keys) == 0 {
			return nil
		}

		storageClient, err := cloudstorage.NewClient(ctx, cloudManagers, profile)
		if err != nil {
			return fmt.Errorf("failed to create storage client for provider %s: %w", profile.CloudProvider, err)
		}

		g, gctx := errgroup.WithContext(ctx)
		maxConc := 4 * runtime.GOMAXPROCS(0)
		if maxConc > maxConcurrency {
			maxConc = maxConcurrency
		}
		g.SetLimit(maxConc)

		for _, key := range keys {
			key := key // capture loop var
			g.Go(func() error {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				// keep same relative layout locally
				localPath := key
				dir := filepath.Dir(localPath)

				if err := os.MkdirAll(dir, 0o755); err != nil {
					return fmt.Errorf("mkdir %q: %w", dir, err)
				}

				// IMPORTANT: pass the directory, not the file path
				tmpfn, _, is404, err := storageClient.DownloadObject(gctx, dir, profile.Bucket, key)
				if err != nil {
					slog.Error("Failed to download object",
						slog.String("cloudProvider", profile.CloudProvider),
						slog.String("bucket", profile.Bucket),
						slog.String("objectID", key),
						slog.Any("error", err))
					return err
				}
				if is404 {
					// Non-fatal skip
					slog.Info("Object not found, skipping",
						slog.String("cloudProvider", profile.CloudProvider),
						slog.String("bucket", profile.Bucket),
						slog.String("objectID", key))
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

	// Create a single shared S3DB pool for all queries with metrics enabled
	s3Pool, err := duckdbx.NewS3DB(
		duckdbx.WithS3DBMetrics(10*time.Second),
		duckdbx.WithConnectionMaxAge(30*time.Minute),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create shared S3DB pool: %w", err)
	}

	return &WorkerService{
		MetricsCM:            NewCacheManager(downloader, "metrics", sp, s3Pool),
		LogsCM:               NewCacheManager(downloader, "logs", sp, s3Pool),
		TracesCM:             NewCacheManager(downloader, "traces", sp, s3Pool),
		StorageProfilePoller: sp,
		MetricsGlobSize:      metricsGlobSize,
		LogsGlobSize:         logsGlobSize,
		TracesGlobSize:       tracesGlobSize,
		s3Pool:               s3Pool,
	}, nil
}

func sketchInputMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	if err := row.Scan(ptrs...); err != nil {
		slog.Error("failed to scan row", "err", err)
		return promql.SketchInput{}, fmt.Errorf("failed to scan row: %w", err)
	}

	var ts int64
	agg := map[string]float64{}
	tags := map[string]any{}

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

func exemplarMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	tags := map[string]any{}

	if err := row.Scan(ptrs...); err != nil {
		slog.Error("failed to scan row", "err", err)
		return promql.Exemplar{}, fmt.Errorf("failed to scan row: %w", err)
	}

	//slog.Info("Found exemplar row", "cols", cols, "vals", vals)

	exemplar := promql.Exemplar{}

	for i, col := range cols {
		switch col {
		case "chq_timestamp":
			exemplar.Timestamp = vals[i].(int64)
		default:
			if vals[i] != nil {
				tags[col] = vals[i]
			}
		}
	}

	exemplar.Tags = tags
	return exemplar, nil
}

func tagValuesMapper(request queryapi.PushDownRequest, cols []string, row *sql.Rows) (promql.Timestamped, error) {
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	if err := row.Scan(ptrs...); err != nil {
		slog.Error("failed to scan row", "err", err)
		return promql.TagValue{}, fmt.Errorf("failed to scan row: %w", err)
	}

	tagValue := promql.TagValue{}
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
	var req queryapi.PushDownRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Segments) == 0 {
		http.Error(w, "no segments", http.StatusBadRequest)
		return
	}

	var workerSql string
	var cacheManager *CacheManager
	var globSize int
	var isTagValuesQuery = false

	if req.BaseExpr != nil {
		if req.TagName != "" {
			workerSql = req.BaseExpr.ToWorkerSQLForTagValues(req.Step, req.TagName)
			cacheManager = ws.MetricsCM
			globSize = ws.MetricsGlobSize
			isTagValuesQuery = true
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
		if req.TagName != "" {
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
		http.Error(w, "no leaf to evaluate", http.StatusBadRequest)
		return
	}

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
		http.Error(w, "cache manager not initialized for this query type", http.StatusInternalServerError)
		return
	}

	if isTagValuesQuery {
		// Handle tag values query
		tagValuesChannel, err := EvaluatePushDown(r.Context(), cacheManager, req, workerSql, globSize, tagValuesMapper)
		if err != nil {
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
		ws.processResponse(w, responseChannel, r.Context())
		return

	} else if req.BaseExpr != nil {
		// Handle metrics query
		sketchChannel, err := EvaluatePushDown(r.Context(), cacheManager, req, workerSql, globSize, sketchInputMapper)
		if err != nil {
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
		ws.processResponse(w, responseChannel, r.Context())
		return

	} else if req.LogLeaf != nil {
		// Handle logs query
		exemplarChannel, err := EvaluatePushDown(r.Context(), cacheManager, req, workerSql, globSize, exemplarMapper)
		if err != nil {
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
		ws.processResponse(w, responseChannel, r.Context())
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
	if ws.s3Pool != nil {
		_ = ws.s3Pool.Close()
	}
}
