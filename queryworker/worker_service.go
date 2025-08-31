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
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// WorkerService wires HTTP → CacheManager → SSE.
type WorkerService struct {
	CM                   *CacheManager
	StorageProfilePoller storageprofile.StorageProfileProvider
	S3GlobSize           int
}

func NewWorkerService(
	s3GlobSize int,
	maxConcurrency int,
	sp storageprofile.StorageProfileProvider,
	awsMgr *awsclient.Manager,
) *WorkerService {
	downloader := func(ctx context.Context, profile storageprofile.StorageProfile, keys []string) error {
		if len(keys) == 0 {
			return nil
		}

		s3cli, err := awsMgr.GetS3ForProfile(ctx, profile)
		if err != nil {
			return fmt.Errorf("failed to get S3 client for profile %w", err)
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
				tmpfn, _, is404, err := s3helper.DownloadS3Object(gctx, dir, s3cli, profile.Bucket, key)
				if err != nil {
					slog.Error("Failed to download S3 object",
						slog.String("bucket", profile.Bucket),
						slog.String("objectID", key),
						slog.Any("error", err))
					return err
				}
				if is404 {
					// Non-fatal skip
					slog.Info("S3 object not found, skipping",
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

	cm := NewCacheManager(downloader, sp)
	return &WorkerService{
		CM:                   cm,
		StorageProfilePoller: sp,
		S3GlobSize:           s3GlobSize,
	}
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

	exemplar := promql.Exemplar{}

	for i, col := range cols {
		switch col {
		case "_cardinalhq.timestamp":
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

	var workerSql = ""
	var rowMapper RowMapper[promql.Timestamped]
	if req.BaseExpr != nil {
		workerSql = req.BaseExpr.ToWorkerSQL(req.Step)
		rowMapper = sketchInputMapper
	} else if req.LogLeaf != nil {
		workerSql = req.LogLeaf.ToWorkerSQL(req.Step)
		rowMapper = exemplarMapper
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

	responseChannel, err := EvaluatePushDown[promql.Timestamped](r.Context(), ws.CM, req, workerSql, ws.S3GlobSize, rowMapper)
	if err != nil {
		slog.Error("failed to query cache", "error", err)
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if err != nil {
		http.Error(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

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
	ctx := r.Context()
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

	return nil
}
