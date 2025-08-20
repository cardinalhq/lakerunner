package query_worker

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
			g.Go(func() error {
				select {
				case <-gctx.Done():
					return gctx.Err()
				default:
				}

				localPath := key // keep same relative layout locally
				if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
					return fmt.Errorf("mkdir %q: %w", filepath.Dir(localPath), err)
				}

				fn, _, is404, err := s3helper.DownloadS3Object(gctx, localPath, s3cli, profile.Bucket, key)
				if err != nil {
					slog.Error("Failed to download S3 object",
						slog.String("bucket", profile.Bucket),
						slog.String("objectID", key),
						slog.Any("error", err))
					return err
				}
				if is404 {
					// Treat 404 as a non-fatal (skip) for this key; do not fail the whole batch.
					slog.Info("S3 object not found, skipping",
						slog.String("bucket", profile.Bucket),
						slog.String("objectID", key))
					return nil
				}

				slog.Info("Successfully downloaded S3 object",
					slog.String("bucket", profile.Bucket),
					slog.String("objectID", key),
					slog.String("localPath", fn))
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

func sketchInputMapper(request promql.PushDownRequest, cols []string, row *sql.Rows) (promql.SketchInput, error) {
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

// PushDownHandler serves SSE with merged, sorted points from cache+S3.
func (ws *WorkerService) PushDownHandler(w http.ResponseWriter, r *http.Request) {
	var req promql.PushDownRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Segments) == 0 {
		http.Error(w, "no segments", http.StatusBadRequest)
		return
	}

	workerSql := req.BaseExpr.ToWorkerSQL(req.Step)

	// group request.segments by organizationId and instanceNum
	segmentsByOrg := make(map[uuid.UUID]map[int16][]promql.SegmentInfo)
	for _, seg := range req.Segments {
		if _, ok := segmentsByOrg[seg.OrganizationID]; !ok {
			segmentsByOrg[seg.OrganizationID] = make(map[int16][]promql.SegmentInfo)
		}
		if _, ok := segmentsByOrg[seg.OrganizationID][seg.InstanceNum]; !ok {
			segmentsByOrg[seg.OrganizationID][seg.InstanceNum] = []promql.SegmentInfo{}
		}
		segmentsByOrg[seg.OrganizationID][seg.InstanceNum] = append(segmentsByOrg[seg.OrganizationID][seg.InstanceNum], seg)
	}

	// Fill time placeholders. {table} stays in; CacheManager.EvaluatePushDown will replace it appropriately.
	workerSql = strings.ReplaceAll(workerSql, "{start}", fmt.Sprintf("%d", req.StartTs))
	workerSql = strings.ReplaceAll(workerSql, "{end}", fmt.Sprintf("%d", req.EndTs))

	responseChannel, err := EvaluatePushDown[promql.SketchInput](r.Context(), ws.CM, req, workerSql, ws.S3GlobSize, sketchInputMapper)
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
