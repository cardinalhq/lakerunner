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

package queryapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var metadataDefaultTimeRangeCounter metric.Int64Counter

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryapi")

	var err error
	metadataDefaultTimeRangeCounter, err = meter.Int64Counter(
		"lakerunner.queryapi.metadata_default_time_range_total",
		metric.WithDescription("Count of metadata requests using default 1-hour time range"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create metadata_default_time_range_total counter: %w", err))
	}
}

type promTagsReq struct {
	S      string `json:"s"`
	E      string `json:"e"`
	Metric string `json:"metric,omitempty"`
}

type metricItem struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type promTagsListMetricsResp struct {
	Metrics []metricItem `json:"metrics"`
}

type promTagsForMetricResp struct {
	Metric metricItem `json:"metric"`
	Tags   []string   `json:"tags"`
}

func (q *QuerierService) handleListPromQLMetricsMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "unsupported content type", http.StatusBadRequest)
		return
	}

	// Get orgId from context (set by middleware)
	orgUUID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	// Parse optional time range from request body
	var req promTagsReq
	body, _ := io.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	_ = json.Unmarshal(body, &req)

	ctx := r.Context()
	out := promTagsListMetricsResp{Metrics: make([]metricItem, 0)}

	// Determine time range - use provided values or default to last hour
	var startTs, endTs int64
	var usingDefault bool

	if req.S != "" && req.E != "" {
		var err error
		startTs, endTs, err = dateutils.ToStartEnd(req.S, req.E)
		if err != nil {
			http.Error(w, "invalid start/end time: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		// Default to last 1 hour
		now := time.Now()
		endTs = now.UnixMilli()
		startTs = now.Add(-1 * time.Hour).UnixMilli()
		usingDefault = true
		metadataDefaultTimeRangeCounter.Add(ctx, 1)
		slog.Debug("metrics metadata using default 1-hour time range")
	}

	startTime := time.Unix(0, startTs*1e6).UTC()
	endTime := time.Unix(0, endTs*1e6).UTC()
	startDateint := int32(startTime.Year()*10000 + int(startTime.Month())*100 + startTime.Day())
	endDateint := int32(endTime.Year()*10000 + int(endTime.Month())*100 + endTime.Day())

	rows, err := q.mdb.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgUUID,
		StartDateint:   startDateint,
		EndDateint:     endDateint,
		StartTs:        startTs,
		EndTs:          endTs,
	})
	if err != nil {
		slog.Error("ListMetricNamesWithTypes failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if len(rows) > 0 {
		slog.Info("metrics metadata using fast path", "count", len(rows), "default_range", usingDefault)
		for _, m := range rows {
			out.Metrics = append(out.Metrics, metricItem{
				Name: m.MetricName,
				Type: lrdb.MetricTypeToString(m.MetricType),
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (q *QuerierService) handleListPromQLTags(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "only POST method is allowed", http.StatusMethodNotAllowed)
		return
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
		http.Error(w, "unsupported content type", http.StatusBadRequest)
		return
	}

	var req promTagsReq
	body, _ := io.ReadAll(r.Body)
	defer func() { _ = r.Body.Close() }()
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Get orgId from context (set by middleware)
	orgUUID, ok := GetOrgIDFromContext(r.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	metric := strings.TrimSpace(req.Metric)
	if metric == "" {
		http.Error(w, "missing metric", http.StatusBadRequest)
		return
	}

	// Parse time range from request, or use defaults (yesterday to today)
	var startDateint, endDateint int32
	if req.S != "" && req.E != "" {
		startTs, endTs, err := dateutils.ToStartEnd(req.S, req.E)
		if err != nil {
			http.Error(w, "invalid start/end time: "+err.Error(), http.StatusBadRequest)
			return
		}
		// Convert timestamps to dateints for partition pruning
		startTime := time.Unix(0, startTs*1e6).UTC()
		endTime := time.Unix(0, endTs*1e6).UTC()
		startDateint = int32(startTime.Year()*10000 + int(startTime.Month())*100 + startTime.Day())
		endDateint = int32(endTime.Year()*10000 + int(endTime.Month())*100 + endTime.Day())
	} else {
		// Default to yesterday and today for partition pruning
		now := time.Now().UTC()
		endDateint = int32(now.Year()*10000 + int(now.Month())*100 + now.Day())
		yesterday := now.AddDate(0, 0, -1)
		startDateint = int32(yesterday.Year()*10000 + int(yesterday.Month())*100 + yesterday.Day())
	}

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	mt, err := q.mdb.GetMetricType(ctx, lrdb.GetMetricTypeParams{
		OrganizationID: orgUUID,
		MetricName:     metric,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "metric not found for org", http.StatusNotFound)
			return
		}
		slog.Error("GetMetricType failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Compute fingerprint for the metric name to filter segments
	metricFingerprint := fingerprint.ComputeFingerprint("metric_name", metric)

	tags, err := q.mdb.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgUUID,
		StartDateint:      startDateint,
		EndDateint:        endDateint,
		MetricFingerprint: metricFingerprint,
	})
	if err != nil {
		slog.Error("ListPromMetricTags failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := promTagsForMetricResp{
		Metric: metricItem{Name: metric, Type: mt},
		Tags:   tags,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
