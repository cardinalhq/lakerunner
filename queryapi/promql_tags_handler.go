package queryapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

type promTagsReq struct {
	OrgID  string `json:"orgId"`
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
	defer r.Body.Close()
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.OrgID) == "" {
		http.Error(w, "missing orgId", http.StatusBadRequest)
		return
	}
	orgUUID, err := uuid.Parse(req.OrgID)
	if err != nil {
		http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	w.Header().Set("Content-Type", "application/json")

	if strings.TrimSpace(req.Metric) == "" {
		rows, err := q.mdb.ListPromMetrics(ctx, orgUUID)
		if err != nil {
			slog.Error("ListPromMetrics failed", slog.Any("error", err))
			http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
			return
		}
		out := promTagsListMetricsResp{Metrics: make([]metricItem, 0, len(rows))}
		for _, r := range rows {
			out.Metrics = append(out.Metrics, metricItem{
				Name: r.MetricName,
				Type: r.MetricType,
			})
		}
		_ = json.NewEncoder(w).Encode(out)
		return
	}

	mt, err := q.mdb.GetMetricType(ctx, lrdb.GetMetricTypeParams{
		OrganizationID: orgUUID,
		MetricName:     req.Metric,
	})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "metric not found for org", http.StatusNotFound)
			return
		}
		slog.Error("GetMetricType failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	tagKeys, err := q.mdb.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID: orgUUID,
		MetricName:     req.Metric,
	})
	if err != nil {
		slog.Error("ListPromMetricTags failed", slog.Any("error", err))
		http.Error(w, "db error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	resp := promTagsForMetricResp{
		Metric: metricItem{Name: req.Metric, Type: mt},
		Tags:   tagKeys,
	}
	_ = json.NewEncoder(w).Encode(resp)
}
