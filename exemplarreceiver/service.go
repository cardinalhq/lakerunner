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

package exemplarreceiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/orgapikey"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ReceiverService handles incoming exemplar data from external sources
type ReceiverService struct {
	db             lrdb.StoreFull
	apiKeyProvider orgapikey.OrganizationAPIKeyProvider
	port           int
}

// NewReceiverService creates a new exemplar receiver service
func NewReceiverService(db lrdb.StoreFull, apiKeyProvider orgapikey.OrganizationAPIKeyProvider) (*ReceiverService, error) {
	port := 8091 // Default port
	if portStr := os.Getenv("EXEMPLAR_RECEIVER_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	return &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           port,
	}, nil
}

// Run starts the HTTP server
func (r *ReceiverService) Run(ctx context.Context) error {
	router := mux.NewRouter()
	router.HandleFunc("/api/v1/exemplar/{signal}", r.apiKeyMiddleware(r.handleExemplar)).Methods("POST")
	router.HandleFunc("/healthz", r.healthCheck).Methods("GET")

	addr := fmt.Sprintf(":%d", r.port)
	server := &http.Server{
		Addr:              addr,
		Handler:           router,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
	}

	slog.Info("Starting exemplar receiver service", "addr", addr)

	errChan := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-errChan:
		return err
	}
}

// healthCheck provides a simple health check endpoint
func (r *ReceiverService) healthCheck(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleExemplar processes incoming batch exemplar data
func (r *ReceiverService) handleExemplar(w http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	signal := vars["signal"]

	// Get organization ID from context (set by middleware)
	orgID, ok := GetOrgIDFromContext(req.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	// Read raw JSON bytes
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request body: %v", err), http.StatusBadRequest)
		return
	}

	// Unmarshal batch request
	var batchReq ExemplarBatchRequest
	if err := json.Unmarshal(body, &batchReq); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Validate source
	if batchReq.Source == "" {
		http.Error(w, "source is required", http.StatusBadRequest)
		return
	}

	// Validate exemplars array
	if len(batchReq.Exemplars) == 0 {
		http.Error(w, "exemplars array cannot be empty", http.StatusBadRequest)
		return
	}

	// Process based on signal type
	var response *ExemplarBatchResponse
	switch signal {
	case "logs":
		response, err = r.processLogsBatch(req.Context(), orgID, batchReq.Source, batchReq.Exemplars)
	case "metrics":
		response, err = r.processMetricsBatch(req.Context(), orgID, batchReq.Source, batchReq.Exemplars)
	case "traces":
		response, err = r.processTracesBatch(req.Context(), orgID, batchReq.Source, batchReq.Exemplars)
	default:
		http.Error(w, fmt.Sprintf("unknown signal type: %s (must be logs, metrics, or traces)", signal), http.StatusBadRequest)
		return
	}

	if err != nil {
		slog.Error("Failed to process exemplar batch", "signal", signal, "error", err)
		http.Error(w, fmt.Sprintf("failed to process exemplar batch: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(response)
}

// processLogsBatch processes a batch of logs exemplars
func (r *ReceiverService) processLogsBatch(ctx context.Context, orgID uuid.UUID, source string, rawExemplars []map[string]interface{}) (*ExemplarBatchResponse, error) {
	response := &ExemplarBatchResponse{
		Status: "ok",
		Errors: []string{},
	}

	var records []lrdb.BatchUpsertExemplarLogsParams

	for i, raw := range rawExemplars {
		// Unmarshal to LogsExemplar to extract explicit fields
		data, err := json.Marshal(raw)
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: failed to marshal: %v", i, err))
			continue
		}

		var exemplar LogsExemplar
		if err := json.Unmarshal(data, &exemplar); err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: invalid format: %v", i, err))
			continue
		}

		// Validate required fields
		if exemplar.ServiceName == "" || exemplar.ClusterName == "" || exemplar.Namespace == "" {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: service_name, cluster_name, and namespace are required", i))
			continue
		}

		// Upsert service identifier
		serviceIdentifierID, err := r.upsertServiceIdentifier(ctx, orgID, exemplar.ServiceName, exemplar.ClusterName, exemplar.Namespace)
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: failed to upsert service identifier: %v", i, err))
			continue
		}

		// Convert to Row for fingerprinting
		row := make(pipeline.Row)
		for k, v := range exemplar.Data {
			row[wkk.NewRowKey(k)] = v
		}

		// Always compute fingerprint server-side
		fingerprint := computeLogsFingerprint(row)

		// Check for old_fingerprint field (optional)
		oldFingerprint := int64(0)
		if v, ok := exemplar.Data["old_fingerprint"]; ok {
			switch val := v.(type) {
			case int64:
				oldFingerprint = val
			case float64:
				oldFingerprint = int64(val)
			case int:
				oldFingerprint = int64(val)
			}
		}

		records = append(records, lrdb.BatchUpsertExemplarLogsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			OldFingerprint:      oldFingerprint,
			Exemplar:            exemplar.Data,
			Source:              lrdb.ExemplarSource(source),
		})
	}

	if len(records) == 0 {
		return response, nil
	}

	// Batch upsert all records
	batchResults := r.db.BatchUpsertExemplarLogs(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d: upsert failed: %v", i, err))
			slog.Error("Failed to upsert exemplar log", "error", err, "index", i)
		} else {
			response.Accepted++
			slog.Debug("Upserted logs exemplar", "is_new", isNew, "index", i)
		}
	})

	return response, nil
}

// processMetricsBatch processes a batch of metrics exemplars
func (r *ReceiverService) processMetricsBatch(ctx context.Context, orgID uuid.UUID, source string, rawExemplars []map[string]interface{}) (*ExemplarBatchResponse, error) {
	response := &ExemplarBatchResponse{
		Status: "ok",
		Errors: []string{},
	}

	var records []lrdb.BatchUpsertExemplarMetricsParams

	for i, raw := range rawExemplars {
		// Unmarshal to MetricsExemplar to extract explicit fields
		data, err := json.Marshal(raw)
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: failed to marshal: %v", i, err))
			continue
		}

		var exemplar MetricsExemplar
		if err := json.Unmarshal(data, &exemplar); err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: invalid format: %v", i, err))
			continue
		}

		// Validate required fields
		if exemplar.ServiceName == "" || exemplar.ClusterName == "" || exemplar.Namespace == "" {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: service_name, cluster_name, and namespace are required", i))
			continue
		}

		if exemplar.MetricName == "" || exemplar.MetricType == "" {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: metric_name and metric_type are required", i))
			continue
		}

		// Upsert service identifier
		serviceIdentifierID, err := r.upsertServiceIdentifier(ctx, orgID, exemplar.ServiceName, exemplar.ClusterName, exemplar.Namespace)
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: failed to upsert service identifier: %v", i, err))
			continue
		}

		records = append(records, lrdb.BatchUpsertExemplarMetricsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			MetricName:          exemplar.MetricName,
			MetricType:          exemplar.MetricType,
			Exemplar:            exemplar.Data,
			Source:              lrdb.ExemplarSource(source),
		})
	}

	if len(records) == 0 {
		return response, nil
	}

	// Batch upsert all records
	batchResults := r.db.BatchUpsertExemplarMetrics(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d: upsert failed: %v", i, err))
			slog.Error("Failed to upsert exemplar metric", "error", err, "index", i)
		} else {
			response.Accepted++
			slog.Debug("Upserted metrics exemplar", "is_new", isNew, "index", i)
		}
	})

	return response, nil
}

// processTracesBatch processes a batch of traces exemplars
func (r *ReceiverService) processTracesBatch(ctx context.Context, orgID uuid.UUID, source string, rawExemplars []map[string]interface{}) (*ExemplarBatchResponse, error) {
	response := &ExemplarBatchResponse{
		Status: "ok",
		Errors: []string{},
	}

	var records []lrdb.BatchUpsertExemplarTracesParams

	for i, raw := range rawExemplars {
		// Unmarshal to TracesExemplar to extract explicit fields
		data, err := json.Marshal(raw)
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: failed to marshal: %v", i, err))
			continue
		}

		var exemplar TracesExemplar
		if err := json.Unmarshal(data, &exemplar); err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: invalid format: %v", i, err))
			continue
		}

		// Validate required fields
		if exemplar.ServiceName == "" || exemplar.ClusterName == "" || exemplar.Namespace == "" {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: service_name, cluster_name, and namespace are required", i))
			continue
		}

		if exemplar.SpanName == "" {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: span_name is required", i))
			continue
		}

		// Upsert service identifier
		serviceIdentifierID, err := r.upsertServiceIdentifier(ctx, orgID, exemplar.ServiceName, exemplar.ClusterName, exemplar.Namespace)
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: failed to upsert service identifier: %v", i, err))
			continue
		}

		// Convert to Row for fingerprinting
		row := make(pipeline.Row)
		for k, v := range exemplar.Data {
			row[wkk.NewRowKey(k)] = v
		}

		// Convert span_kind to int32 if it's a string
		spanKind := int32(0)
		if exemplar.SpanKind != nil {
			switch v := exemplar.SpanKind.(type) {
			case int:
				spanKind = int32(v)
			case int32:
				spanKind = v
			case int64:
				spanKind = int32(v)
			case float64:
				spanKind = int32(v)
			case string:
				// Convert string span kind to int32 if needed
				row[wkk.NewRowKey("span_kind")] = v
			}
		}

		// Always compute fingerprint server-side
		fingerprint := computeTracesFingerprint(row)

		records = append(records, lrdb.BatchUpsertExemplarTracesParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			Exemplar:            exemplar.Data,
			SpanName:            exemplar.SpanName,
			SpanKind:            spanKind,
			Source:              lrdb.ExemplarSource(source),
		})
	}

	if len(records) == 0 {
		return response, nil
	}

	// Batch upsert all records
	batchResults := r.db.BatchUpsertExemplarTraces(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d: upsert failed: %v", i, err))
			slog.Error("Failed to upsert exemplar trace", "error", err, "index", i)
		} else {
			response.Accepted++
			slog.Debug("Upserted traces exemplar", "is_new", isNew, "index", i)
		}
	})

	return response, nil
}

// upsertServiceIdentifier creates or updates a service identifier and returns its ID
func (r *ReceiverService) upsertServiceIdentifier(ctx context.Context, orgID uuid.UUID, serviceName, clusterName, namespaceName string) (uuid.UUID, error) {
	params := lrdb.UpsertServiceIdentifierParams{
		OrganizationID: pgtype.UUID{Bytes: orgID, Valid: true},
		ServiceName:    pgtype.Text{String: serviceName, Valid: true},
		ClusterName:    pgtype.Text{String: clusterName, Valid: true},
		Namespace:      pgtype.Text{String: namespaceName, Valid: true},
	}

	result, err := r.db.UpsertServiceIdentifier(ctx, params)
	if err != nil {
		return uuid.Nil, err
	}

	return result.ID, nil
}
