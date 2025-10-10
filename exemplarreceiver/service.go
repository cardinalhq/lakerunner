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
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/orgapikey"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
)

// ExemplarStore defines the database operations needed by the receiver service
type ExemplarStore interface {
	BatchUpsertExemplarLogs(ctx context.Context, params []lrdb.BatchUpsertExemplarLogsParams) *lrdb.BatchUpsertExemplarLogsBatchResults
	BatchUpsertExemplarMetrics(ctx context.Context, params []lrdb.BatchUpsertExemplarMetricsParams) *lrdb.BatchUpsertExemplarMetricsBatchResults
	BatchUpsertExemplarTraces(ctx context.Context, params []lrdb.BatchUpsertExemplarTracesParams) *lrdb.BatchUpsertExemplarTracesBatchResults
	UpsertServiceIdentifier(ctx context.Context, params lrdb.UpsertServiceIdentifierParams) (lrdb.UpsertServiceIdentifierRow, error)
}

// ReceiverService handles incoming exemplar data from external sources
type ReceiverService struct {
	db             ExemplarStore
	apiKeyProvider orgapikey.OrganizationAPIKeyProvider
	port           int
	tenantManager  *fingerprint.TenantManager
	fingerprinter  fingerprinter.Fingerprinter
}

// NewReceiverService creates a new exemplar receiver service
func NewReceiverService(db lrdb.StoreFull, apiKeyProvider orgapikey.OrganizationAPIKeyProvider) (*ReceiverService, error) {
	port := 8080
	if portStr := os.Getenv("LAKERUNNER_EXEMPLAR_RECEIVER_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil {
			port = p
		}
	}

	// Initialize tenant manager with default threshold (0.8 is a reasonable default)
	tenantManager := fingerprint.NewTenantManager(0.8)

	// Initialize fingerprinter with default options
	fp := fingerprinter.NewFingerprinter()

	return &ReceiverService{
		db:             db,
		apiKeyProvider: apiKeyProvider,
		port:           port,
		tenantManager:  tenantManager,
		fingerprinter:  fp,
	}, nil
}

// Run starts the HTTP server
func (r *ReceiverService) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/exemplar/{signal}", r.apiKeyMiddleware(r.handleExemplar))
	mux.HandleFunc("GET /healthz", r.healthCheck)

	addr := fmt.Sprintf(":%d", r.port)
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
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
	signal := req.PathValue("signal")

	// Get organization ID from context (set by middleware)
	orgID, ok := GetOrgIDFromContext(req.Context())
	if !ok {
		http.Error(w, "organization ID not found in context", http.StatusInternalServerError)
		return
	}

	// Process based on signal type
	var response *ExemplarBatchResponse
	var err error
	switch signal {
	case "logs":
		var batchReq LogsBatchRequest
		if err := json.NewDecoder(req.Body).Decode(&batchReq); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
		if batchReq.Source == "" {
			http.Error(w, "source is required", http.StatusBadRequest)
			return
		}
		if len(batchReq.Exemplars) == 0 {
			http.Error(w, "exemplars array cannot be empty", http.StatusBadRequest)
			return
		}
		response, err = r.processLogsBatch(req.Context(), orgID, batchReq.Source, batchReq.Exemplars)
	case "metrics":
		var batchReq MetricsBatchRequest
		if err := json.NewDecoder(req.Body).Decode(&batchReq); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
		if batchReq.Source == "" {
			http.Error(w, "source is required", http.StatusBadRequest)
			return
		}
		if len(batchReq.Exemplars) == 0 {
			http.Error(w, "exemplars array cannot be empty", http.StatusBadRequest)
			return
		}
		response, err = r.processMetricsBatch(req.Context(), orgID, batchReq.Source, batchReq.Exemplars)
	case "traces":
		var batchReq TracesBatchRequest
		if err := json.NewDecoder(req.Body).Decode(&batchReq); err != nil {
			http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
			return
		}
		if batchReq.Source == "" {
			http.Error(w, "source is required", http.StatusBadRequest)
			return
		}
		if len(batchReq.Exemplars) == 0 {
			http.Error(w, "exemplars array cannot be empty", http.StatusBadRequest)
			return
		}
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
func (r *ReceiverService) processLogsBatch(ctx context.Context, orgID uuid.UUID, source string, rawExemplars []LogsExemplar) (*ExemplarBatchResponse, error) {
	slog.Info("Processing logs exemplar batch",
		"organization_id", orgID,
		"source", source,
		"count", len(rawExemplars))

	response := &ExemplarBatchResponse{
		Status: "ok",
		Errors: []string{},
	}

	var records []lrdb.BatchUpsertExemplarLogsParams

	for i, exemplar := range rawExemplars {

		// Validate required fields
		if exemplar.ServiceName == nil || exemplar.ClusterName == nil || exemplar.Namespace == nil {
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

		// Convert attributes to Row for fingerprinting, including message and level
		row := pipeline.CopyRow(exemplar.Attributes)

		// Always compute fingerprint server-side using tenant-specific clustering
		fingerprint, err := computeLogsFingerprint(orgID.String(), exemplar.Message, exemplar.Level, row, r.tenantManager, r.fingerprinter)
		if err != nil {
			// Drop the item if fingerprinting fails
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("exemplar %d: fingerprinting failed: %v", i, err))
			slog.Warn("Fingerprinting failed, dropping exemplar",
				"index", i,
				"message", exemplar.Message,
				"error", err)
			continue
		}

		slog.Debug("Computed fingerprint for log exemplar",
			"index", i,
			"fingerprint", fingerprint,
			"message", exemplar.Message,
			"level", exemplar.Level,
			"service_name", *exemplar.ServiceName,
			"attributes_count", len(exemplar.Attributes))

		records = append(records, lrdb.BatchUpsertExemplarLogsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			Exemplar:            pipeline.ToStringMap(exemplar.Attributes),
			Source:              lrdb.ExemplarSource(source),
		})
	}

	if len(records) == 0 {
		slog.Info("No valid records to insert after processing")
		return response, nil
	}

	slog.Info("Inserting log exemplar records",
		"count", len(records),
		"organization_id", orgID)

	// Batch upsert all records
	batchResults := r.db.BatchUpsertExemplarLogs(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			response.Failed++
			response.Errors = append(response.Errors, fmt.Sprintf("record %d: upsert failed: %v", i, err))
			slog.Error("Failed to upsert exemplar log",
				"error", err,
				"index", i,
				"organization_id", orgID,
				"service_identifier_id", records[i].ServiceIdentifierID,
				"fingerprint", records[i].Fingerprint)
		} else {
			response.Accepted++
			slog.Info("Successfully upserted logs exemplar",
				"is_new", isNew,
				"index", i,
				"organization_id", records[i].OrganizationID,
				"service_identifier_id", records[i].ServiceIdentifierID,
				"fingerprint", records[i].Fingerprint,
				"source", records[i].Source)
		}
	})

	slog.Info("Completed processing logs exemplar batch",
		"accepted", response.Accepted,
		"failed", response.Failed,
		"total", len(rawExemplars))

	return response, nil
}

// processMetricsBatch processes a batch of metrics exemplars
func (r *ReceiverService) processMetricsBatch(ctx context.Context, orgID uuid.UUID, source string, rawExemplars []MetricsExemplar) (*ExemplarBatchResponse, error) {
	response := &ExemplarBatchResponse{
		Status: "ok",
		Errors: []string{},
	}

	var records []lrdb.BatchUpsertExemplarMetricsParams

	for i, exemplar := range rawExemplars {

		// Validate required fields
		if exemplar.ServiceName == nil || exemplar.ClusterName == nil || exemplar.Namespace == nil {
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
			Exemplar:            pipeline.ToStringMap(exemplar.Attributes),
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
func (r *ReceiverService) processTracesBatch(ctx context.Context, orgID uuid.UUID, source string, rawExemplars []TracesExemplar) (*ExemplarBatchResponse, error) {
	response := &ExemplarBatchResponse{
		Status: "ok",
		Errors: []string{},
	}

	var records []lrdb.BatchUpsertExemplarTracesParams

	for i, exemplar := range rawExemplars {

		// Validate required fields
		if exemplar.ServiceName == nil || exemplar.ClusterName == nil || exemplar.Namespace == nil {
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

		// Convert span_kind to int32
		spanKind := int32(0)
		if exemplar.SpanKind != "" {
			switch exemplar.SpanKind {
			case "0", "SPAN_KIND_UNSPECIFIED":
				spanKind = 0
			case "1", "SPAN_KIND_INTERNAL":
				spanKind = 1
			case "2", "SPAN_KIND_SERVER":
				spanKind = 2
			case "3", "SPAN_KIND_CLIENT":
				spanKind = 3
			case "4", "SPAN_KIND_PRODUCER":
				spanKind = 4
			case "5", "SPAN_KIND_CONSUMER":
				spanKind = 5
			}
		}

		// Always compute fingerprint server-side
		fingerprint := computeTracesFingerprint(exemplar.Attributes)

		records = append(records, lrdb.BatchUpsertExemplarTracesParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			Exemplar:            pipeline.ToStringMap(exemplar.Attributes),
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
func (r *ReceiverService) upsertServiceIdentifier(ctx context.Context, orgID uuid.UUID, serviceName, clusterName, namespaceName *string) (uuid.UUID, error) {
	params := lrdb.UpsertServiceIdentifierParams{
		OrganizationID: pgtype.UUID{Bytes: orgID, Valid: true},
		ServiceName:    serviceName,
		ClusterName:    clusterName,
		Namespace:      namespaceName,
	}

	result, err := r.db.UpsertServiceIdentifier(ctx, params)
	if err != nil {
		return uuid.Nil, err
	}

	return result.ID, nil
}
