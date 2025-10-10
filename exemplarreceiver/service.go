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

// handleExemplar processes incoming exemplar data
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

	// Unmarshal directly to Row
	var row pipeline.Row
	if err := row.Unmarshal(body); err != nil {
		http.Error(w, fmt.Sprintf("invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Process based on signal type
	switch signal {
	case "logs":
		err = r.processLogsExemplar(req.Context(), orgID, row)
	case "metrics":
		err = r.processMetricsExemplar(req.Context(), orgID, row)
	case "traces":
		err = r.processTracesExemplar(req.Context(), orgID, row)
	default:
		http.Error(w, fmt.Sprintf("unknown signal type: %s (must be logs, metrics, or traces)", signal), http.StatusBadRequest)
		return
	}

	if err != nil {
		slog.Error("Failed to process exemplar", "signal", signal, "error", err)
		http.Error(w, fmt.Sprintf("failed to process exemplar: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
}

// processLogsExemplar processes a logs exemplar
func (r *ReceiverService) processLogsExemplar(ctx context.Context, orgID uuid.UUID, row pipeline.Row) error {
	serviceName := row.GetString(wkk.NewRowKey("service_name"))
	clusterName := row.GetString(wkk.NewRowKey("cluster_name"))
	namespaceName := row.GetString(wkk.NewRowKey("namespace"))

	// Prefer client-provided fingerprint, compute fallback if not provided
	fingerprint, _ := row.GetInt64(wkk.NewRowKey("fingerprint"))
	if fingerprint == 0 {
		fingerprint = computeLogsFingerprint(row)
	}
	oldFingerprint, _ := row.GetInt64(wkk.NewRowKey("old_fingerprint"))

	// Upsert service identifier
	serviceIdentifierID, err := r.upsertServiceIdentifier(ctx, orgID, serviceName, clusterName, namespaceName)
	if err != nil {
		return fmt.Errorf("failed to upsert service identifier: %w", err)
	}

	// Get source from row, default to external source
	source := row.GetString(wkk.NewRowKey("source"))
	if source == "" {
		source = string(lrdb.ExemplarSourceDatadog)
	}

	// Convert Row to map[string]any for storage in JSONB
	exemplarData := rowToMap(row)

	// Prepare batch upsert params
	records := []lrdb.BatchUpsertExemplarLogsParams{
		{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			OldFingerprint:      oldFingerprint,
			Exemplar:            exemplarData,
			Source:              lrdb.ExemplarSource(source),
		},
	}

	batchResults := r.db.BatchUpsertExemplarLogs(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar log", "error", err, "index", i)
		} else {
			slog.Info("Upserted logs exemplar", "is_new", isNew, "fingerprint", fingerprint)
		}
	})

	return nil
}

// processMetricsExemplar processes a metrics exemplar
func (r *ReceiverService) processMetricsExemplar(ctx context.Context, orgID uuid.UUID, row pipeline.Row) error {
	serviceName := row.GetString(wkk.NewRowKey("service_name"))
	clusterName := row.GetString(wkk.NewRowKey("cluster_name"))
	namespaceName := row.GetString(wkk.NewRowKey("namespace"))
	metricName := row.GetString(wkk.NewRowKey("metric_name"))
	metricType := row.GetString(wkk.NewRowKey("metric_type"))

	if metricName == "" || metricType == "" {
		return fmt.Errorf("metric_name and metric_type are required")
	}

	// Upsert service identifier
	serviceIdentifierID, err := r.upsertServiceIdentifier(ctx, orgID, serviceName, clusterName, namespaceName)
	if err != nil {
		return fmt.Errorf("failed to upsert service identifier: %w", err)
	}

	// Get source from row, default to external source
	source := row.GetString(wkk.NewRowKey("source"))
	if source == "" {
		source = string(lrdb.ExemplarSourceDatadog)
	}

	// Convert Row to map[string]any for storage in JSONB
	exemplarData := rowToMap(row)

	// Prepare batch upsert params
	records := []lrdb.BatchUpsertExemplarMetricsParams{
		{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			MetricName:          metricName,
			MetricType:          metricType,
			Exemplar:            exemplarData,
			Source:              lrdb.ExemplarSource(source),
		},
	}

	batchResults := r.db.BatchUpsertExemplarMetrics(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar metric", "error", err, "index", i)
		} else {
			slog.Info("Upserted metrics exemplar", "is_new", isNew, "metric_name", metricName)
		}
	})

	return nil
}

// processTracesExemplar processes a traces exemplar
func (r *ReceiverService) processTracesExemplar(ctx context.Context, orgID uuid.UUID, row pipeline.Row) error {
	serviceName := row.GetString(wkk.NewRowKey("service_name"))
	clusterName := row.GetString(wkk.NewRowKey("cluster_name"))
	namespaceName := row.GetString(wkk.NewRowKey("namespace"))
	spanName := row.GetString(wkk.NewRowKey("span_name"))
	spanKind, _ := row.GetInt32(wkk.NewRowKey("span_kind"))

	// Compute fingerprint from the row
	fingerprint := computeTracesFingerprint(row)

	// Upsert service identifier
	serviceIdentifierID, err := r.upsertServiceIdentifier(ctx, orgID, serviceName, clusterName, namespaceName)
	if err != nil {
		return fmt.Errorf("failed to upsert service identifier: %w", err)
	}

	// Get source from row, default to external source
	source := row.GetString(wkk.NewRowKey("source"))
	if source == "" {
		source = string(lrdb.ExemplarSourceDatadog)
	}

	// Convert Row to map[string]any for storage in JSONB
	exemplarData := rowToMap(row)

	// Prepare batch upsert params
	records := []lrdb.BatchUpsertExemplarTracesParams{
		{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			Exemplar:            exemplarData,
			SpanName:            spanName,
			SpanKind:            spanKind,
			Source:              lrdb.ExemplarSource(source),
		},
	}

	batchResults := r.db.BatchUpsertExemplarTraces(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar trace", "error", err, "index", i)
		} else {
			slog.Info("Upserted traces exemplar", "is_new", isNew, "fingerprint", fingerprint)
		}
	})

	return nil
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

// rowToMap converts a Row back to map[string]any for JSONB storage
func rowToMap(row pipeline.Row) map[string]any {
	result := make(map[string]any, len(row))
	for k, v := range row {
		result[wkk.RowKeyValue(k)] = v
	}
	return result
}
