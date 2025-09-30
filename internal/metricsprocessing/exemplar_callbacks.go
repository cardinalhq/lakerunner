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

package metricsprocessing

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func processLogsExemplarsDirect(ctx context.Context, organizationID string, exemplars []*exemplars.ExemplarData, store LogIngestStore) error {
	orgID, err := uuid.Parse(organizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	slog.Info("Processing logs exemplars",
		"num_exemplars", len(exemplars),
		"organization_id", organizationID)

	// Sort exemplars deterministically to prevent deadlocks
	sort.Slice(exemplars, func(i, j int) bool {
		if exemplars[i].Attributes["service_name"] != exemplars[j].Attributes["service_name"] {
			return exemplars[i].Attributes["service_name"] < exemplars[j].Attributes["service_name"]
		}
		if exemplars[i].Attributes["k8s_cluster_name"] != exemplars[j].Attributes["k8s_cluster_name"] {
			return exemplars[i].Attributes["k8s_cluster_name"] < exemplars[j].Attributes["k8s_cluster_name"]
		}
		if exemplars[i].Attributes["k8s_namespace_name"] != exemplars[j].Attributes["k8s_namespace_name"] {
			return exemplars[i].Attributes["k8s_namespace_name"] < exemplars[j].Attributes["k8s_namespace_name"]
		}
		return exemplars[i].Attributes["fingerprint"] < exemplars[j].Attributes["fingerprint"]
	})

	records := make([]lrdb.BatchUpsertExemplarLogsParams, 0, len(exemplars))

	for _, exemplar := range exemplars {
		serviceName := exemplar.Attributes["service_name"]
		clusterName := exemplar.Attributes["k8s_cluster_name"]
		namespaceName := exemplar.Attributes["k8s_namespace_name"]
		fingerprintStr := exemplar.Attributes["fingerprint"]
		oldFingerprintStr := exemplar.Attributes["old_fingerprint"]

		if fingerprintStr == "" {
			slog.Warn("Missing fingerprint", "fingerprint", fingerprintStr)
			continue
		}

		fingerprint, err := strconv.ParseInt(fingerprintStr, 10, 64)
		if err != nil {
			slog.Error("Failed to parse fingerprint", "error", err, "fingerprint", fingerprintStr)
			continue
		}

		// Parse old fingerprint (0 if not present or invalid)
		var oldFingerprint int64
		if oldFingerprintStr != "" && oldFingerprintStr != "0" {
			oldFingerprint, err = strconv.ParseInt(oldFingerprintStr, 10, 64)
			if err != nil {
				slog.Debug("Failed to parse old fingerprint, using 0", "error", err, "old_fingerprint", oldFingerprintStr)
				oldFingerprint = 0
			}
		}

		params := lrdb.UpsertServiceIdentifierParams{
			OrganizationID: pgtype.UUID{Bytes: orgID, Valid: true},
			ServiceName:    pgtype.Text{String: serviceName, Valid: true},
			ClusterName:    pgtype.Text{String: clusterName, Valid: true},
			Namespace:      pgtype.Text{String: namespaceName, Valid: true},
		}

		result, err := store.UpsertServiceIdentifier(ctx, params)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}
		serviceIdentifierID := result.ID

		attributesAny := make(map[string]any)
		for k, v := range exemplar.Attributes {
			attributesAny[k] = v
		}

		var exemplarMap map[string]any
		if err := json.Unmarshal([]byte(exemplar.Payload), &exemplarMap); err != nil {
			slog.Error("Failed to parse exemplar payload", "error", err)
			continue
		}

		record := lrdb.BatchUpsertExemplarLogsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			OldFingerprint:      oldFingerprint,
			Attributes:          attributesAny,
			Exemplar:            exemplarMap,
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil
	}

	batchResults := store.BatchUpsertExemplarLogs(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar log", "error", err, "index", i)
		}
	})

	slog.Info("Processed logs exemplars", "count", len(records))
	return nil
}

func processMetricsExemplarsDirect(ctx context.Context, organizationID string, exemplars []*exemplars.ExemplarData, store MetricIngestStore) error {
	orgID, err := uuid.Parse(organizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	slog.Info("Processing metrics exemplars",
		"num_exemplars", len(exemplars),
		"organization_id", organizationID)

	// Sort exemplars deterministically to prevent deadlocks
	sort.Slice(exemplars, func(i, j int) bool {
		if exemplars[i].Attributes["service_name"] != exemplars[j].Attributes["service_name"] {
			return exemplars[i].Attributes["service_name"] < exemplars[j].Attributes["service_name"]
		}
		if exemplars[i].Attributes["k8s_cluster_name"] != exemplars[j].Attributes["k8s_cluster_name"] {
			return exemplars[i].Attributes["k8s_cluster_name"] < exemplars[j].Attributes["k8s_cluster_name"]
		}
		if exemplars[i].Attributes["k8s_namespace_name"] != exemplars[j].Attributes["k8s_namespace_name"] {
			return exemplars[i].Attributes["k8s_namespace_name"] < exemplars[j].Attributes["k8s_namespace_name"]
		}
		if exemplars[i].Attributes["metric_name"] != exemplars[j].Attributes["metric_name"] {
			return exemplars[i].Attributes["metric_name"] < exemplars[j].Attributes["metric_name"]
		}
		return exemplars[i].Attributes["metric_type"] < exemplars[j].Attributes["metric_type"]
	})

	records := make([]lrdb.BatchUpsertExemplarMetricsParams, 0, len(exemplars))

	for _, exemplar := range exemplars {
		serviceName := exemplar.Attributes["service_name"]
		clusterName := exemplar.Attributes["k8s_cluster_name"]
		namespaceName := exemplar.Attributes["k8s_namespace_name"]
		metricName := exemplar.Attributes["metric_name"]
		metricType := exemplar.Attributes["metric_type"]

		if metricName == "" || metricType == "" {
			slog.Warn("Missing metric name or type", "metric_name", metricName, "metric_type", metricType)
			continue
		}

		params := lrdb.UpsertServiceIdentifierParams{
			OrganizationID: pgtype.UUID{Bytes: orgID, Valid: true},
			ServiceName:    pgtype.Text{String: serviceName, Valid: true},
			ClusterName:    pgtype.Text{String: clusterName, Valid: true},
			Namespace:      pgtype.Text{String: namespaceName, Valid: true},
		}

		result, err := store.UpsertServiceIdentifier(ctx, params)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}
		serviceIdentifierID := result.ID

		attributesAny := make(map[string]any)
		for k, v := range exemplar.Attributes {
			attributesAny[k] = v
		}

		var exemplarMap map[string]any
		if err := json.Unmarshal([]byte(exemplar.Payload), &exemplarMap); err != nil {
			slog.Error("Failed to parse exemplar payload", "error", err)
			continue
		}

		record := lrdb.BatchUpsertExemplarMetricsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			MetricName:          metricName,
			MetricType:          metricType,
			Attributes:          attributesAny,
			Exemplar:            exemplarMap,
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil
	}

	batchResults := store.BatchUpsertExemplarMetrics(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar metric", "error", err, "index", i)
		}
	})

	slog.Info("Processed metrics exemplars", "count", len(records))
	return nil
}

func processTracesExemplarsDirect(ctx context.Context, organizationID string, exemplars []*exemplars.ExemplarData, store TraceIngestStore) error {
	orgID, err := uuid.Parse(organizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	slog.Info("Processing traces exemplars",
		"num_exemplars", len(exemplars),
		"organization_id", organizationID)

	// Sort exemplars deterministically to prevent deadlocks
	sort.Slice(exemplars, func(i, j int) bool {
		if exemplars[i].Attributes["service_name"] != exemplars[j].Attributes["service_name"] {
			return exemplars[i].Attributes["service_name"] < exemplars[j].Attributes["service_name"]
		}
		if exemplars[i].Attributes["k8s_cluster_name"] != exemplars[j].Attributes["k8s_cluster_name"] {
			return exemplars[i].Attributes["k8s_cluster_name"] < exemplars[j].Attributes["k8s_cluster_name"]
		}
		if exemplars[i].Attributes["k8s_namespace_name"] != exemplars[j].Attributes["k8s_namespace_name"] {
			return exemplars[i].Attributes["k8s_namespace_name"] < exemplars[j].Attributes["k8s_namespace_name"]
		}
		return exemplars[i].Attributes["fingerprint"] < exemplars[j].Attributes["fingerprint"]
	})

	records := make([]lrdb.BatchUpsertExemplarTracesParams, 0, len(exemplars))

	for _, exemplar := range exemplars {
		serviceName := exemplar.Attributes["service_name"]
		clusterName := exemplar.Attributes["k8s_cluster_name"]
		namespaceName := exemplar.Attributes["k8s_namespace_name"]
		fingerprintStr := exemplar.Attributes["fingerprint"]
		spanName := exemplar.Attributes["span_name"]
		spanKind := exemplar.Attributes["span_kind"]

		if fingerprintStr == "" {
			slog.Warn("Missing fingerprint", "fingerprint", fingerprintStr)
			continue
		}

		fingerprint, err := strconv.ParseInt(fingerprintStr, 10, 64)
		if err != nil {
			slog.Error("Failed to parse fingerprint", "error", err, "fingerprint", fingerprintStr)
			continue
		}

		params := lrdb.UpsertServiceIdentifierParams{
			OrganizationID: pgtype.UUID{Bytes: orgID, Valid: true},
			ServiceName:    pgtype.Text{String: serviceName, Valid: true},
			ClusterName:    pgtype.Text{String: clusterName, Valid: true},
			Namespace:      pgtype.Text{String: namespaceName, Valid: true},
		}

		result, err := store.UpsertServiceIdentifier(ctx, params)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}
		serviceIdentifierID := result.ID

		attributesAny := make(map[string]any)
		for k, v := range exemplar.Attributes {
			attributesAny[k] = v
		}

		var exemplarMap map[string]any
		if err := json.Unmarshal([]byte(exemplar.Payload), &exemplarMap); err != nil {
			slog.Error("Failed to parse exemplar payload", "error", err)
			continue
		}

		record := lrdb.BatchUpsertExemplarTracesParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			Attributes:          attributesAny,
			Exemplar:            exemplarMap,
			SpanName:            spanName,
			SpanKind:            convertSpanKindToInt32(spanKind),
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil
	}

	batchResults := store.BatchUpsertExemplarTraces(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar trace", "error", err, "index", i)
		}
	})

	slog.Info("Processed traces exemplars", "count", len(records))
	return nil
}

// convertSpanKindToInt32 converts OpenTelemetry span kind string to int32
func convertSpanKindToInt32(spanKind string) int32 {
	switch spanKind {
	case "unspecified":
		return 0
	case "internal":
		return 1
	case "server":
		return 2
	case "client":
		return 3
	case "producer":
		return 4
	case "consumer":
		return 5
	default:
		return 0 // Default to unspecified
	}
}
