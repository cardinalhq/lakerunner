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
	"log/slog"
	"sort"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func processLogsExemplarsDirect(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row, store LogIngestStore) error {
	var err error
	slog.Info("Processing logs exemplars",
		"num_exemplars", len(rows),
		"organization_id", organizationID.String())

	// Sort rows deterministically to prevent deadlocks
	sort.Slice(rows, func(i, j int) bool {
		iService := getRowString(rows[i], exemplars.RowKeyResourceServiceName)
		jService := getRowString(rows[j], exemplars.RowKeyResourceServiceName)
		if iService != jService {
			return iService < jService
		}
		iCluster := getRowString(rows[i], exemplars.RowKeyResourceK8sClusterName)
		jCluster := getRowString(rows[j], exemplars.RowKeyResourceK8sClusterName)
		if iCluster != jCluster {
			return iCluster < jCluster
		}
		iNamespace := getRowString(rows[i], exemplars.RowKeyResourceK8sNamespaceName)
		jNamespace := getRowString(rows[j], exemplars.RowKeyResourceK8sNamespaceName)
		if iNamespace != jNamespace {
			return iNamespace < jNamespace
		}
		iFingerprint := getRowString(rows[i], wkk.RowKeyCFingerprint)
		jFingerprint := getRowString(rows[j], wkk.RowKeyCFingerprint)
		return iFingerprint < jFingerprint
	})

	records := make([]lrdb.BatchUpsertExemplarLogsParams, 0, len(rows))

	for _, row := range rows {
		serviceName := getRowString(row, exemplars.RowKeyResourceServiceName)
		clusterName := getRowString(row, exemplars.RowKeyResourceK8sClusterName)
		namespaceName := getRowString(row, exemplars.RowKeyResourceK8sNamespaceName)

		// Get fingerprint from Row
		var fingerprint int64
		if val, ok := row[wkk.RowKeyCFingerprint]; ok {
			switch v := val.(type) {
			case int64:
				fingerprint = v
			case int:
				fingerprint = int64(v)
			case float64:
				fingerprint = int64(v)
			case string:
				fingerprint, err = strconv.ParseInt(v, 10, 64)
				if err != nil {
					slog.Error("Failed to parse fingerprint", "error", err, "fingerprint", v)
					continue
				}
			}
		}

		if fingerprint == 0 {
			slog.Warn("Missing fingerprint")
			continue
		}

		// Get old fingerprint from Row (0 if not present)
		var oldFingerprint int64
		if val, ok := row[exemplars.RowKeyCardinalhqOldFingerprint]; ok {
			switch v := val.(type) {
			case int64:
				oldFingerprint = v
			case int:
				oldFingerprint = int64(v)
			case float64:
				oldFingerprint = int64(v)
			case string:
				if v != "" && v != "0" {
					oldFingerprint, _ = strconv.ParseInt(v, 10, 64)
				}
			}
		}

		params := lrdb.UpsertServiceIdentifierParams{
			OrganizationID: pgtype.UUID{Bytes: organizationID, Valid: true},
			ServiceName:    &serviceName,
			ClusterName:    &clusterName,
			Namespace:      &namespaceName,
		}

		result, err := store.UpsertServiceIdentifier(ctx, params)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}
		serviceIdentifierID := result.ID

		// Convert Row to map for storage
		exemplarMap := make(map[string]any)
		for k, v := range row {
			key := wkk.RowKeyValue(k)
			exemplarMap[key] = v
		}

		record := lrdb.BatchUpsertExemplarLogsParams{
			OrganizationID:      organizationID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			OldFingerprint:      oldFingerprint,
			Exemplar:            exemplarMap,
			Source:              lrdb.ExemplarSourceLakerunner,
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

// getRowString safely extracts a string value from a Row
func getRowString(row pipeline.Row, key wkk.RowKey) string {
	if val, ok := row[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

func processMetricsExemplarsDirect(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row, store MetricIngestStore) error {
	slog.Info("Processing metrics exemplars",
		"num_exemplars", len(rows),
		"organization_id", organizationID.String())

	// Sort rows deterministically to prevent deadlocks
	sort.Slice(rows, func(i, j int) bool {
		iService := getRowString(rows[i], exemplars.RowKeyResourceServiceName)
		jService := getRowString(rows[j], exemplars.RowKeyResourceServiceName)
		if iService != jService {
			return iService < jService
		}
		iCluster := getRowString(rows[i], exemplars.RowKeyResourceK8sClusterName)
		jCluster := getRowString(rows[j], exemplars.RowKeyResourceK8sClusterName)
		if iCluster != jCluster {
			return iCluster < jCluster
		}
		iNamespace := getRowString(rows[i], exemplars.RowKeyResourceK8sNamespaceName)
		jNamespace := getRowString(rows[j], exemplars.RowKeyResourceK8sNamespaceName)
		if iNamespace != jNamespace {
			return iNamespace < jNamespace
		}
		iMetricName := getRowString(rows[i], wkk.RowKeyCName)
		jMetricName := getRowString(rows[j], wkk.RowKeyCName)
		if iMetricName != jMetricName {
			return iMetricName < jMetricName
		}
		iMetricType := getRowString(rows[i], wkk.RowKeyCMetricType)
		jMetricType := getRowString(rows[j], wkk.RowKeyCMetricType)
		return iMetricType < jMetricType
	})

	records := make([]lrdb.BatchUpsertExemplarMetricsParams, 0, len(rows))

	for _, row := range rows {
		serviceName := getRowString(row, exemplars.RowKeyResourceServiceName)
		clusterName := getRowString(row, exemplars.RowKeyResourceK8sClusterName)
		namespaceName := getRowString(row, exemplars.RowKeyResourceK8sNamespaceName)
		metricName := getRowString(row, wkk.RowKeyCName)
		metricType := getRowString(row, wkk.RowKeyCMetricType)

		if metricName == "" || metricType == "" {
			slog.Warn("Missing metric name or type", "metric_name", metricName, "metric_type", metricType)
			continue
		}

		params := lrdb.UpsertServiceIdentifierParams{
			OrganizationID: pgtype.UUID{Bytes: organizationID, Valid: true},
			ServiceName:    &serviceName,
			ClusterName:    &clusterName,
			Namespace:      &namespaceName,
		}

		result, err := store.UpsertServiceIdentifier(ctx, params)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}
		serviceIdentifierID := result.ID

		// Convert Row to map for storage
		exemplarMap := make(map[string]any)
		for k, v := range row {
			key := wkk.RowKeyValue(k)
			exemplarMap[key] = v
		}

		record := lrdb.BatchUpsertExemplarMetricsParams{
			OrganizationID:      organizationID,
			ServiceIdentifierID: serviceIdentifierID,
			MetricName:          metricName,
			MetricType:          metricType,
			Exemplar:            exemplarMap,
			Source:              lrdb.ExemplarSourceLakerunner,
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

func processTracesExemplarsDirect(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row, store TraceIngestStore) error {
	var err error
	slog.Info("Processing traces exemplars",
		"num_exemplars", len(rows),
		"organization_id", organizationID.String())

	// Sort rows deterministically to prevent deadlocks
	sort.Slice(rows, func(i, j int) bool {
		iService := getRowString(rows[i], exemplars.RowKeyResourceServiceName)
		jService := getRowString(rows[j], exemplars.RowKeyResourceServiceName)
		if iService != jService {
			return iService < jService
		}
		iCluster := getRowString(rows[i], exemplars.RowKeyResourceK8sClusterName)
		jCluster := getRowString(rows[j], exemplars.RowKeyResourceK8sClusterName)
		if iCluster != jCluster {
			return iCluster < jCluster
		}
		iNamespace := getRowString(rows[i], exemplars.RowKeyResourceK8sNamespaceName)
		jNamespace := getRowString(rows[j], exemplars.RowKeyResourceK8sNamespaceName)
		if iNamespace != jNamespace {
			return iNamespace < jNamespace
		}
		iFingerprint := getRowString(rows[i], wkk.RowKeyCFingerprint)
		jFingerprint := getRowString(rows[j], wkk.RowKeyCFingerprint)
		return iFingerprint < jFingerprint
	})

	records := make([]lrdb.BatchUpsertExemplarTracesParams, 0, len(rows))

	for _, row := range rows {
		serviceName := getRowString(row, exemplars.RowKeyResourceServiceName)
		clusterName := getRowString(row, exemplars.RowKeyResourceK8sClusterName)
		namespaceName := getRowString(row, exemplars.RowKeyResourceK8sNamespaceName)
		spanName := getRowString(row, exemplars.RowKeySpanName)
		spanKind := getRowString(row, exemplars.RowKeySpanKind)

		// Get fingerprint from Row
		var fingerprint int64
		if val, ok := row[wkk.RowKeyCFingerprint]; ok {
			switch v := val.(type) {
			case int64:
				fingerprint = v
			case int:
				fingerprint = int64(v)
			case float64:
				fingerprint = int64(v)
			case string:
				fingerprint, err = strconv.ParseInt(v, 10, 64)
				if err != nil {
					slog.Error("Failed to parse fingerprint", "error", err, "fingerprint", v)
					continue
				}
			}
		}

		if fingerprint == 0 {
			slog.Warn("Missing fingerprint")
			continue
		}

		params := lrdb.UpsertServiceIdentifierParams{
			OrganizationID: pgtype.UUID{Bytes: organizationID, Valid: true},
			ServiceName:    &serviceName,
			ClusterName:    &clusterName,
			Namespace:      &namespaceName,
		}

		result, err := store.UpsertServiceIdentifier(ctx, params)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}
		serviceIdentifierID := result.ID

		// Convert Row to map for storage
		exemplarMap := make(map[string]any)
		for k, v := range row {
			key := wkk.RowKeyValue(k)
			exemplarMap[key] = v
		}

		record := lrdb.BatchUpsertExemplarTracesParams{
			OrganizationID:      organizationID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			Exemplar:            exemplarMap,
			SpanName:            spanName,
			SpanKind:            convertSpanKindToInt32(spanKind),
			Source:              lrdb.ExemplarSourceLakerunner,
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
