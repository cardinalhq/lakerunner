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
	"fmt"
	"log/slog"
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

	records := make([]lrdb.BatchUpsertExemplarLogsParams, 0, len(exemplars))

	for _, exemplar := range exemplars {
		serviceName := exemplar.Attributes["service.name"]
		clusterName := exemplar.Attributes["k8s.cluster.name"]
		namespaceName := exemplar.Attributes["k8s.namespace.name"]
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

		record := lrdb.BatchUpsertExemplarLogsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			OldFingerprint:      oldFingerprint,
			Attributes:          attributesAny,
			Exemplar:            exemplar.Payload,
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

	records := make([]lrdb.BatchUpsertExemplarMetricsParams, 0, len(exemplars))

	for _, exemplar := range exemplars {
		serviceName := exemplar.Attributes["service.name"]
		clusterName := exemplar.Attributes["k8s.cluster.name"]
		namespaceName := exemplar.Attributes["k8s.namespace.name"]
		metricName := exemplar.Attributes["metric.name"]
		metricType := exemplar.Attributes["metric.type"]

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

		record := lrdb.BatchUpsertExemplarMetricsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			MetricName:          metricName,
			MetricType:          metricType,
			Attributes:          attributesAny,
			Exemplar:            exemplar.Payload,
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
