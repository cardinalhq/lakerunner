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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func processMetricsExemplarsDirect(ctx context.Context, organizationID string, exemplars []*exemplar.ExemplarData, mdb lrdb.StoreFull) error {
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

		var exemplarData any
		if err := json.Unmarshal([]byte(exemplar.Payload), &exemplarData); err != nil {
			slog.Error("Failed to parse exemplar payload", "error", err)
			continue
		}

		serviceIdentifierID, err := upsertServiceIdentifierDirect(ctx, mdb, orgID, serviceName, clusterName, namespaceName)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}

		attributesAny := make(map[string]any)
		for k, v := range exemplar.Attributes {
			attributesAny[k] = v
		}

		var exemplarMap map[string]any
		if exemplarDataMap, ok := exemplarData.(map[string]any); ok {
			exemplarMap = exemplarDataMap
		} else {
			exemplarBytes, err := json.Marshal(exemplarData)
			if err != nil {
				slog.Error("Failed to marshal exemplar data", "error", err)
				continue
			}
			if err := json.Unmarshal(exemplarBytes, &exemplarMap); err != nil {
				slog.Error("Failed to convert exemplar data to map", "error", err)
				continue
			}
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

	batchResults := mdb.BatchUpsertExemplarMetrics(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar metric", "error", err, "index", i)
		}
	})

	slog.Info("Processed metrics exemplars", "count", len(records))
	return nil
}

func upsertServiceIdentifierDirect(ctx context.Context, mdb lrdb.StoreFull, orgID uuid.UUID, serviceName, clusterName, namespaceName string) (uuid.UUID, error) {
	params := lrdb.UpsertServiceIdentifierParams{
		OrganizationID: pgtype.UUID{Bytes: orgID, Valid: true},
		ServiceName:    pgtype.Text{String: serviceName, Valid: true},
		ClusterName:    pgtype.Text{String: clusterName, Valid: true},
		Namespace:      pgtype.Text{String: namespaceName, Valid: true},
	}

	result, err := mdb.UpsertServiceIdentifier(ctx, params)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to upsert service identifier: %w", err)
	}

	return result.ID, nil
}

// processExemplarsFromReader processes exemplars from a metrics reader that supports OTEL
func processExemplarsFromReader(_ context.Context, reader filereader.Reader, processor *exemplar.Processor, orgID string, mdb lrdb.StoreFull) error {
	// Check if the reader provides OTEL metrics
	if otelProvider, ok := reader.(filereader.OTELMetricsProvider); ok {
		otelMetrics, err := otelProvider.GetOTELMetrics()
		if err != nil {
			return fmt.Errorf("failed to get OTEL metrics: %w", err)
		}

		if metrics, ok := otelMetrics.(*pmetric.Metrics); ok {
			if err := processExemplarsFromMetrics(metrics, processor, orgID); err != nil {
				return fmt.Errorf("failed to process exemplars from metrics: %w", err)
			}
		}
	}
	return nil
}

// processExemplarsFromMetrics processes exemplars from parsed pmetric.Metrics
func processExemplarsFromMetrics(metrics *pmetric.Metrics, processor *exemplar.Processor, customerID string) error {
	ctx := context.Background()
	if err := processor.ProcessMetrics(ctx, *metrics, customerID); err != nil {
		return fmt.Errorf("failed to process metrics exemplars: %w", err)
	}
	return nil
}
