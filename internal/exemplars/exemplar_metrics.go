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

package exemplars

import (
	"context"
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// createMetricsCallback creates a callback function for metrics exemplars for a specific organization
func (p *Processor) createMetricsCallback(ctx context.Context, organizationID string) func([]*Entry[map[wkk.RowKey]any]) {
	return func(entries []*Entry[map[wkk.RowKey]any]) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing metrics exemplars",
			slog.String("organization_id", organizationID),
			slog.Int("count", len(entries)))

		exemplarData := make([]*ExemplarData, 0, len(entries))
		for _, entry := range entries {
			data, err := p.marshalMetricsFromRow(entry.value)
			if err != nil {
				ll.Error("Failed to marshal metrics data from row", slog.Any("error", err))
				continue
			}

			attributes := p.toAttributesFromRow(entry.value)
			metricName := p.getStringFromRow(entry.value, wkk.RowKeyCName)
			metricType := p.getStringFromRow(entry.value, wkk.RowKeyCMetricType)
			attributes["metric_name"] = metricName
			attributes["metric_type"] = metricType

			exemplarData = append(exemplarData, &ExemplarData{
				Attributes:  attributes,
				PartitionId: 0,
				Payload:     data,
			})
		}

		if len(exemplarData) == 0 {
			return
		}

		if p.sendMetricsExemplars != nil {
			if err := p.sendMetricsExemplars(context.Background(), organizationID, exemplarData); err != nil {
				ll.Error("Failed to send exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// marshalMetricsFromRow serializes a Row to JSON for metrics exemplars
func (p *Processor) marshalMetricsFromRow(row map[wkk.RowKey]any) (string, error) {
	bytes, err := pipeline.MarshalRowJSON(row)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ProcessMetricsFromRow processes metrics from a Row and generates exemplars
// This method uses the already-processed Row data with underscore field names
func (p *Processor) ProcessMetricsFromRow(ctx context.Context, organizationID string, row map[wkk.RowKey]any) error {
	if !p.config.Metrics.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.metricCache == nil {
		return nil
	}

	// Extract key fields from the Row using underscore names
	clusterName := p.getStringFromRow(row, wkk.NewRowKey("resource_k8s_cluster_name"))
	namespaceName := p.getStringFromRow(row, wkk.NewRowKey("resource_k8s_namespace_name"))
	serviceName := p.getStringFromRow(row, wkk.NewRowKey("resource_service_name"))

	metricName := p.getStringFromRow(row, wkk.RowKeyCName)
	metricType := p.getStringFromRow(row, wkk.RowKeyCMetricType)

	// Build cache key using underscore field names from Row
	key := clusterName + "|" + namespaceName + "|" + serviceName + "|" + metricName + "|" + metricType

	if tenant.metricCache.Contains(key) {
		return nil
	}

	// Create a copy of the row for the exemplar
	exemplarRow := make(map[wkk.RowKey]any, len(row))
	for k, v := range row {
		exemplarRow[k] = v
	}

	tenant.metricCache.Put(key, exemplarRow)
	return nil
}

// SetMetricsCallback updates the sendMetricsExemplars callback function
func (p *Processor) SetMetricsCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendMetricsExemplars = callback
}
