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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// createMetricsCallback creates a callback function for metrics exemplars for a specific organization
func (p *Processor) createMetricsCallback(ctx context.Context, organizationID uuid.UUID) func([]*Entry) {
	return func(entries []*Entry) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing metrics exemplars",
			slog.String("organization_id", organizationID.String()),
			slog.Int("count", len(entries)))

		rows := make([]pipeline.Row, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, entry.value)
		}

		if len(rows) == 0 {
			return
		}

		if p.sendMetricsExemplars != nil {
			if err := p.sendMetricsExemplars(context.Background(), organizationID, rows); err != nil {
				ll.Error("Failed to send exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// ProcessMetricsFromRow processes metrics from a Row and generates exemplars
// This method uses the already-processed Row data with underscore field names
func (p *Processor) ProcessMetricsFromRow(ctx context.Context, organizationID uuid.UUID, row pipeline.Row) error {
	if !p.config.Metrics.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.metricCache == nil {
		return nil
	}

	clusterName := p.getStringFromRow(row, RowKeyResourceK8sClusterName)
	namespaceName := p.getStringFromRow(row, RowKeyResourceK8sNamespaceName)
	serviceName := p.getStringFromRow(row, RowKeyResourceServiceName)
	metricName := p.getStringFromRow(row, wkk.RowKeyCName)
	metricType := p.getStringFromRow(row, wkk.RowKeyCMetricType)

	key := computeMetricsKey(clusterName, namespaceName, serviceName, metricName, metricType)

	exemplarRow := pipeline.CopyRow(row)
	if !tenant.metricCache.PutIfAbsent(key, exemplarRow) {
		// Key already existed, return the copied row to the pool
		pipeline.ReturnPooledRow(exemplarRow)
	}
	return nil
}

// SetMetricsCallback updates the sendMetricsExemplars callback function
func (p *Processor) SetMetricsCallback(callback func(ctx context.Context, organizationID uuid.UUID, exemplars []pipeline.Row) error) {
	p.sendMetricsExemplars = callback
}
