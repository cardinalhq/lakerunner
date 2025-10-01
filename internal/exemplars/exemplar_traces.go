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

// createTracesCallback creates a callback function for traces exemplars for a specific organization
func (p *Processor) createTracesCallback(ctx context.Context, organizationID string) func([]*Entry) {
	return func(entries []*Entry) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing traces exemplars",
			slog.String("organization_id", organizationID),
			slog.Int("count", len(entries)))

		rows := make([]pipeline.Row, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, entry.value)
		}

		if len(rows) == 0 {
			return
		}

		if p.sendTracesExemplars != nil {
			if err := p.sendTracesExemplars(context.Background(), organizationID, rows); err != nil {
				ll.Error("Failed to send traces exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// ProcessTracesFromRow processes traces from a Row and generates exemplars
// This method uses the already-processed Row data with underscore field names
func (p *Processor) ProcessTracesFromRow(ctx context.Context, organizationID string, row pipeline.Row) error {
	if !p.config.Traces.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.traceCache == nil {
		return nil
	}

	clusterName := p.getStringFromRow(row, RowKeyResourceK8sClusterName)
	namespaceName := p.getStringFromRow(row, RowKeyResourceK8sNamespaceName)
	serviceName := p.getStringFromRow(row, RowKeyResourceServiceName)

	var fingerprint int64
	if val, ok := row[wkk.RowKeyCFingerprint]; ok {
		switch v := val.(type) {
		case int64:
			fingerprint = v
		case int:
			fingerprint = int64(v)
		case float64:
			fingerprint = int64(v)
		}
	}

	key := computeLogsTracesKey(clusterName, namespaceName, serviceName, fingerprint)

	if tenant.traceCache.Contains(key) {
		return nil
	}

	exemplarRow := pipeline.CopyRow(row)
	tenant.traceCache.Put(key, exemplarRow)
	return nil
}

// SetTracesCallback updates the sendTracesExemplars callback function
func (p *Processor) SetTracesCallback(callback func(ctx context.Context, organizationID string, exemplars []pipeline.Row) error) {
	p.sendTracesExemplars = callback
}
