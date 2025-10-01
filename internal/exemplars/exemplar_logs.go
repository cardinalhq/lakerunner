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
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// createLogsCallback creates a callback function for logs exemplars for a specific organization
func (p *Processor) createLogsCallback(ctx context.Context, organizationID uuid.UUID) func([]*Entry) {
	return func(entries []*Entry) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing logs exemplars",
			slog.String("organization_id", organizationID.String()),
			slog.Int("count", len(entries)))

		rows := make([]pipeline.Row, 0, len(entries))
		for _, entry := range entries {
			rows = append(rows, entry.value)
		}

		if len(rows) == 0 {
			return
		}

		if p.sendLogsExemplars != nil {
			if err := p.sendLogsExemplars(context.Background(), organizationID, rows); err != nil {
				ll.Error("Failed to send logs exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// ProcessLogsFromRow processes logs from a Row and generates exemplars
// This method uses the already-processed Row data with underscore field names
func (p *Processor) ProcessLogsFromRow(ctx context.Context, organizationID uuid.UUID, row pipeline.Row) error {
	if !p.config.Logs.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.logCache == nil {
		return nil
	}

	// Extract key fields from the Row using underscore names
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

	// Compute cache key hash for deduplication
	key := computeLogsTracesKey(clusterName, namespaceName, serviceName, fingerprint)

	if tenant.logCache.Contains(key) {
		return nil
	}

	exemplarRow := pipeline.CopyRow(row)
	tenant.logCache.Put(key, exemplarRow)
	return nil
}

// SetLogsCallback updates the sendLogsExemplars callback function
func (p *Processor) SetLogsCallback(callback func(ctx context.Context, organizationID uuid.UUID, exemplars []pipeline.Row) error) {
	p.sendLogsExemplars = callback
}
