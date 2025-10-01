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
	"strconv"

	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// createLogsCallback creates a callback function for logs exemplars for a specific organization
func (p *Processor) createLogsCallback(ctx context.Context, organizationID string) func([]*Entry[plog.Logs]) {
	return func(entries []*Entry[plog.Logs]) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing logs exemplars",
			slog.String("organization_id", organizationID),
			slog.Int("count", len(entries)))

		exemplarData := make([]*ExemplarData, 0, len(entries))
		for _, entry := range entries {
			data, err := p.marshalLogs(entry.value)
			if err != nil {
				ll.Error("Failed to marshal logs data", slog.Any("error", err))
				continue
			}

			resourceAttributes := entry.value.ResourceLogs().At(0).Resource().Attributes()
			attributes := p.toAttributes(resourceAttributes)
			attributes["fingerprint"] = strconv.FormatInt(getLogFingerprint(entry.value.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)), 10)

			exemplarData = append(exemplarData, &ExemplarData{
				Attributes:  attributes,
				PartitionId: 0,
				Payload:     data,
			})
		}

		if len(exemplarData) == 0 {
			return
		}

		if p.sendLogsExemplars != nil {
			if err := p.sendLogsExemplars(context.Background(), organizationID, exemplarData); err != nil {
				ll.Error("Failed to send logs exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// marshalLogs serializes plog.Logs to JSON string
func (p *Processor) marshalLogs(ld plog.Logs) (string, error) {
	marshaller := &plog.JSONMarshaler{}
	bytes, err := marshaller.MarshalLogs(ld)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ProcessLogs processes logs and generates exemplars for a specific organization
func (p *Processor) ProcessLogs(ctx context.Context, organizationID string, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	if !p.config.Logs.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.logCache == nil {
		return nil
	}

	p.addLogExemplar(tenant, rl, sl, lr)
	return nil
}

// ProcessLogsFromRow processes logs from a Row and generates exemplars
// This method uses the already-processed Row data with underscore field names
func (p *Processor) ProcessLogsFromRow(ctx context.Context, organizationID string, row map[wkk.RowKey]any, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) error {
	if !p.config.Logs.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.logCache == nil {
		return nil
	}

	// Extract key fields from the Row using underscore names
	clusterName := p.getStringFromRow(row, wkk.NewRowKey("resource_k8s_cluster_name"))
	namespaceName := p.getStringFromRow(row, wkk.NewRowKey("resource_k8s_namespace_name"))
	serviceName := p.getStringFromRow(row, wkk.NewRowKey("resource_service_name"))

	// Get fingerprint from the Row if available
	var fingerprint int64
	if val, ok := row[wkk.NewRowKey("_cardinalhq_fingerprint")]; ok {
		switch v := val.(type) {
		case int64:
			fingerprint = v
		case int:
			fingerprint = int64(v)
		default:
			// Fall back to getting it from the log record
			fingerprint = getLogFingerprint(lr)
		}
	} else {
		fingerprint = getLogFingerprint(lr)
	}

	// Build cache key using underscore field names from Row
	key := clusterName + "|" + namespaceName + "|" + serviceName + "|" + strconv.FormatInt(fingerprint, 10)

	if tenant.logCache.Contains(key) {
		return nil
	}

	// Still use OTEL format for the actual exemplar data
	exemplarRecord := toLogExemplar(rl, sl, lr)
	tenant.logCache.Put(key, exemplarRecord)
	return nil
}

// addLogExemplar adds a logs exemplar to the organization's cache
func (p *Processor) addLogExemplar(tenant *Tenant, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) {
	// Get old fingerprint from attributes (if exists from collector)
	fingerprint := getLogFingerprint(lr)
	resource := rl.Resource()
	clusterName := getFromResource(resource.Attributes(), clusterNameKey)
	namespaceName := getFromResource(resource.Attributes(), namespaceNameKey)
	serviceName := getFromResource(resource.Attributes(), serviceNameKey)
	key := clusterName + "|" + namespaceName + "|" + serviceName + "|" + strconv.FormatInt(fingerprint, 10)
	if tenant.logCache.Contains(key) {
		return
	}

	exemplarRecord := toLogExemplar(rl, sl, lr)
	tenant.logCache.Put(key, exemplarRecord)
}

// SetLogsCallback updates the sendLogsExemplars callback function
func (p *Processor) SetLogsCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendLogsExemplars = callback
}
