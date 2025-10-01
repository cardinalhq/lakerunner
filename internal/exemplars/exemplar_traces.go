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

	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// createTracesCallback creates a callback function for traces exemplars for a specific organization
func (p *Processor) createTracesCallback(ctx context.Context, organizationID string) func([]*Entry[ptrace.Traces]) {
	return func(entries []*Entry[ptrace.Traces]) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing traces exemplars",
			slog.String("organization_id", organizationID),
			slog.Int("count", len(entries)))

		exemplarData := make([]*ExemplarData, 0, len(entries))
		for _, entry := range entries {
			data, err := p.marshalTraces(entry.value)
			if err != nil {
				ll.Error("Failed to marshal traces data", slog.Any("error", err))
				continue
			}

			resourceAttributes := entry.value.ResourceSpans().At(0).Resource().Attributes()
			attributes := p.toAttributes(resourceAttributes)
			attributes["fingerprint"] = strconv.FormatInt(getTraceFingerprint(entry.value.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)), 10)
			attributes["span.name"] = entry.value.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Name()
			attributes["span.kind"] = entry.value.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Kind().String()

			exemplarData = append(exemplarData, &ExemplarData{
				Attributes:  attributes,
				PartitionId: 0,
				Payload:     data,
			})
		}

		if len(exemplarData) == 0 {
			return
		}

		if p.sendTracesExemplars != nil {
			if err := p.sendTracesExemplars(context.Background(), organizationID, exemplarData); err != nil {
				ll.Error("Failed to send traces exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// marshalTraces serializes ptrace.Traces to JSON string
func (p *Processor) marshalTraces(td ptrace.Traces) (string, error) {
	marshaller := &ptrace.JSONMarshaler{}
	bytes, err := marshaller.MarshalTraces(td)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ProcessTraces processes traces and generates exemplars for a specific organization
func (p *Processor) ProcessTraces(ctx context.Context, organizationID string, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) error {
	if !p.config.Traces.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.traceCache == nil {
		return nil
	}

	p.addTraceExemplar(tenant, rs, ss, span)
	return nil
}

// addTraceExemplar adds a traces exemplar to the organization's cache
func (p *Processor) addTraceExemplar(tenant *Tenant, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) {
	// Get fingerprint from span attributes (if exists from collector)
	fingerprint := getTraceFingerprint(span)
	resource := rs.Resource()
	clusterName := getFromResource(resource.Attributes(), clusterNameKey)
	namespaceName := getFromResource(resource.Attributes(), namespaceNameKey)
	serviceName := getFromResource(resource.Attributes(), serviceNameKey)
	key := clusterName + "|" + namespaceName + "|" + serviceName + "|" + strconv.FormatInt(fingerprint, 10)
	if tenant.traceCache.Contains(key) {
		return
	}

	exemplarRecord := toTraceExemplar(rs, ss, span)
	tenant.traceCache.Put(key, exemplarRecord)
}

// SetTracesCallback updates the sendTracesExemplars callback function
func (p *Processor) SetTracesCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendTracesExemplars = callback
}
