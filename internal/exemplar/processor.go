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

package exemplar

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TelemetryType represents the type of telemetry being processed
type TelemetryType string

const (
	TelemetryTypeLogs    TelemetryType = "logs"
	TelemetryTypeMetrics TelemetryType = "metrics"
	TelemetryTypeTraces  TelemetryType = "traces"
)

// Tenant holds the caches for each telemetry type for a specific organization
type Tenant struct {
	metricCache *LRUCache[pmetric.Metrics]
	// logCache and traceCache will be added when those telemetry types are implemented
}

// Processor handles exemplar generation from different telemetry types using tenant-based LRU caches
type Processor struct {
	tenants       sync.Map // organizationID -> *Tenant
	logger        *slog.Logger
	telemetryType TelemetryType

	// Callback for metrics exemplars (logs and traces not implemented yet)
	sendMetricsExemplars func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error

	// Type-specific configurations
	config Config
}

// Config holds configuration for different telemetry types
type Config struct {
	Logs    TelemetryConfig
	Metrics TelemetryConfig
	Traces  TelemetryConfig
}

// TelemetryConfig holds configuration for a specific telemetry type
type TelemetryConfig struct {
	Enabled        bool
	CacheSize      int
	Expiry         time.Duration
	ReportInterval time.Duration
	BatchSize      int
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		Logs: TelemetryConfig{
			Enabled:        false,
			CacheSize:      1000,
			Expiry:         5 * time.Minute,
			ReportInterval: 1 * time.Minute,
			BatchSize:      100,
		},
		Metrics: TelemetryConfig{
			Enabled:        true,
			CacheSize:      1000,
			Expiry:         5 * time.Minute,
			ReportInterval: 1 * time.Minute,
			BatchSize:      100,
		},
		Traces: TelemetryConfig{
			Enabled:        false,
			CacheSize:      1000,
			Expiry:         5 * time.Minute,
			ReportInterval: 1 * time.Minute,
			BatchSize:      100,
		},
	}
}

// NewProcessor creates a new processor for a specific telemetry type
func NewProcessor(telemetryType TelemetryType, config Config, logger *slog.Logger) *Processor {
	return &Processor{
		tenants:       sync.Map{},
		logger:        logger,
		telemetryType: telemetryType,
		config:        config,
	}
}

func (p *Processor) getTenant(organizationID string) *Tenant {
	if existing, ok := p.tenants.Load(organizationID); ok {
		return existing.(*Tenant)
	}

	p.logger.Info("Creating new tenant", slog.String("organization_id", organizationID))
	tenant := &Tenant{}

	// Create cache based on processor type
	switch p.telemetryType {
	case TelemetryTypeLogs:
		// Logs not implemented yet
		p.logger.Debug("Logs processing not implemented yet")
	case TelemetryTypeMetrics:
		if p.config.Metrics.Enabled {
			tenant.metricCache = NewLRUCache(
				p.config.Metrics.CacheSize,
				p.config.Metrics.Expiry,
				p.config.Metrics.ReportInterval,
				p.createMetricsCallback(organizationID))
		}
	case TelemetryTypeTraces:
		// Traces not implemented yet
		p.logger.Debug("Traces processing not implemented yet")
	}

	p.tenants.Store(organizationID, tenant)
	return tenant
}

// createMetricsCallback creates a callback function for metrics exemplars for a specific organization
func (p *Processor) createMetricsCallback(organizationID string) func([]*Entry[pmetric.Metrics]) {
	return func(entries []*Entry[pmetric.Metrics]) {
		p.logger.Info("Processing metrics exemplars",
			slog.String("organization_id", organizationID),
			slog.Int("count", len(entries)))

		exemplarData := make([]*ExemplarData, 0, len(entries))
		for _, entry := range entries {
			data, err := p.marshalMetrics(entry.value)
			if err != nil {
				p.logger.Error("Failed to marshal metrics data", slog.Any("error", err))
				continue
			}

			attributes := make(map[string]string)
			for i := 0; i < len(entry.attributes); i += 2 {
				if i+1 < len(entry.attributes) {
					attributes[entry.attributes[i]] = entry.attributes[i+1]
				}
			}

			exemplarData = append(exemplarData, &ExemplarData{
				Attributes:  attributes,
				PartitionId: entry.key,
				Payload:     data,
			})
		}

		if len(exemplarData) == 0 {
			return
		}

		if p.sendMetricsExemplars != nil {
			if err := p.sendMetricsExemplars(context.Background(), organizationID, exemplarData); err != nil {
				p.logger.Error("Failed to send exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// pmetric.Metrics -> JSON string
func (p *Processor) marshalMetrics(md pmetric.Metrics) (string, error) {
	marshaller := &pmetric.JSONMarshaler{}
	bytes, err := marshaller.MarshalMetrics(md)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// ProcessMetrics processes metrics and generates exemplars for a specific organization
func (p *Processor) ProcessMetrics(ctx context.Context, md pmetric.Metrics, organizationID string) error {
	if !p.config.Metrics.Enabled {
		return nil
	}

	tenant := p.getTenant(organizationID)
	if tenant.metricCache == nil {
		return nil
	}

	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		for j := range rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(j)
			for k := range sm.Metrics().Len() {
				m := sm.Metrics().At(k)
				p.addMetricsExemplar(tenant, rm, sm, m, m.Name(), m.Type())
			}
		}
	}
	return nil
}

// add a metrics exemplar to the organization's cache
func (p *Processor) addMetricsExemplar(tenant *Tenant, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricName string, metricType pmetric.MetricType) {
	extraKeys := []string{
		metricNameKey, metricName,
		metricTypeKey, metricType.String(),
	}
	keys, exemplarKey := computeExemplarKey(rm.Resource(), extraKeys)

	if tenant.metricCache.Contains(exemplarKey) {
		return
	}

	exemplarRecord := toMetricExemplar(rm, sm, mm, metricType)
	tenant.metricCache.Put(exemplarKey, keys, exemplarRecord)
}

func (p *Processor) Close() error {
	p.tenants.Range(func(key, value any) bool {
		if tenant, ok := value.(*Tenant); ok {
			if tenant.metricCache != nil {
				// Force flush all pending exemplars before closing
				tenant.metricCache.FlushPending()
				tenant.metricCache.Close()
			}
			// logCache and traceCache will be handled when those telemetry types are implemented
		}
		return true
	})
	return nil
}

// SetMetricsCallback updates the sendMetricsExemplars callback function
func (p *Processor) SetMetricsCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendMetricsExemplars = callback
}

// NewMetricsProcessor creates a new processor specifically for metrics
func NewMetricsProcessor(config Config, logger *slog.Logger) *Processor {
	return NewProcessor(TelemetryTypeMetrics, config, logger)
}
