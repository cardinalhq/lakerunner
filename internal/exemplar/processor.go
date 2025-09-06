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
	"strconv"
	"sync"
	"time"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/internal/logctx"
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
	logCache    *LRUCache[plog.Logs]
	// traceCache will be added when traces are implemented

	// TrieClusterManager for fingerprinting (one per organization)
	trieClusterManager *fingerprinter.TrieClusterManager
}

func (t *Tenant) GetTrieClusterManager() *fingerprinter.TrieClusterManager {
	return t.trieClusterManager
}

// Processor handles exemplar generation from different telemetry types using tenant-based LRU caches
type Processor struct {
	tenants sync.Map // organizationID -> *Tenant

	// Callback for metrics exemplars
	sendMetricsExemplars func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error

	// Callback for logs exemplars
	sendLogsExemplars func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error

	// Configuration for all telemetry types
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
// NewProcessor creates a new unified processor for all telemetry types
func NewProcessor(config Config) *Processor {
	return &Processor{
		tenants: sync.Map{},
		config:  config,
	}
}

func (p *Processor) GetTenant(ctx context.Context, organizationID string) *Tenant {
	ll := logctx.FromContext(ctx)
	if existing, ok := p.tenants.Load(organizationID); ok {
		return existing.(*Tenant)
	}

	ll.Info("Creating new tenant", slog.String("organization_id", organizationID))
	tenant := &Tenant{
		trieClusterManager: fingerprinter.NewTrieClusterManager(0.5),
	}

	// Create caches for enabled telemetry types
	if p.config.Logs.Enabled {
		tenant.logCache = NewLRUCache(
			p.config.Logs.CacheSize,
			p.config.Logs.Expiry,
			p.config.Logs.ReportInterval,
			p.createLogsCallback(ctx, organizationID))
	}

	if p.config.Metrics.Enabled {
		tenant.metricCache = NewLRUCache(
			p.config.Metrics.CacheSize,
			p.config.Metrics.Expiry,
			p.config.Metrics.ReportInterval,
			p.createMetricsCallback(ctx, organizationID))
	}

	if p.config.Traces.Enabled {
		// Traces not implemented yet
		ll.Debug("Traces processing not implemented yet")
	}

	p.tenants.Store(organizationID, tenant)
	return tenant
}

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

		if p.sendLogsExemplars != nil {
			if err := p.sendLogsExemplars(context.Background(), organizationID, exemplarData); err != nil {
				ll.Error("Failed to send logs exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// createMetricsCallback creates a callback function for metrics exemplars for a specific organization
func (p *Processor) createMetricsCallback(ctx context.Context, organizationID string) func([]*Entry[pmetric.Metrics]) {
	return func(entries []*Entry[pmetric.Metrics]) {
		ll := logctx.FromContext(ctx)
		ll.Info("Processing metrics exemplars",
			slog.String("organization_id", organizationID),
			slog.Int("count", len(entries)))

		exemplarData := make([]*ExemplarData, 0, len(entries))
		for _, entry := range entries {
			data, err := p.marshalMetrics(entry.value)
			if err != nil {
				ll.Error("Failed to marshal metrics data", slog.Any("error", err))
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
				ll.Error("Failed to send exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// pmetric.Metrics -> JSON string
// plog.Logs -> JSON string
func (p *Processor) marshalLogs(ld plog.Logs) (string, error) {
	marshaller := &plog.JSONMarshaler{}
	bytes, err := marshaller.MarshalLogs(ld)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
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

// ProcessMetrics processes metrics and generates exemplars for a specific organization
func (p *Processor) ProcessMetrics(ctx context.Context, organizationID string, rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, m pmetric.Metric) error {
	if !p.config.Metrics.Enabled {
		return nil
	}

	tenant := p.GetTenant(ctx, organizationID)
	if tenant.metricCache == nil {
		return nil
	}

	p.addMetricsExemplar(tenant, rm, sm, m, m.Name(), m.Type())
	return nil
}

// add a logs exemplar to the organization's cache
func (p *Processor) addLogExemplar(tenant *Tenant, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) {

	// Get old fingerprint from attributes (if exists from collector)
	fingerprint := getLogFingerprint(lr)
	extraKeys := []string{
		fingerprintKey, strconv.FormatInt(fingerprint, 10),
	}
	keys, exemplarKey := computeExemplarKey(rl.Resource(), extraKeys)

	if tenant.logCache.Contains(exemplarKey) {
		return
	}

	exemplarRecord := toLogExemplar(rl, sl, lr)
	tenant.logCache.Put(exemplarKey, keys, exemplarRecord)
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
	p.tenants.Range(func(key, value interface{}) bool {
		if tenant, ok := value.(*Tenant); ok {
			if tenant.metricCache != nil {
				// Force flush all pending exemplars before closing
				tenant.metricCache.FlushPending()
				tenant.metricCache.Close()
			}
			if tenant.logCache != nil {
				// Force flush all pending exemplars before closing
				tenant.logCache.FlushPending()
				tenant.logCache.Close()
			}
			// traceCache will be handled when traces are implemented
		}
		return true
	})
	return nil
}

// SetMetricsCallback updates the sendMetricsExemplars callback function
func (p *Processor) SetMetricsCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendMetricsExemplars = callback
}

// SetLogsCallback updates the sendLogsExemplars callback function
func (p *Processor) SetLogsCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendLogsExemplars = callback
}
