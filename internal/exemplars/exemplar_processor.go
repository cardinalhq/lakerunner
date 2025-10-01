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
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TelemetryType represents the type of telemetry being processed
type TelemetryType string

// Tenant holds the caches for each telemetry type for a specific organization
type Tenant struct {
	metricCache *LRUCache[map[wkk.RowKey]any]
	logCache    *LRUCache[plog.Logs]
	traceCache  *LRUCache[ptrace.Traces]

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

	// Callback for traces exemplars
	sendTracesExemplars func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error

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
			Enabled:        true,
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
			Enabled:        true,
			CacheSize:      1000,
			Expiry:         5 * time.Minute,
			ReportInterval: 1 * time.Minute,
			BatchSize:      100,
		},
	}
}

// NewProcessor creates a new unified processor for all telemetry types
func NewProcessor(config Config) *Processor {
	return &Processor{
		tenants: sync.Map{},
		config:  config,
	}
}

// GetTenant retrieves or creates a tenant for the given organization ID
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
		tenant.traceCache = NewLRUCache(
			p.config.Traces.CacheSize,
			p.config.Traces.Expiry,
			p.config.Traces.ReportInterval,
			p.createTracesCallback(ctx, organizationID))
	}

	p.tenants.Store(organizationID, tenant)
	return tenant
}

// toAttributes extracts standard attributes from OTEL resource attributes
func (p *Processor) toAttributes(resourceAttributes pcommon.Map) map[string]string {
	attributes := make(map[string]string)
	attributes[serviceNameKey] = getFromResource(resourceAttributes, serviceNameKey)
	attributes[namespaceNameKey] = getFromResource(resourceAttributes, namespaceNameKey)
	attributes[clusterNameKey] = getFromResource(resourceAttributes, clusterNameKey)
	return attributes
}

// toAttributesFromRow extracts attributes from a Row
func (p *Processor) toAttributesFromRow(row map[wkk.RowKey]any) map[string]string {
	attributes := make(map[string]string)
	attributes[serviceNameKey] = p.getStringFromRow(row, wkk.NewRowKey("resource_service_name"))
	attributes[namespaceNameKey] = p.getStringFromRow(row, wkk.NewRowKey("resource_k8s_namespace_name"))
	attributes[clusterNameKey] = p.getStringFromRow(row, wkk.NewRowKey("resource_k8s_cluster_name"))
	return attributes
}

// getStringFromRow safely extracts a string value from a Row
func (p *Processor) getStringFromRow(row map[wkk.RowKey]any, key wkk.RowKey) string {
	if val, ok := row[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return "unknown"
}

// Close flushes all pending exemplars and closes all caches
func (p *Processor) Close() error {
	p.tenants.Range(func(key, value interface{}) bool {
		if tenant, ok := value.(*Tenant); ok {
			if tenant.metricCache != nil {
				tenant.metricCache.FlushPending()
				tenant.metricCache.Close()
			}
			if tenant.logCache != nil {
				tenant.logCache.FlushPending()
				tenant.logCache.Close()
			}
			if tenant.traceCache != nil {
				tenant.traceCache.FlushPending()
				tenant.traceCache.Close()
			}
		}
		return true
	})
	return nil
}
