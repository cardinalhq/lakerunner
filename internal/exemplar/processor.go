package exemplar

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

// Tenant holds the caches for each telemetry type for a specific organization
type Tenant struct {
	metricCache *LRUCache[pmetric.Metrics]
}

// Processor handles exemplar generation from metrics using tenant-based LRU caches
type Processor struct {
	tenants        sync.Map // organizationID -> *Tenant
	logger         *slog.Logger
	mu             sync.RWMutex
	cacheSize      int
	expiry         time.Duration
	reportInterval time.Duration
	// Function to send exemplars to the database (important: this will be set by caller)
	sendExemplars func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error
	metricsMarshaller pmetric.Marshaler
}

func NewProcessor(cacheSize int, expiry, reportInterval time.Duration, logger *slog.Logger, sendExemplars func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) *Processor {
	return &Processor{
		tenants:           sync.Map{},
		logger:            logger,
		cacheSize:         cacheSize,
		expiry:            expiry,
		reportInterval:    reportInterval,
		sendExemplars:     sendExemplars,
		metricsMarshaller: &pmetric.JSONMarshaler{},
	}
}

func (p *Processor) getTenant(organizationID string) *Tenant {
	if existing, ok := p.tenants.Load(organizationID); ok {
		return existing.(*Tenant)
	}

	p.logger.Info("Creating new tenant", slog.String("organization_id", organizationID))
	tenant := &Tenant{}

	tenant.metricCache = NewLRUCache(
		p.cacheSize,
		p.expiry,
		p.reportInterval,
		p.createMetricsCallback(organizationID))

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

		if p.sendExemplars != nil {
			if err := p.sendExemplars(context.Background(), organizationID, exemplarData); err != nil {
				p.logger.Error("Failed to send exemplars to database", slog.Any("error", err))
			}
		}
	}
}

// pmetric.Metrics -> JSON string
func (p *Processor) marshalMetrics(md pmetric.Metrics) (string, error) {
	bytes, err := p.metricsMarshaller.MarshalMetrics(md)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

// generate exemplars for a specific organization
func (p *Processor) ProcessMetrics(ctx context.Context, md pmetric.Metrics, organizationID string) error {
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
	p.tenants.Range(func(key, value interface{}) bool {
		if tenant, ok := value.(*Tenant); ok {
			if tenant.metricCache != nil {
				// Force flush all pending exemplars before closing
				tenant.metricCache.FlushPending()
				tenant.metricCache.Close()
			}
		}
		return true
	})
	return nil
}

// SetCallback updates the sendExemplars callback function
func (p *Processor) SetCallback(callback func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error) {
	p.sendExemplars = callback
}
