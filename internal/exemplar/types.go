package exemplar

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
)

const (
	serviceNameKey   = string(semconv.ServiceNameKey)
	clusterNameKey   = string(semconv.K8SClusterNameKey)
	namespaceNameKey = string(semconv.K8SNamespaceNameKey)
	metricNameKey    = "metric.name"
	metricTypeKey    = "metric.type"
)

// getFromResource extracts a value from resource attributes with a default fallback
func getFromResource(attr pcommon.Map, key string) string {
	val, found := attr.Get(key)
	if !found {
		return "unknown"
	}
	return val.AsString()
}

// computeExemplarKey generates a unique key for an exemplar based on resource attributes and extra keys
func computeExemplarKey(rl pcommon.Resource, extraKeys []string) ([]string, int64) {
	keys := []string{
		clusterNameKey, getFromResource(rl.Attributes(), clusterNameKey),
		namespaceNameKey, getFromResource(rl.Attributes(), namespaceNameKey),
		serviceNameKey, getFromResource(rl.Attributes(), serviceNameKey),
	}
	keys = append(keys, extraKeys...)

	// Simple hash function for the key
	hash := int64(0)
	for _, key := range keys {
		for _, char := range key {
			hash = hash*31 + int64(char)
		}
	}
	return keys, hash
}

// toMetricExemplar creates a copy of the metric data with the first data point as an exemplar
func toMetricExemplar(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, mm pmetric.Metric, metricType pmetric.MetricType) pmetric.Metrics {
	exemplarRecord := pmetric.NewMetrics()
	copyRm := exemplarRecord.ResourceMetrics().AppendEmpty()
	rm.Resource().CopyTo(copyRm.Resource())
	copySm := copyRm.ScopeMetrics().AppendEmpty()
	sm.Scope().CopyTo(copySm.Scope())
	copyMm := copySm.Metrics().AppendEmpty()
	copyMm.SetName(mm.Name())
	copyMm.SetDescription(mm.Description())
	copyMm.SetUnit(mm.Unit())

	switch metricType {
	case pmetric.MetricTypeGauge:
		if mm.Gauge().DataPoints().Len() > 0 {
			newGauge := copyMm.SetEmptyGauge()
			dp := mm.Gauge().DataPoints().At(0)
			ccd := newGauge.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeSum:
		if mm.Sum().DataPoints().Len() > 0 {
			newSum := copyMm.SetEmptySum()
			dp := mm.Sum().DataPoints().At(0)
			ccd := newSum.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeHistogram:
		if mm.Histogram().DataPoints().Len() > 0 {
			newHistogram := copyMm.SetEmptyHistogram()
			dp := mm.Histogram().DataPoints().At(0)
			ccd := newHistogram.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeSummary:
		if mm.Summary().DataPoints().Len() > 0 {
			newSummary := copyMm.SetEmptySummary()
			dp := mm.Summary().DataPoints().At(0)
			ccd := newSummary.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeExponentialHistogram:
		if mm.ExponentialHistogram().DataPoints().Len() > 0 {
			newExponentialHistogram := copyMm.SetEmptyExponentialHistogram()
			dp := mm.ExponentialHistogram().DataPoints().At(0)
			ccd := newExponentialHistogram.DataPoints().AppendEmpty()
			dp.CopyTo(ccd)
		}
	case pmetric.MetricTypeEmpty:
		// do nothing
	default:
		// do nothing
	}
	return exemplarRecord
}

type ExemplarData struct {
	Attributes  map[string]string
	PartitionId int64
	Payload     string
}
