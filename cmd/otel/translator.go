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

package otel

import (
	"encoding/json"
	"maps"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/cardinalhq/oteltools/pkg/fingerprinter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/internal/idgen"

	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/cardinalhq/oteltools/pkg/authenv"
	"github.com/cardinalhq/oteltools/pkg/translate"
)

type TableTranslator struct {
	idg                idgen.IDGenerator
	trieClusterManager *fingerprinter.TrieClusterManager
}

func NewTableTranslator() *TableTranslator {
	return &TableTranslator{
		idg:                idgen.NewULIDGenerator(),
		trieClusterManager: fingerprinter.NewTrieClusterManager(0.5),
	}
}

func SeverityNumberToText(severityNumber plog.SeverityNumber) string {
	switch {
	case severityNumber >= 1 && severityNumber <= 4:
		return "TRACE"
	case severityNumber >= 5 && severityNumber <= 8:
		return "DEBUG"
	case severityNumber >= 9 && severityNumber <= 12:
		return "INFO"
	case severityNumber >= 13 && severityNumber <= 16:
		return "WARN"
	case severityNumber >= 17 && severityNumber <= 20:
		return "ERROR"
	case severityNumber >= 21 && severityNumber <= 24:
		return "FATAL"
	default:
		return "UNSPECIFIED"
	}
}

func (l *TableTranslator) LogsFromOtel(ol *plog.Logs, environment authenv.Environment) ([]map[string]any, error) {
	var rets []map[string]any

	for i := 0; i < ol.ResourceLogs().Len(); i++ {
		rl := ol.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			ill := rl.ScopeLogs().At(j)
			for k := 0; k < ill.LogRecords().Len(); k++ {
				log := ill.LogRecords().At(k)
				ret := map[string]any{translate.CardinalFieldTelemetryType: "logs"}
				addAttributes(ret, rl.Resource().Attributes(), "resource")
				addAttributes(ret, ill.Scope().Attributes(), "scope")
				addAttributes(ret, log.Attributes(), "log")
				stringBody := log.Body().AsString()
				ret[translate.CardinalFieldMessage] = stringBody
				fingerprint, levelFromFingerprinter, _, err := fingerprinter.Fingerprint(stringBody, l.trieClusterManager)
				if err != nil {
					slog.Error("Error fingerprinting log", slog.String("message", stringBody), slog.Any("error", err))
					continue
				}
				if fingerprint != 0 {
					ret[translate.CardinalFieldFingerprint] = fingerprint
				}
				ts := log.Timestamp().AsTime().UnixMilli()
				if ts == 0 {
					ts = log.ObservedTimestamp().AsTime().UnixMilli()
				}
				ret[translate.CardinalFieldTimestamp] = ts
				ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
				if environment != nil {
					for k, v := range environment.Tags() {
						ret["env."+k] = v
					}
				}

				// If severity number is set, use it to set log levelFromFingerprinter
				if log.SeverityNumber() != plog.SeverityNumberUnspecified {
					log.SetSeverityText(SeverityNumberToText(log.SeverityNumber()))
				} else if levelFromFingerprinter != "" {
					log.SetSeverityText(strings.ToUpper(levelFromFingerprinter))
				} else if log.SeverityText() == "" || log.SeverityText() == plog.SeverityNumberUnspecified.String() {
					log.SetSeverityText("INFO")
				}
				ret[translate.CardinalFieldLevel] = log.SeverityText()
				log.Attributes().PutStr(translate.CardinalFieldLevel, log.SeverityText())
				ensureExpectedKeysLogs(ret)
				rets = append(rets, ret)
			}
		}
	}

	return rets, nil
}

func ensureExpectedKeysLogs(m map[string]any) {
	keys := map[string]any{
		translate.CardinalFieldFingerprint: int64(0),
		translate.CardinalFieldMessage:     "",
		translate.CardinalFieldValue:       float64(1),
		translate.CardinalFieldName:        "log.events",
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
}

func addAttributes(m map[string]any, attrs pcommon.Map, prefix string) {
	attrs.Range(func(name string, v pcommon.Value) bool {
		if strings.HasPrefix(name, translate.CardinalFieldPrefixDot) {
			m[name] = handleValue(v.AsRaw())
		} else {
			m[prefix+"."+name] = v.AsString()
		}

		return true
	})
}

func handleValue(v any) any {
	switch v.(type) {
	case string, int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		return v
	default:
		bytes, err := json.Marshal(v)
		if err != nil {
			return "[]"
		}
		return string(bytes)
	}
}

func (l *TableTranslator) MetricsFromOtel(om *pmetric.Metrics, environment authenv.Environment) ([]map[string]any, error) {
	rets := []map[string]any{}

	for i := 0; i < om.ResourceMetrics().Len(); i++ {
		rm := om.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			imm := rm.ScopeMetrics().At(j)
			for k := 0; k < imm.Metrics().Len(); k++ {
				baseret := map[string]any{translate.CardinalFieldTelemetryType: translate.CardinalTelemetryTypeMetrics}
				if environment != nil {
					for k, v := range environment.Tags() {
						baseret["env."+k] = v
					}
				}
				addAttributes(baseret, rm.Resource().Attributes(), "resource")
				addAttributes(baseret, imm.Scope().Attributes(), "scope")
				metric := imm.Metrics().At(k)
				rets = append(rets, l.toddmetric(metric, baseret)...)
			}
		}
	}

	return rets, nil
}

func (l *TableTranslator) toddmetric(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return l.toddGauge(metric, baseattrs)
	case pmetric.MetricTypeSum:
		return l.toddSum(metric, baseattrs)
	case pmetric.MetricTypeHistogram:
		return l.toddHistogram(metric, baseattrs)
	case pmetric.MetricTypeExponentialHistogram:
		return l.toddExponentialHistogram(metric, baseattrs)
	case pmetric.MetricTypeSummary:
		return nil
	case pmetric.MetricTypeEmpty:
		return nil
	}
	return nil
}

func (l *TableTranslator) toddGauge(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	for i := 0; i < metric.Gauge().DataPoints().Len(); i++ {
		dp := metric.Gauge().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret[translate.CardinalFieldMetricType] = translate.CardinalMetricTypeGauge
		ret[translate.CardinalFieldTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			val, safe := safeFloat(dp.DoubleValue())
			if !safe {
				continue
			}
			ret[translate.CardinalFieldValue] = val
		case pmetric.NumberDataPointValueTypeInt:
			ret[translate.CardinalFieldValue] = float64(dp.IntValue())
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		default:
			continue
		}
		ret[translate.CardinalFieldName] = metric.Name()
		ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
		ok := ensureExpectedKeysMetrics(ret)
		if !ok {
			slog.Info("missing critical key", slog.String("metric", metric.Name()))
			continue
		}
		rets = append(rets, ret)
	}

	return rets
}

func (l *TableTranslator) toddSum(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	for i := 0; i < metric.Sum().DataPoints().Len(); i++ {
		dp := metric.Sum().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		if metric.Sum().AggregationTemporality() == pmetric.AggregationTemporalityCumulative && !metric.Sum().IsMonotonic() {
			ret[translate.CardinalFieldMetricType] = translate.CardinalMetricTypeGauge
		} else {
			ret[translate.CardinalFieldMetricType] = translate.CardinalMetricTypeCount
		}
		ret[translate.CardinalFieldMetricType] = translate.CardinalMetricTypeCount
		ret[translate.CardinalFieldTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		switch dp.ValueType() {
		case pmetric.NumberDataPointValueTypeDouble:
			val, safe := safeFloat(dp.DoubleValue())
			if !safe {
				continue
			}
			ret[translate.CardinalFieldValue] = val
		case pmetric.NumberDataPointValueTypeInt:
			ret[translate.CardinalFieldValue] = float64(dp.IntValue())
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		default:
			continue
		}
		ret[translate.CardinalFieldName] = metric.Name()
		ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
		ok := ensureExpectedKeysMetrics(ret)
		if !ok {
			slog.Info("missing critical key", slog.String("metric", metric.Name()))
			continue
		}
		rets = append(rets, ret)
	}

	return rets
}

func safeFloat(v float64) (float64, bool) {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return 0, false
	}
	return v, true
}

func (l *TableTranslator) toddHistogram(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	var rets []map[string]any

	for i := 0; i < metric.Histogram().DataPoints().Len(); i++ {
		dp := metric.Histogram().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret[translate.CardinalFieldMetricType] = translate.CardinalMetricTypeHistogram
		ret[translate.CardinalFieldTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		ret[translate.CardinalFieldCounts] = asJson(dp.BucketCounts().AsRaw())
		ret[translate.CardinalFieldBucketBounds] = asJson(dp.ExplicitBounds().AsRaw())
		ret[translate.CardinalFieldName] = metric.Name()
		ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
		ret[translate.CardinalFieldValue] = float64(-1)
		ok := ensureExpectedKeysMetrics(ret)
		if !ok {
			slog.Info("missing critical key", slog.String("metric", metric.Name()))
			continue
		}
		rets = append(rets, ret)
	}

	return rets
}

func (l *TableTranslator) toddExponentialHistogram(metric pmetric.Metric, baseattrs map[string]any) []map[string]any {
	rets := []map[string]any{}

	for i := 0; i < metric.ExponentialHistogram().DataPoints().Len(); i++ {
		dp := metric.ExponentialHistogram().DataPoints().At(i)
		ret := maps.Clone(baseattrs)
		addAttributes(ret, dp.Attributes(), "metric")
		ret[translate.CardinalFieldMetricType] = translate.CardinalMetricTypeExponentialHistogram
		ret[translate.CardinalFieldTimestamp] = dp.Timestamp().AsTime().UnixMilli()
		ret[translate.CardinalFieldScale] = dp.Scale()
		ret[translate.CardinalFieldNegativeCounts] = asJson(dp.Negative().BucketCounts().AsRaw())
		ret[translate.CardinalFieldPositiveCounts] = asJson(dp.Positive().BucketCounts().AsRaw())
		ret[translate.CardinalFieldZeroCount] = dp.ZeroCount()
		ret[translate.CardinalFieldName] = metric.Name()
		ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
		ret[translate.CardinalFieldValue] = float64(-1)
		ok := ensureExpectedKeysMetrics(ret)
		if !ok {
			slog.Info("missing critical key", slog.String("metric", metric.Name()))
			continue
		}
		rets = append(rets, ret)
	}

	return rets
}

func asJson[T uint64 | float64](s []T) string {
	ret, _ := json.Marshal(s)
	return string(ret)
}

func ensureExpectedKeysMetrics(m map[string]any) bool {
	keys := map[string]any{
		translate.CardinalFieldMetricType:     translate.CardinalMetricTypeGauge,
		translate.CardinalFieldBucketBounds:   "[]",
		translate.CardinalFieldCounts:         "[]",
		translate.CardinalFieldNegativeCounts: "[]",
		translate.CardinalFieldPositiveCounts: "[]",
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
	return true
}

type DDWrapper struct {
	Sketch         *ddsketch.DDSketch
	StartTimestamp time.Time
	Timestamp      time.Time
	Attributes     map[string]any
}

func (l *TableTranslator) TracesFromOtel(ot *ptrace.Traces, environment authenv.Environment) ([]map[string]any, error) {
	var rets []map[string]any

	for i := 0; i < ot.ResourceSpans().Len(); i++ {
		rs := ot.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				ret := map[string]any{translate.CardinalFieldTelemetryType: "traces"}
				addAttributes(ret, rs.Resource().Attributes(), "resource")
				addAttributes(ret, ss.Scope().Attributes(), "scope")
				addAttributes(ret, span.Attributes(), "span")

				// Add span-specific fields
				ret[translate.CardinalFieldSpanTraceID] = span.TraceID().String()
				ret[translate.CardinalFieldSpanSpanID] = span.SpanID().String()
				if span.ParentSpanID().IsEmpty() {
					ret["span.parent_span_id"] = ""
				} else {
					ret["span.parent_span_id"] = span.ParentSpanID().String()
				}
				ret["span.name"] = span.Name()
				ret["span.kind"] = span.Kind().String()
				ret["span.status.code"] = span.Status().Code().String()
				ret["span.status.message"] = span.Status().Message()

				// Add timestamps
				startTS := span.StartTimestamp().AsTime().UnixMilli()
				endTS := span.EndTimestamp().AsTime().UnixMilli()
				ret[translate.CardinalFieldTimestamp] = startTS
				ret["span.start_timestamp"] = startTS
				ret["span.end_timestamp"] = endTS
				ret["span.duration_ms"] = endTS - startTS

				ret[translate.CardinalFieldID] = l.idg.Make(time.Now())
				if environment != nil {
					for k, v := range environment.Tags() {
						ret["env."+k] = v
					}
				}

				ensureExpectedKeysTraces(ret)
				rets = append(rets, ret)
			}
		}
	}

	return rets, nil
}

func ensureExpectedKeysTraces(m map[string]any) {
	keys := map[string]any{
		translate.CardinalFieldFingerprint: int64(0),
		translate.CardinalFieldValue:       float64(1),
		translate.CardinalFieldName:        "span.events",
		"span.trace_id":                    "",
		"span.span_id":                     "",
		"span.parent_span_id":              "",
		"span.name":                        "",
		"span.kind":                        "",
		"span.status.code":                 "",
		"span.status.message":              "",
		"span.start_timestamp":             int64(0),
		"span.end_timestamp":               int64(0),
		"span.duration_ms":                 int64(0),
	}

	for key, val := range keys {
		if _, ok := m[key]; !ok {
			m[key] = val
		}
	}
}
