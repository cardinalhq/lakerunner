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

package fingerprinter

import (
	"bytes"
	"hash"
	"hash/fnv"
	"slices"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// metricTypeToString converts OTEL metric type to our canonical string representation.
func metricTypeToString(metricType pmetric.MetricType) string {
	switch metricType {
	case pmetric.MetricTypeGauge:
		return "gauge"
	case pmetric.MetricTypeSum:
		return "count"
	case pmetric.MetricTypeHistogram, pmetric.MetricTypeExponentialHistogram, pmetric.MetricTypeSummary:
		return "histogram"
	default:
		return "gauge"
	}
}

// Pre-interned keys for TID computation
var (
	tidKeyMetricName           = wkk.NewRowKey("metric_name")
	tidKeyCardinalhqMetricType = wkk.NewRowKey("chq_metric_type")
)

// tidState holds reusable state for TID computation
type tidState struct {
	hasher hash.Hash64
	keys   []wkk.RowKey
	buf    bytes.Buffer
}

var tidStatePool = sync.Pool{
	New: func() any {
		return &tidState{
			hasher: fnv.New64a(),
			keys:   make([]wkk.RowKey, 0, 32),
		}
	},
}

// DebugTIDKeys returns the sorted key-value pairs that would be used for TID computation.
// This is for debugging only.
func DebugTIDKeys(tags map[wkk.RowKey]any) []string {
	var kvs []string
	for k, v := range tags {
		if v == nil || v == "" {
			continue
		}
		if k == tidKeyMetricName || k == tidKeyCardinalhqMetricType {
			if s, ok := v.(string); ok {
				kvs = append(kvs, wkk.RowKeyValue(k)+"="+s)
			}
			continue
		}
		kStr := wkk.RowKeyValue(k)
		if strings.HasPrefix(kStr, "resource_") || strings.HasPrefix(kStr, "attr_") || strings.HasPrefix(kStr, "metric_") {
			if s, ok := v.(string); ok {
				kvs = append(kvs, kStr+"="+s)
			}
		}
	}
	slices.Sort(kvs)
	return kvs
}

// DebugTIDKeysFromOTEL returns the sorted key-value pairs that ComputeTIDFromOTEL would use.
// This is for debugging only.
func DebugTIDKeysFromOTEL(
	resourceAttrs pcommon.Map,
	dpAttrs pcommon.Map,
	metricName string,
	metricType pmetric.MetricType,
) []string {
	var kvs []string

	resourceAttrs.Range(func(name string, v pcommon.Value) bool {
		normalizedName := wkk.NormalizeName(name)
		if KeepResourceKeys[normalizedName] {
			kvs = append(kvs, "resource_"+normalizedName+"="+v.AsString())
		}
		return true
	})

	dpAttrs.Range(func(name string, v pcommon.Value) bool {
		// Skip underscore-prefixed attributes (internal/special attributes)
		if len(name) > 0 && name[0] == '_' {
			return true
		}
		val := v.AsString()
		if val != "" {
			normalizedName := wkk.NormalizeName(name)
			kvs = append(kvs, "attr_"+normalizedName+"="+val)
		}
		return true
	})

	normalizedMetricName := wkk.NormalizeName(metricName)
	if normalizedMetricName != "" {
		kvs = append(kvs, "metric_name="+normalizedMetricName)
	}

	kvs = append(kvs, "chq_metric_type="+metricTypeToString(metricType))

	slices.Sort(kvs)
	return kvs
}

// ComputeTID computes the TID (Time-series ID) using wkk.RowKey keys
// It includes metric_name, chq_metric_type, resource_*, and attr_* fields
func ComputeTID(tags map[wkk.RowKey]any) int64 {
	state := tidStatePool.Get().(*tidState)
	state.hasher.Reset()
	state.keys = state.keys[:0]
	state.buf.Reset()

	for k, v := range tags {
		// Skip nil and empty values
		if v == nil || v == "" {
			continue
		}

		// Fast path for common keys using equality comparison
		if k == tidKeyMetricName || k == tidKeyCardinalhqMetricType {
			state.keys = append(state.keys, k)
			continue
		}

		// Get string value only for prefix checks
		kStr := wkk.RowKeyValue(k)

		// Check prefixes
		if strings.HasPrefix(kStr, "resource_") || strings.HasPrefix(kStr, "attr_") || strings.HasPrefix(kStr, "metric_") {
			state.keys = append(state.keys, k)
		}
	}

	// Sort by string value for deterministic hashing
	slices.SortFunc(state.keys, func(a, b wkk.RowKey) int {
		return strings.Compare(wkk.RowKeyValue(a), wkk.RowKeyValue(b))
	})

	// Build hash input in buffer to avoid per-write allocations
	for _, k := range state.keys {
		if v, ok := tags[k].(string); ok {
			kStr := wkk.RowKeyValue(k)
			state.buf.WriteString(kStr)
			state.buf.WriteByte('=')
			state.buf.WriteString(v)
			state.buf.WriteByte('|')
		}
	}

	state.hasher.Write(state.buf.Bytes())
	result := int64(state.hasher.Sum64())
	tidStatePool.Put(state)
	return result
}

// KeepResourceKeys defines which resource attribute keys are included in TID computation.
// Keys are stored WITHOUT the "resource_" prefix. The prefix is added when building rows.
var KeepResourceKeys = map[string]bool{
	"app":                  true,
	"container_image_name": true,
	"container_image_tag":  true,
	"k8s_cluster_name":     true,
	"k8s_daemonset_name":   true,
	"k8s_deployment_name":  true,
	"k8s_namespace_name":   true,
	"k8s_pod_ip":           true,
	"k8s_pod_name":         true,
	"k8s_statefulset_name": true,
	"service_name":         true,
	"service_version":      true,
}

// tidKV holds a key-value pair for TID computation
type tidKV struct {
	key   string
	value string
}

// tidKVSlice wraps a slice for pooling (avoids allocation on Put)
type tidKVSlice struct {
	kvs []tidKV
}

// tidKVPool is a pool of reusable tidKV slices
var tidKVPool = sync.Pool{
	New: func() any {
		return &tidKVSlice{kvs: make([]tidKV, 0, 32)}
	},
}

// ComputeTIDFromOTEL computes the TID directly from OTEL metric structures.
// This produces the same result as ComputeTID but without materializing a full Row.
func ComputeTIDFromOTEL(
	resourceAttrs pcommon.Map,
	dpAttrs pcommon.Map,
	metricName string,
	metricType pmetric.MetricType,
) int64 {
	state := tidStatePool.Get().(*tidState)
	state.hasher.Reset()
	state.buf.Reset()

	kvsWrapper := tidKVPool.Get().(*tidKVSlice)
	kvsWrapper.kvs = kvsWrapper.kvs[:0]

	// Add filtered resource attributes (prefixed with "resource_")
	// Uses wkk.NormalizeName to convert OTEL dots to underscores.
	resourceAttrs.Range(func(name string, v pcommon.Value) bool {
		normalizedName := wkk.NormalizeName(name)
		if KeepResourceKeys[normalizedName] {
			kvsWrapper.kvs = append(kvsWrapper.kvs, tidKV{
				key:   "resource_" + normalizedName,
				value: v.AsString(),
			})
		}
		return true
	})

	// Add datapoint attributes (prefixed with "attr_")
	// Uses wkk.NormalizeName to convert OTEL dots to underscores.
	// Underscore-prefixed attributes are SKIPPED - they don't get the "attr_" prefix
	// in row building, and ComputeTID only looks for "attr_" prefixed keys.
	dpAttrs.Range(func(name string, v pcommon.Value) bool {
		// Skip underscore-prefixed attributes (internal/special attributes)
		if len(name) > 0 && name[0] == '_' {
			return true
		}
		val := v.AsString()
		if val != "" {
			normalizedName := wkk.NormalizeName(name)
			kvsWrapper.kvs = append(kvsWrapper.kvs, tidKV{
				key:   "attr_" + normalizedName,
				value: val,
			})
		}
		return true
	})

	// Add metric name (normalized via wkk.NormalizeName)
	normalizedMetricName := wkk.NormalizeName(metricName)
	if normalizedMetricName != "" {
		kvsWrapper.kvs = append(kvsWrapper.kvs, tidKV{key: "metric_name", value: normalizedMetricName})
	}

	// Add metric type
	kvsWrapper.kvs = append(kvsWrapper.kvs, tidKV{key: "chq_metric_type", value: metricTypeToString(metricType)})

	// Sort by key for deterministic hashing
	slices.SortFunc(kvsWrapper.kvs, func(a, b tidKV) int {
		return strings.Compare(a.key, b.key)
	})

	// Build hash input
	for _, kv := range kvsWrapper.kvs {
		state.buf.WriteString(kv.key)
		state.buf.WriteByte('=')
		state.buf.WriteString(kv.value)
		state.buf.WriteByte('|')
	}

	state.hasher.Write(state.buf.Bytes())
	result := int64(state.hasher.Sum64())

	tidKVPool.Put(kvsWrapper)
	tidStatePool.Put(state)
	return result
}

// GetDatapointAttributes extracts the attributes map from any datapoint type.
func GetDatapointAttributes(metric pmetric.Metric, dpIndex int) pcommon.Map {
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return metric.Gauge().DataPoints().At(dpIndex).Attributes()
	case pmetric.MetricTypeSum:
		return metric.Sum().DataPoints().At(dpIndex).Attributes()
	case pmetric.MetricTypeHistogram:
		return metric.Histogram().DataPoints().At(dpIndex).Attributes()
	case pmetric.MetricTypeExponentialHistogram:
		return metric.ExponentialHistogram().DataPoints().At(dpIndex).Attributes()
	case pmetric.MetricTypeSummary:
		return metric.Summary().DataPoints().At(dpIndex).Attributes()
	default:
		return pcommon.NewMap()
	}
}
