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

package queryapi

import (
	"strings"
)

// LabelDenormalizer converts underscored label names back to dotted names.
type LabelDenormalizer struct {
	segmentMaps map[int64]map[string]string
	fallback    map[string]string // Known common labels
}

// NewLabelDenormalizer creates a new label denormalizer.
func NewLabelDenormalizer(segmentMaps map[int64]map[string]string) *LabelDenormalizer {
	return &LabelDenormalizer{
		segmentMaps: segmentMaps,
		fallback:    getCommonLabels(),
	}
}

// Denormalize converts a single underscored label name to dotted format.
func (ld *LabelDenormalizer) Denormalize(segmentID int64, underscored string) string {
	// Try segment-specific map first
	if segMap, ok := ld.segmentMaps[segmentID]; ok {
		if dotted, ok := segMap[underscored]; ok {
			return dotted
		}
	}

	// Try fallback for common labels
	if dotted, ok := ld.fallback[underscored]; ok {
		return dotted
	}

	// Last resort: heuristic conversion
	return heuristicDenormalize(underscored)
}

// DenormalizeMap converts all underscored label names in a map to dotted format.
func (ld *LabelDenormalizer) DenormalizeMap(segmentID int64, tags map[string]any) map[string]any {
	result := make(map[string]any, len(tags))
	for k, v := range tags {
		dottedKey := ld.Denormalize(segmentID, k)
		result[dottedKey] = v
	}
	return result
}

// heuristicDenormalize applies heuristic rules to convert underscored to dotted.
func heuristicDenormalize(underscored string) string {
	// Handle special prefixes that need to preserve leading characters
	if strings.HasPrefix(underscored, "_cardinalhq_") {
		// Keep the leading underscore, replace only internal underscores
		rest := underscored[len("_cardinalhq_"):]
		return "_cardinalhq." + strings.ReplaceAll(rest, "_", ".")
	}

	// Handle attr_ prefix: strip it and denormalize the rest
	// For legacy API compatibility: attr_log_level → log.level, attr_log_source → log.source
	if strings.HasPrefix(underscored, "attr_") {
		rest := underscored[len("attr_"):]
		// Recursively denormalize the rest
		return heuristicDenormalize(rest)
	}

	// Known prefixes that should have dots
	prefixes := []string{"resource_", "log_", "metric_", "span_", "trace_", "chq_"}

	for _, prefix := range prefixes {
		if strings.HasPrefix(underscored, prefix) {
			return strings.ReplaceAll(underscored, "_", ".")
		}
	}

	// Unknown pattern: keep as-is (safer than guessing)
	return underscored
}

// getCommonLabels returns a hardcoded map of known common labels.
func getCommonLabels() map[string]string {
	return map[string]string{
		// CardinalHQ internal labels
		"_cardinalhq_fingerprint": "_cardinalhq.fingerprint",
		"_cardinalhq_timestamp":   "_cardinalhq.timestamp",
		"_cardinalhq_message":     "_cardinalhq.message",
		"_cardinalhq_level":       "_cardinalhq.level",
		"_cardinalhq_name":        "_cardinalhq.name",
		"_cardinalhq_trace_id":    "_cardinalhq.trace_id",
		"_cardinalhq_span_id":     "_cardinalhq.span_id",

		// Common resource labels
		"resource_service_name":      "resource.service.name",
		"resource_service_namespace": "resource.service.namespace",
		"resource_service_version":   "resource.service.version",
		"resource_service_instance":  "resource.service.instance",
		"resource_bucket_name":       "resource.bucket.name",
		"resource_file":              "resource.file",
		"resource_file_name":         "resource.file.name",
		"resource_file_type":         "resource.file.type",
		"resource_protocol":          "resource.protocol",
		"resource_host_name":         "resource.host.name",
		"resource_host_id":           "resource.host.id",
		"resource_cluster_name":      "resource.cluster.name",
		"resource_namespace_name":    "resource.namespace.name",
		"resource_pod_name":          "resource.pod.name",
		"resource_container_name":    "resource.container.name",

		// Common log labels
		"log_level":     "log.level",
		"log_timestamp": "log.timestamp",
		"log_body":      "log.body",
		"log_severity":  "log.severity",

		// Kubernetes labels
		"resource_k8s_cluster_name":   "resource.k8s.cluster.name",
		"resource_k8s_namespace_name": "resource.k8s.namespace.name",
		"resource_k8s_pod_name":       "resource.k8s.pod.name",
		"resource_k8s_container_name": "resource.k8s.container.name",
	}
}
