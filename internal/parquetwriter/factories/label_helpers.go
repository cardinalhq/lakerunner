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

package factories

import (
	"encoding/json"
	"log/slog"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
)

// isLabelColumn determines if a column should be included in the label name mapping.
// This includes user-facing labels (resource.*, log.*, metric.*, span.*, etc.),
// internal fields (_cardinalhq.*), and convenience columns (chq_, scope_).
// Note: chq_ and scope_ columns do not have dotted equivalents and are stored with empty string values in the label name map.
func isLabelColumn(columnName string) bool {
	labelPrefixes := []string{
		"_cardinalhq_",
		"chq_",
		"resource_",
		"scope_",
		"log_",
		"metric_",
		"span_",
		"trace_",
		"attr_",
	}

	for _, prefix := range labelPrefixes {
		if strings.HasPrefix(columnName, prefix) {
			return true
		}
	}
	return false
}

// underscoreToDotted converts an underscored name to a dotted name where applicable.
// Returns the original name if no dotted equivalent exists (e.g., for chq_ and scope_ fields).
func underscoreToDotted(underscored string) string {
	// Handle attr_ prefix: strip it first, then convert the rest
	// For legacy API compatibility: attr_log_level → log.level, attr_log_source → log.source
	if strings.HasPrefix(underscored, "attr_") {
		rest := underscored[len("attr_"):]
		return underscoreToDotted(rest) // Recursively handle the rest
	}

	// Known prefixes that should be converted to dotted format
	convertiblePrefixes := []string{"_cardinalhq_", "resource_", "log_", "metric_", "span_", "trace_"}

	for _, prefix := range convertiblePrefixes {
		if strings.HasPrefix(underscored, prefix) {
			return strings.ReplaceAll(underscored, "_", ".")
		}
	}

	// Fields with chq_ and scope_ prefixes don't have dotted equivalents
	// Return as-is - these will be stored with empty string values in label_name_map
	return underscored
}

// buildLabelNameMap creates a JSON mapping of label columns to their dotted equivalents.
// This map serves dual purposes:
// 1. For v2 APIs (LogQL/PromQL): All keys are used for tag queries
// 2. For v1 legacy APIs: Only entries with non-empty values (dotted mappings) are used
func buildLabelNameMap(columns mapset.Set[string]) []byte {
	cardinality := columns.Cardinality()
	if cardinality == 0 {
		slog.Warn("buildLabelNameMap called with zero label columns - label_name_map will be NULL")
		return nil
	}

	mapping := make(map[string]string)
	for columnName := range columns.Iter() {
		dottedName := underscoreToDotted(columnName)
		// Store all label columns:
		// - If a dotted mapping exists and is different, store the mapping
		// - If no dotted mapping exists (e.g., chq_ fields), store with empty string
		if dottedName != columnName {
			mapping[columnName] = dottedName
		} else {
			mapping[columnName] = ""
		}
	}

	if len(mapping) == 0 {
		slog.Error("buildLabelNameMap: no mappings created despite having columns",
			slog.Int("cardinality", cardinality))
		return nil
	}

	jsonBytes, err := json.Marshal(mapping)
	if err != nil {
		slog.Error("buildLabelNameMap: failed to marshal mapping", slog.Any("error", err))
		return nil
	}

	slog.Debug("buildLabelNameMap created mapping",
		slog.Int("columnCount", cardinality),
		slog.Int("mappingCount", len(mapping)))

	return jsonBytes
}
