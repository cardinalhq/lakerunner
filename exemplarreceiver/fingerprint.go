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

package exemplarreceiver

import (
	"fmt"
	"slices"

	"github.com/cespare/xxhash/v2"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// valueToString converts any value to its string representation for hashing
func valueToString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// computeTracesFingerprint calculates a fingerprint for traces using the Row.
// Uses the standard fingerprinter.CalculateSpanFingerprintFromRow function.
func computeTracesFingerprint(row pipeline.Row) int64 {
	// Handle span_kind conversion if it's numeric
	if spanKindNum, ok := row.GetInt32(wkk.NewRowKey("span_kind")); ok {
		spanKindStr := spanKindToString(spanKindNum)
		row[wkk.NewRowKey("span_kind")] = spanKindStr
	}

	// Use the existing fingerprinting function directly
	return fingerprinter.CalculateSpanFingerprintFromRow(row)
}

// computeLogsFingerprint calculates a fingerprint for logs based on message and attributes.
// TODO: Use the same log fingerprinting calculation as internal/oteltools/pkg/fingerprinter
// which does proper tokenization-based clustering. The current implementation is a simple
// hash of all key-value pairs and doesn't cluster similar log messages properly.
func computeLogsFingerprint(message, level string, row pipeline.Row) int64 {
	// Convert to string map for easier processing
	stringMap := pipeline.ToStringMap(row)

	// Collect and sort all keys for stable hashing
	keys := make([]string, 0, len(stringMap))
	for key := range stringMap {
		keys = append(keys, key)
	}
	slices.Sort(keys)

	h := xxhash.New()
	_, _ = h.WriteString(message + ":" + level + ":")
	for _, key := range keys {
		value := valueToString(stringMap[key])
		if value == "" {
			continue
		}
		_, _ = h.WriteString(key + ":")
	}

	return int64(h.Sum64())
}

// spanKindToString converts a span kind integer to its string representation
func spanKindToString(kind int32) string {
	switch kind {
	case 0:
		return "SPAN_KIND_UNSPECIFIED"
	case 1:
		return "SPAN_KIND_INTERNAL"
	case 2:
		return "SPAN_KIND_SERVER"
	case 3:
		return "SPAN_KIND_CLIENT"
	case 4:
		return "SPAN_KIND_PRODUCER"
	case 5:
		return "SPAN_KIND_CONSUMER"
	default:
		return "SPAN_KIND_UNSPECIFIED"
	}
}
