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
	"github.com/cespare/xxhash/v2"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

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

// computeLogsFingerprint calculates a simple fallback fingerprint for logs.
// This is a basic hash of the message and is NOT using clustering-based fingerprinting.
// Clients should provide the proper fingerprint when possible (via the "fingerprint" field).
func computeLogsFingerprint(row pipeline.Row) int64 {
	// Get the log message
	message := row.GetString(wkk.NewRowKey("message"))
	if message == "" {
		message = row.GetString(wkk.NewRowKey("body"))
	}

	// For a fallback fingerprint, just hash the message
	// Note: This doesn't do clustering like the proper log fingerprinting in internal/logcrunch
	if message == "" {
		return 0
	}

	// Use xxhash for consistency with trace fingerprinting
	return int64(xxhash.Sum64String(message))
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
