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

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
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

// computeLogsFingerprint calculates a fingerprint for logs based on message and attributes.
// It uses the fingerprinter with per-organization clustering to properly group similar log messages.
// Returns an error if fingerprinting fails, in which case the item should be dropped.
func computeLogsFingerprint(organizationID string, message, level string, row pipeline.Row, tenantMgr *fingerprint.TenantManager, fp fingerprinter.Fingerprinter) (int64, error) {
	// Get the tenant for this organization
	tenant := tenantMgr.GetTenant(organizationID)

	// Get the cluster manager for this tenant
	clusterManager := tenant.GetTrieClusterManager()

	// Use the fingerprinter to get a fingerprint for the message body
	messageFingerprint, _, err := fp.Fingerprint(message, clusterManager)
	if err != nil {
		// If fingerprinting fails, return error to drop the item
		return 0, fmt.Errorf("failed to fingerprint message: %w", err)
	}

	// Collect and sort all keys for stable hashing (iterate directly over Row)
	keys := make([]string, 0, len(row))
	for key := range row {
		keys = append(keys, wkk.RowKeyValue(key))
	}
	slices.Sort(keys)

	// Hash the attribute keys (not values) to combine with message fingerprint
	h := xxhash.New()
	_, _ = h.WriteString(level + ":")
	for _, key := range keys {
		if key == "" {
			continue
		}
		_, _ = h.WriteString(key + ":")
	}
	attributesHash := int64(h.Sum64())

	// Combine message fingerprint with attributes hash
	// XOR is a simple way to combine two hashes
	return messageFingerprint ^ attributesHash, nil
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
