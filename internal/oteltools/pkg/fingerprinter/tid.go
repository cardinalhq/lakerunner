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
	"hash/fnv"
	"sort"
	"strings"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// Pre-interned keys for TID computation
var (
	tidKeyMetricName           = wkk.NewRowKey("metric_name")
	tidKeyCardinalhqMetricType = wkk.NewRowKey("chq_metric_type")
)

// ComputeTID computes the TID (Time-series ID) using wkk.RowKey keys
// It includes metric_name, chq_metric_type, resource_*, and attr_* fields
func ComputeTID(tags map[wkk.RowKey]any) int64 {
	// Collect key strings directly
	keys := make([]string, 0, len(tags))

	for k, v := range tags {
		// Skip nil and empty values
		if v == nil || v == "" {
			continue
		}

		// Fast path for common keys using equality comparison
		if k == tidKeyMetricName || k == tidKeyCardinalhqMetricType {
			kStr := wkk.RowKeyValue(k)
			keys = append(keys, kStr)
			continue
		}

		// Get string value only for prefix checks
		kStr := wkk.RowKeyValue(k)

		// Check prefixes
		if strings.HasPrefix(kStr, "resource_") || strings.HasPrefix(kStr, "attr_") || strings.HasPrefix(kStr, "metric_") {
			keys = append(keys, kStr)
		}
	}

	sort.Strings(keys)
	h := fnv.New64a()

	// Build hash - need to look up values again
	for _, kStr := range keys {
		k := wkk.NewRowKey(kStr)
		if v, ok := tags[k].(string); ok {
			_, _ = h.Write([]byte(kStr + "=" + v + "|"))
		}
	}

	return int64(h.Sum64())
}
