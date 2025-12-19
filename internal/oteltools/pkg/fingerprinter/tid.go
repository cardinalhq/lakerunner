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

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

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
