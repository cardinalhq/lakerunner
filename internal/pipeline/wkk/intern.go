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

package wkk

import "unique"

type rowkey string

type RowKey = unique.Handle[rowkey]

func NewRowKey(s string) RowKey {
	return unique.Make(rowkey(s))
}

var (
	// _cardinalhq.* keys (sorted alphabetically)
	RowKeyCCollectorID   = NewRowKey("_cardinalhq.collector_id")
	RowKeyCCustomerID    = NewRowKey("_cardinalhq.customer_id")
	RowKeyCFingerprint   = NewRowKey("_cardinalhq.fingerprint")
	RowKeyCID            = NewRowKey("_cardinalhq.id")
	RowKeyCMessage       = NewRowKey("_cardinalhq.message")
	RowKeyCMetricType    = NewRowKey("_cardinalhq.metric_type")
	RowKeyCName          = NewRowKey("_cardinalhq.name")
	RowKeyCSpanEvents    = NewRowKey("_cardinalhq.span_events")
	RowKeyCTelemetryType = NewRowKey("_cardinalhq.telemetry_type")
	RowKeyCTID           = NewRowKey("_cardinalhq.tid")
	RowKeyCTimestamp     = NewRowKey("_cardinalhq.timestamp")
	RowKeyCValue         = NewRowKey("_cardinalhq.value") // Deprecated

	// histogram handling (TODO: just make histograms)
	RowKeyCBucketBounds   = NewRowKey("_cardinalhq.bucket_bounds")
	RowKeyCNegativeCounts = NewRowKey("_cardinalhq.negative_counts")
	RowKeyCPositiveCounts = NewRowKey("_cardinalhq.positive_counts")
	RowKeyCCounts         = NewRowKey("_cardinalhq.counts")    // Deprecated
	RowKeyCSumValue       = NewRowKey("_cardinalhq.sum_value") // Deprecated

	// rollup_* keys (sorted alphabetically)
	RowKeyRollupAvg   = NewRowKey("rollup_avg")
	RowKeyRollupCount = NewRowKey("rollup_count")
	RowKeyRollupMax   = NewRowKey("rollup_max")
	RowKeyRollupMin   = NewRowKey("rollup_min")
	RowKeyRollupP25   = NewRowKey("rollup_p25")
	RowKeyRollupP50   = NewRowKey("rollup_p50")
	RowKeyRollupP75   = NewRowKey("rollup_p75")
	RowKeyRollupP90   = NewRowKey("rollup_p90")
	RowKeyRollupP95   = NewRowKey("rollup_p95")
	RowKeyRollupP99   = NewRowKey("rollup_p99")
	RowKeyRollupSum   = NewRowKey("rollup_sum")

	// sketch key
	RowKeySketch = NewRowKey("sketch")
)
