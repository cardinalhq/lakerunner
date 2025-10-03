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

package filereader

import (
	"cmp"
	"sync"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// compareOptional compares two optional values (value + ok flag)
// Returns: -1 if k < o, 0 if k == o, 1 if k > o
// nil values sort after non-nil values
func compareOptional[T cmp.Ordered](kVal T, kOk bool, oVal T, oOk bool) int {
	if !kOk || !oOk {
		if !kOk && !oOk {
			return 0
		}
		if !kOk {
			return 1
		}
		return -1
	}
	return cmp.Compare(kVal, oVal)
}

// MetricSortKey represents the sort key for metrics: [name, tid, timestamp]
type MetricSortKey struct {
	Name      string
	Tid       int64
	Timestamp int64
	NameOk    bool
	TidOk     bool
	TsOk      bool
}

// Compare implements SortKey interface for MetricSortKey
func (k *MetricSortKey) Compare(other SortKey) int {
	o, ok := other.(*MetricSortKey)
	if !ok {
		panic("MetricSortKey.Compare: other key is not MetricSortKey")
	}

	if cmp := compareOptional(k.Name, k.NameOk, o.Name, o.NameOk); cmp != 0 {
		return cmp
	}
	if cmp := compareOptional(k.Tid, k.TidOk, o.Tid, o.TidOk); cmp != 0 {
		return cmp
	}
	return compareOptional(k.Timestamp, k.TsOk, o.Timestamp, o.TsOk)
}

// Release returns the MetricSortKey to the pool for reuse
func (k *MetricSortKey) Release() {
	putMetricSortKey(k)
}

// metricSortKeyPool is a pool of reusable MetricSortKey instances
var metricSortKeyPool = sync.Pool{
	New: func() any {
		return &MetricSortKey{}
	},
}

// getMetricSortKey gets a MetricSortKey from the pool
func getMetricSortKey() *MetricSortKey {
	return metricSortKeyPool.Get().(*MetricSortKey)
}

// putMetricSortKey returns a MetricSortKey to the pool after resetting it
func putMetricSortKey(key *MetricSortKey) {
	// Reset all fields to zero values
	*key = MetricSortKey{}
	metricSortKeyPool.Put(key)
}

// MetricSortKeyProvider creates MetricSortKey instances from rows
type MetricSortKeyProvider struct{}

// MakeKey implements SortKeyProvider interface for metrics
func (p *MetricSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	key := getMetricSortKey()
	key.Name, key.NameOk = row[wkk.RowKeyCName].(string)
	key.Tid, key.TidOk = row[wkk.RowKeyCTID].(int64)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}

// MakeMetricSortKey creates a pooled MetricSortKey from a row
func MakeMetricSortKey(row pipeline.Row) *MetricSortKey {
	key := getMetricSortKey()
	key.Name, key.NameOk = row[wkk.RowKeyCName].(string)
	key.Tid, key.TidOk = row[wkk.RowKeyCTID].(int64)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}
