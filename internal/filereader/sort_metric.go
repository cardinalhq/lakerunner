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
	"strings"
	"sync"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

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

	// Compare name field
	if !k.NameOk || !o.NameOk {
		if !k.NameOk && !o.NameOk {
			return 0
		}
		if !k.NameOk {
			return 1
		}
		return -1
	}
	if cmp := strings.Compare(k.Name, o.Name); cmp != 0 {
		return cmp
	}

	// Compare TID field
	if !k.TidOk || !o.TidOk {
		if !k.TidOk && !o.TidOk {
			return 0
		}
		if !k.TidOk {
			return 1
		}
		return -1
	}
	if k.Tid < o.Tid {
		return -1
	}
	if k.Tid > o.Tid {
		return 1
	}

	// Compare timestamp field
	if !k.TsOk || !o.TsOk {
		if !k.TsOk && !o.TsOk {
			return 0
		}
		if !k.TsOk {
			return 1
		}
		return -1
	}
	if k.Timestamp < o.Timestamp {
		return -1
	}
	if k.Timestamp > o.Timestamp {
		return 1
	}

	return 0
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
func (p *MetricSortKeyProvider) MakeKey(row Row) SortKey {
	key := getMetricSortKey()
	key.Name, key.NameOk = row[wkk.RowKeyCName].(string)
	key.Tid, key.TidOk = row[wkk.RowKeyCTID].(int64)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}

// MakeMetricSortKey creates a pooled MetricSortKey from a row
func MakeMetricSortKey(row Row) *MetricSortKey {
	key := getMetricSortKey()
	key.Name, key.NameOk = row[wkk.RowKeyCName].(string)
	key.Tid, key.TidOk = row[wkk.RowKeyCTID].(int64)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}
