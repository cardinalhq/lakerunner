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
	"sync"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// TimestampSortKey represents timestamp-only sort key
type TimestampSortKey struct {
	Timestamp int64
	TsOk      bool
}

// Compare implements SortKey interface for TimestampSortKey
func (k *TimestampSortKey) Compare(other SortKey) int {
	o, ok := other.(*TimestampSortKey)
	if !ok {
		panic("TimestampSortKey.Compare: other key is not TimestampSortKey")
	}

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

// Release returns the TimestampSortKey to the pool for reuse
func (k *TimestampSortKey) Release() {
	putTimestampSortKey(k)
}

// timestampSortKeyPool is a pool of reusable TimestampSortKey instances
var timestampSortKeyPool = sync.Pool{
	New: func() any {
		return &TimestampSortKey{}
	},
}

// getTimestampSortKey gets a TimestampSortKey from the pool
func getTimestampSortKey() *TimestampSortKey {
	return timestampSortKeyPool.Get().(*TimestampSortKey)
}

// putTimestampSortKey returns a TimestampSortKey to the pool after resetting it
func putTimestampSortKey(key *TimestampSortKey) {
	*key = TimestampSortKey{}
	timestampSortKeyPool.Put(key)
}

// TimestampSortKeyProvider creates TimestampSortKey instances from rows
type TimestampSortKeyProvider struct{}

// MakeKey implements SortKeyProvider interface for timestamps
func (p *TimestampSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	key := getTimestampSortKey()
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}

// MakeTimestampSortKey creates a pooled TimestampSortKey from a row
func MakeTimestampSortKey(row pipeline.Row) *TimestampSortKey {
	key := getTimestampSortKey()
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}
