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

// LogSortKey represents the sort key for logs: [service_identifier, timestamp]
// where service_identifier is resource_customer_domain if set, otherwise resource_service_name
type LogSortKey struct {
	ServiceIdentifier string
	Timestamp         int64
	ServiceOk         bool
	TsOk              bool
}

// Compare implements SortKey interface for LogSortKey
func (k *LogSortKey) Compare(other SortKey) int {
	o, ok := other.(*LogSortKey)
	if !ok {
		panic("LogSortKey.Compare: other key is not LogSortKey")
	}

	if cmp := compareOptional(k.ServiceIdentifier, k.ServiceOk, o.ServiceIdentifier, o.ServiceOk); cmp != 0 {
		return cmp
	}
	return compareOptional(k.Timestamp, k.TsOk, o.Timestamp, o.TsOk)
}

// Release returns the LogSortKey to the pool for reuse
func (k *LogSortKey) Release() {
	putLogSortKey(k)
}

// logSortKeyPool is a pool of reusable LogSortKey instances
var logSortKeyPool = sync.Pool{
	New: func() any {
		return &LogSortKey{}
	},
}

// getLogSortKey gets a LogSortKey from the pool
func getLogSortKey() *LogSortKey {
	return logSortKeyPool.Get().(*LogSortKey)
}

// putLogSortKey returns a LogSortKey to the pool after resetting it
func putLogSortKey(key *LogSortKey) {
	*key = LogSortKey{}
	logSortKeyPool.Put(key)
}

// LogSortKeyProvider creates LogSortKey instances from rows
type LogSortKeyProvider struct{}

// MakeKey implements SortKeyProvider interface for logs
func (p *LogSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	key := getLogSortKey()
	key.ServiceIdentifier, key.ServiceOk = getServiceIdentifier(row)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}

// getServiceIdentifier returns resource_customer_domain if set, otherwise resource_service_name.
// Returns empty string and true if neither is set (empty string sorts before any non-empty string).
func getServiceIdentifier(row pipeline.Row) (string, bool) {
	if domain, ok := row[wkk.RowKeyResourceCustomerDomain].(string); ok && domain != "" {
		return domain, true
	}
	if serviceName, ok := row[wkk.RowKeyResourceServiceName].(string); ok && serviceName != "" {
		return serviceName, true
	}
	// Neither is set, use empty string (per user requirement)
	return "", true
}

// MakeLogSortKey creates a pooled LogSortKey from a row
func MakeLogSortKey(row pipeline.Row) *LogSortKey {
	key := getLogSortKey()
	key.ServiceIdentifier, key.ServiceOk = getServiceIdentifier(row)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}
