// Copyright (C) 2025-2026 CardinalHQ, Inc
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

// LogSortKeyProvider creates LogSortKey instances from rows.
// If StreamField is set, that field is used for the service identifier.
// Otherwise, falls back to resource_customer_domain then resource_service_name.
type LogSortKeyProvider struct {
	StreamField string // Optional: specific field to use for stream identification
}

func NewLogSortKeyProvider(streamField string) *LogSortKeyProvider {
	return &LogSortKeyProvider{
		StreamField: streamField,
	}
}

// MakeKey implements SortKeyProvider interface for logs
func (p *LogSortKeyProvider) MakeKey(row pipeline.Row) SortKey {
	key := getLogSortKey()
	key.ServiceIdentifier, key.ServiceOk = p.getServiceIdentifier(row)
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)
	return key
}

// getServiceIdentifier returns the stream identifier from the row.
// If StreamField is configured, uses that field.
// Otherwise falls back to resource_customer_domain > resource_service_name.
// Returns empty string and true if no field is set (empty string sorts before any non-empty string).
func (p *LogSortKeyProvider) getServiceIdentifier(row pipeline.Row) (string, bool) {
	// If a specific stream field is configured, use it
	if p.StreamField != "" {
		rowKey := wkk.NewRowKey(p.StreamField)
		if val, ok := row[rowKey].(string); ok && val != "" {
			return val, true
		}
		// Field not present or empty, return empty string
		return "", true
	}

	// Default fallback: resource_customer_domain > resource_service_name
	if domain, ok := row[wkk.RowKeyResourceCustomerDomain].(string); ok && domain != "" {
		return domain, true
	}
	if serviceName, ok := row[wkk.RowKeyResourceServiceName].(string); ok && serviceName != "" {
		return serviceName, true
	}
	// Neither is set, use empty string (per user requirement)
	return "", true
}
