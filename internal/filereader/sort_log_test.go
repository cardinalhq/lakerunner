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
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestLogSortKey_Compare(t *testing.T) {
	tests := []struct {
		name     string
		key1     LogSortKey
		key2     LogSortKey
		expected int
	}{
		{
			name:     "same keys",
			key1:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 1000, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 1000, TsOk: true},
			expected: 0,
		},
		{
			name:     "service identifier less than (primary sort)",
			key1:     LogSortKey{ServiceIdentifier: "aaa", ServiceOk: true, Timestamp: 1000, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "zzz", ServiceOk: true, Timestamp: 1000, TsOk: true},
			expected: -1,
		},
		{
			name:     "service identifier greater than (primary sort)",
			key1:     LogSortKey{ServiceIdentifier: "zzz", ServiceOk: true, Timestamp: 1000, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "aaa", ServiceOk: true, Timestamp: 1000, TsOk: true},
			expected: 1,
		},
		{
			name:     "timestamp less than (secondary sort)",
			key1:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 500, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 1000, TsOk: true},
			expected: -1,
		},
		{
			name:     "timestamp greater than (secondary sort)",
			key1:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 1000, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 500, TsOk: true},
			expected: 1,
		},
		{
			name:     "missing service identifier sorts after",
			key1:     LogSortKey{ServiceIdentifier: "", ServiceOk: false, Timestamp: 1000, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 1000, TsOk: true},
			expected: 1,
		},
		{
			name:     "both missing service - compare by timestamp",
			key1:     LogSortKey{ServiceIdentifier: "", ServiceOk: false, Timestamp: 500, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "", ServiceOk: false, Timestamp: 1000, TsOk: true},
			expected: -1,
		},
		{
			name:     "empty service identifier is valid (default case)",
			key1:     LogSortKey{ServiceIdentifier: "", ServiceOk: true, Timestamp: 1000, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "svc1", ServiceOk: true, Timestamp: 1000, TsOk: true},
			expected: -1, // empty string sorts before non-empty
		},
		{
			name:     "service identifier takes priority over timestamp",
			key1:     LogSortKey{ServiceIdentifier: "aaa", ServiceOk: true, Timestamp: 9999, TsOk: true},
			key2:     LogSortKey{ServiceIdentifier: "zzz", ServiceOk: true, Timestamp: 1, TsOk: true},
			expected: -1, // aaa < zzz, even though 9999 > 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.key1.Compare(&tt.key2)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLogSortKey_PoolingWorks(t *testing.T) {
	// Test that pooling works correctly
	key1 := getLogSortKey()
	key1.ServiceIdentifier = "test-service"
	key1.ServiceOk = true
	key1.Timestamp = 1000
	key1.TsOk = true

	// Release and get again
	key1.Release()

	key2 := getLogSortKey()
	// After release and get, the key should be reset
	assert.Equal(t, "", key2.ServiceIdentifier)
	assert.False(t, key2.ServiceOk)
	assert.Equal(t, int64(0), key2.Timestamp)
	assert.False(t, key2.TsOk)

	putLogSortKey(key2)
}

func TestLogSortKeyProvider_MakeKey(t *testing.T) {
	tests := []struct {
		name                      string
		row                       pipeline.Row
		expectedServiceIdentifier string
		expectedServiceOk         bool
		expectedTimestamp         int64
		expectedTsOk              bool
	}{
		{
			name: "all fields present with customer_domain",
			row: pipeline.Row{
				wkk.RowKeyResourceCustomerDomain: "customer.example.com",
				wkk.RowKeyResourceServiceName:    "backend-service",
				wkk.RowKeyCTimestamp:             int64(1000),
			},
			expectedServiceIdentifier: "customer.example.com", // customer_domain takes priority
			expectedServiceOk:         true,
			expectedTimestamp:         1000,
			expectedTsOk:              true,
		},
		{
			name: "customer_domain empty, uses service_name",
			row: pipeline.Row{
				wkk.RowKeyResourceCustomerDomain: "",
				wkk.RowKeyResourceServiceName:    "backend-service",
				wkk.RowKeyCTimestamp:             int64(1000),
			},
			expectedServiceIdentifier: "backend-service",
			expectedServiceOk:         true,
			expectedTimestamp:         1000,
			expectedTsOk:              true,
		},
		{
			name: "only service_name present",
			row: pipeline.Row{
				wkk.RowKeyResourceServiceName: "backend-service",
				wkk.RowKeyCTimestamp:          int64(1000),
			},
			expectedServiceIdentifier: "backend-service",
			expectedServiceOk:         true,
			expectedTimestamp:         1000,
			expectedTsOk:              true,
		},
		{
			name: "neither customer_domain nor service_name - uses empty string",
			row: pipeline.Row{
				wkk.RowKeyCTimestamp: int64(1000),
			},
			expectedServiceIdentifier: "",
			expectedServiceOk:         true, // empty string is valid default
			expectedTimestamp:         1000,
			expectedTsOk:              true,
		},
		{
			name: "missing timestamp",
			row: pipeline.Row{
				wkk.RowKeyResourceServiceName: "backend-service",
			},
			expectedServiceIdentifier: "backend-service",
			expectedServiceOk:         true,
			expectedTimestamp:         0,
			expectedTsOk:              false,
		},
	}

	provider := &LogSortKeyProvider{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := provider.MakeKey(tt.row)
			logKey, ok := key.(*LogSortKey)
			require.True(t, ok)

			assert.Equal(t, tt.expectedServiceIdentifier, logKey.ServiceIdentifier)
			assert.Equal(t, tt.expectedServiceOk, logKey.ServiceOk)
			assert.Equal(t, tt.expectedTimestamp, logKey.Timestamp)
			assert.Equal(t, tt.expectedTsOk, logKey.TsOk)

			key.Release()
		})
	}
}

func TestMemorySortingReader_LogSortKey(t *testing.T) {
	// Create rows in unsorted order - testing log sort key
	// Sort order is: [service_identifier, timestamp]
	inputRows := []pipeline.Row{
		{
			wkk.RowKeyResourceServiceName: "service-b", // service-b comes after service-a
			wkk.RowKeyCTimestamp:          int64(1000),
		},
		{
			wkk.RowKeyResourceServiceName: "service-a",
			wkk.RowKeyCTimestamp:          int64(2000),
		},
		{
			wkk.RowKeyResourceServiceName: "service-a",
			wkk.RowKeyCTimestamp:          int64(1000), // Same service, earlier timestamp
		},
		{
			wkk.RowKeyResourceServiceName: "service-b",
			wkk.RowKeyCTimestamp:          int64(500), // service-b, earlier timestamp
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, &LogSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Read all results
	var allRows []pipeline.Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	// Should have 4 rows in sorted order
	require.Len(t, allRows, 4)

	// Verify sorting: [svc-a:ts1000, svc-a:ts2000, svc-b:ts500, svc-b:ts1000]
	expectedOrder := []struct {
		service   string
		timestamp int64
	}{
		{"service-a", 1000},
		{"service-a", 2000},
		{"service-b", 500},
		{"service-b", 1000},
	}

	for i, expected := range expectedOrder {
		assert.Equal(t, expected.service, allRows[i][wkk.RowKeyResourceServiceName], "Row %d service mismatch", i)
		assert.Equal(t, expected.timestamp, allRows[i][wkk.RowKeyCTimestamp], "Row %d timestamp mismatch", i)
	}
}

func TestLogSortKey_CustomerDomainPriority(t *testing.T) {
	// Test that customer_domain takes priority over service_name in sorting
	inputRows := []pipeline.Row{
		{
			wkk.RowKeyResourceCustomerDomain: "zzz.example.com", // customer_domain present, should be used
			wkk.RowKeyResourceServiceName:    "aaa-service",     // ignored because customer_domain is set
			wkk.RowKeyCTimestamp:             int64(1000),
		},
		{
			wkk.RowKeyResourceCustomerDomain: "aaa.example.com", // comes before zzz
			wkk.RowKeyResourceServiceName:    "zzz-service",     // ignored
			wkk.RowKeyCTimestamp:             int64(1000),
		},
	}

	mockReader := NewMockReader(inputRows)
	sortingReader, err := NewMemorySortingReader(mockReader, &LogSortKeyProvider{}, 1000)
	require.NoError(t, err)
	defer func() { _ = sortingReader.Close() }()

	// Read all results
	var allRows []pipeline.Row
	for {
		batch, err := sortingReader.Next(context.TODO())
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		for i := 0; i < batch.Len(); i++ {
			allRows = append(allRows, batch.Get(i))
		}
	}

	require.Len(t, allRows, 2)

	// aaa.example.com should come before zzz.example.com
	assert.Equal(t, "aaa.example.com", allRows[0][wkk.RowKeyResourceCustomerDomain])
	assert.Equal(t, "zzz.example.com", allRows[1][wkk.RowKeyResourceCustomerDomain])
}
