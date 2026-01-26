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

package messages

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestObjStoreNotificationMessage_RecordCount_Compression(t *testing.T) {
	tests := []struct {
		name         string
		objectID     string
		fileSize     int64
		expectedSize int64
		description  string
	}{
		{
			name:         "logs gz uses 150x",
			objectID:     "otel-raw/logs/org1/2024/01/logs_123.binpb.gz",
			fileSize:     1000,
			expectedSize: 150000,
			description:  "logs .gz files use 150x estimate",
		},
		{
			name:         "metrics gz uses 20x",
			objectID:     "otel-raw/metrics/org1/2024/01/metrics_123.binpb.gz",
			fileSize:     1000,
			expectedSize: 20000,
			description:  "metrics .gz files use 20x estimate",
		},
		{
			name:         "traces gz uses 40x",
			objectID:     "otel-raw/traces/org1/2024/01/traces_123.binpb.gz",
			fileSize:     1000,
			expectedSize: 40000,
			description:  "traces .gz files use 40x estimate",
		},
		{
			name:         "logs filename prefix detection",
			objectID:     "some/path/logs_456.binpb.gz",
			fileSize:     1000,
			expectedSize: 150000,
			description:  "logs_ filename prefix triggers 150x",
		},
		{
			name:         "metrics filename prefix detection",
			objectID:     "some/path/metrics_456.binpb.gz",
			fileSize:     1000,
			expectedSize: 20000,
			description:  "metrics_ filename prefix triggers 20x",
		},
		{
			name:         "unknown signal uses default 150x",
			objectID:     "some/path/data.binpb.gz",
			fileSize:     1000,
			expectedSize: 150000,
			description:  "unknown signal uses conservative 150x default",
		},
		{
			name:         "json file uses actual size",
			objectID:     "logs/org1/2024/01/data.json",
			fileSize:     1000,
			expectedSize: 1000,
			description:  "uncompressed JSON uses actual size",
		},
		{
			name:         "parquet file uses actual size",
			objectID:     "metrics/org1/2024/01/data.parquet",
			fileSize:     1500,
			expectedSize: 1500,
			description:  "parquet files use actual size",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &ObjStoreNotificationMessage{
				OrganizationID: uuid.New(),
				InstanceNum:    1,
				Bucket:         "test-bucket",
				ObjectID:       tt.objectID,
				FileSize:       tt.fileSize,
			}

			result := msg.RecordCount()
			assert.Equal(t, tt.expectedSize, result, tt.description)
		})
	}
}

func TestObjStoreNotificationMessage_RecordCount_EdgeCases(t *testing.T) {
	tests := []struct {
		name         string
		objectID     string
		fileSize     int64
		expectedSize int64
	}{
		{
			name:         "zero size file",
			objectID:     "data.json",
			fileSize:     0,
			expectedSize: 0,
		},
		{
			name:         "small json file uses actual size",
			objectID:     "data.json",
			fileSize:     5,
			expectedSize: 5,
		},
		{
			name:         "empty filename",
			objectID:     "",
			fileSize:     1000,
			expectedSize: 1000, // defaults to actual size (not .gz)
		},
		{
			name:         "zero size logs gz file",
			objectID:     "logs_123.binpb.gz",
			fileSize:     0,
			expectedSize: 0,
		},
		{
			name:         "small logs gz uses 150x",
			objectID:     "logs_123.binpb.gz",
			fileSize:     10,
			expectedSize: 1500,
		},
		{
			name:         "small metrics gz uses 20x",
			objectID:     "metrics_123.binpb.gz",
			fileSize:     10,
			expectedSize: 200,
		},
		{
			name:         "small traces gz uses 40x",
			objectID:     "traces_123.binpb.gz",
			fileSize:     10,
			expectedSize: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &ObjStoreNotificationMessage{
				OrganizationID: uuid.New(),
				InstanceNum:    1,
				Bucket:         "test-bucket",
				ObjectID:       tt.objectID,
				FileSize:       tt.fileSize,
			}

			result := msg.RecordCount()
			assert.Equal(t, tt.expectedSize, result)
		})
	}
}

func TestObjStoreNotificationMessage_IsParquet(t *testing.T) {
	tests := []struct {
		name      string
		objectID  string
		isParquet bool
	}{
		{
			name:      "parquet file",
			objectID:  "path/to/file.parquet",
			isParquet: true,
		},
		{
			name:      "parquet in nested path",
			objectID:  "org/2024/01/15/data.parquet",
			isParquet: true,
		},
		{
			name:      "json.gz file",
			objectID:  "path/to/file.json.gz",
			isParquet: false,
		},
		{
			name:      "binpb file",
			objectID:  "path/to/file.binpb",
			isParquet: false,
		},
		{
			name:      "json file",
			objectID:  "path/to/file.json",
			isParquet: false,
		},
		{
			name:      "empty objectID",
			objectID:  "",
			isParquet: false,
		},
		{
			name:      "parquet in middle of path is not parquet file",
			objectID:  "path/parquet/file.json",
			isParquet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &ObjStoreNotificationMessage{
				ObjectID: tt.objectID,
			}
			assert.Equal(t, tt.isParquet, msg.IsParquet())
		})
	}
}
