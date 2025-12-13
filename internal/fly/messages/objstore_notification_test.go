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
			name:         "gz file uses actual size",
			objectID:     "logs/org1/2024/01/data.json.gz",
			fileSize:     1000,
			expectedSize: 1000,
			description:  ".gz files use actual size",
		},
		{
			name:         "json file uses 10x smaller",
			objectID:     "logs/org1/2024/01/data.json",
			fileSize:     1000,
			expectedSize: 100,
			description:  "uncompressed JSON uses 10x smaller than actual size",
		},
		{
			name:         "binpb file uses 10x smaller",
			objectID:     "traces/org1/2024/01/data.binpb",
			fileSize:     2000,
			expectedSize: 200,
			description:  "uncompressed binpb uses 10x smaller than actual size",
		},
		{
			name:         "parquet file uses actual size",
			objectID:     "metrics/org1/2024/01/data.parquet",
			fileSize:     1500,
			expectedSize: 1500,
			description:  "parquet files use actual size",
		},
		{
			name:         "other file uses actual size",
			objectID:     "logs/org1/2024/01/data.avro",
			fileSize:     800,
			expectedSize: 800,
			description:  "other file types use actual size",
		},
		{
			name:         "json.gz uses actual size (gz takes precedence)",
			objectID:     "logs/org1/2024/01/data.json.gz",
			fileSize:     500,
			expectedSize: 500,
			description:  ".gz suffix takes precedence over .json",
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
			name:         "very small json file",
			objectID:     "data.json",
			fileSize:     5,
			expectedSize: 0, // 5/10 = 0 due to integer division
		},
		{
			name:         "empty filename",
			objectID:     "",
			fileSize:     1000,
			expectedSize: 1000, // defaults to actual size
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
