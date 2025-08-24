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

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func TestCreateLogReader(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name     string
		objectID string
		setupFn  func(t *testing.T) string // Returns filename
		wantErr  bool
		wantType string
	}{
		{
			name:     "ParquetFile",
			objectID: "test.parquet",
			setupFn: func(t *testing.T) string {
				// Create empty parquet file
				filename := filepath.Join(tempDir, "test.parquet")
				file, err := os.Create(filename)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				return filename
			},
			wantErr:  true, // Empty parquet file will fail to read
			wantType: "*filereader.ParquetReader",
		},
		{
			name:     "JSONGzFile",
			objectID: "test.json.gz",
			setupFn: func(t *testing.T) string {
				// Create empty json.gz file
				filename := filepath.Join(tempDir, "test.json.gz")
				file, err := os.Create(filename)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				return filename
			},
			wantErr:  true, // Empty json.gz file will fail to read
			wantType: "*filereader.JSONLinesReader",
		},
		{
			name:     "BinpbFile",
			objectID: "test.binpb",
			setupFn: func(t *testing.T) string {
				// Create empty binpb file
				filename := filepath.Join(tempDir, "test.binpb")
				file, err := os.Create(filename)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				return filename
			},
			wantErr:  false, // Empty binpb file creates reader successfully
			wantType: "*filereader.ProtoLogsReader",
		},
		{
			name:     "UnsupportedFile",
			objectID: "test.txt",
			setupFn: func(t *testing.T) string {
				filename := filepath.Join(tempDir, "test.txt")
				file, err := os.Create(filename)
				if err != nil {
					t.Fatal(err)
				}
				file.Close()
				return filename
			},
			wantErr:  true,
			wantType: "",
		},
		{
			name:     "NonExistentFile",
			objectID: "nonexistent.parquet",
			setupFn: func(t *testing.T) string {
				return filepath.Join(tempDir, "nonexistent.parquet")
			},
			wantErr:  true,
			wantType: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filename := tt.setupFn(t)

			reader, err := createLogReader(filename)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if reader != nil {
				defer reader.Close()
			}
		})
	}
}

func TestWriterManager_HourSlotKey(t *testing.T) {
	tests := []struct {
		name      string
		timestamp int64
		want      hourSlotKey
	}{
		{
			name:      "Epoch",
			timestamp: 0,
			want:      hourSlotKey{dateint: 19700101, hour: 0, slot: 0},
		},
		{
			name:      "Y2K",
			timestamp: 946684800000, // 2000-01-01 00:00:00 UTC
			want:      hourSlotKey{dateint: 20000101, hour: 0, slot: 0},
		},
		{
			name:      "MidDay",
			timestamp: 946728000000, // 2000-01-01 12:00:00 UTC
			want:      hourSlotKey{dateint: 20000101, hour: 12, slot: 0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the helper function that creates the key
			dateint, hour16 := helpers.MSToDateintHour(tt.timestamp)
			hour := int(hour16)
			slot := 0
			got := hourSlotKey{dateint, hour, slot}

			assert.Equal(t, tt.want, got)
		})
	}
}
