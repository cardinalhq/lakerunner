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

package ingestion

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestLogTranslator_TranslateRow(t *testing.T) {
	// Create a test translator
	translator := NewLogTranslator("test-org", "test-bucket", "test-object.json.gz")

	tests := []struct {
		name    string
		input   filereader.Row
		wantErr bool
		checkFn func(t *testing.T, result filereader.Row)
	}{
		{
			name: "ValidLogWithMessage",
			input: filereader.Row{
				wkk.RowKeyCMessage:   "test log message",
				wkk.RowKeyCTimestamp: int64(1640995200000), // 2022-01-01 00:00:00 UTC
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				// Check resource fields were added
				assert.Equal(t, "test-bucket", result[wkk.NewRowKey("resource.bucket.name")])
				assert.Equal(t, "./test-object.json.gz", result[wkk.NewRowKey("resource.file.name")])
				assert.Equal(t, "testobjectjson", result[wkk.NewRowKey("resource.file.type")])
				// Check required CardinalHQ fields were added
				assert.Equal(t, "logs", result[wkk.RowKeyCTelemetryType])
				assert.Equal(t, "log.events", result[wkk.RowKeyCName])
				assert.Equal(t, float64(1), result[wkk.RowKeyCValue])
				// Check original fields were preserved
				assert.Equal(t, "test log message", result[wkk.RowKeyCMessage])
				assert.Equal(t, int64(1640995200000), result[wkk.RowKeyCTimestamp])
			},
		},
		{
			name: "BasicRowTranslation",
			input: filereader.Row{
				wkk.RowKeyCMessage:                 "log body content",
				wkk.RowKeyCTimestamp:               int64(1640995200000),
				wkk.NewRowKey("_cardinalhq.level"): "info",
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				// Check resource fields were added
				assert.Equal(t, "test-bucket", result[wkk.NewRowKey("resource.bucket.name")])
				assert.Equal(t, "./test-object.json.gz", result[wkk.NewRowKey("resource.file.name")])
				assert.Equal(t, "testobjectjson", result[wkk.NewRowKey("resource.file.type")])
				// Check required CardinalHQ fields were added
				assert.Equal(t, "logs", result[wkk.RowKeyCTelemetryType])
				assert.Equal(t, "log.events", result[wkk.RowKeyCName])
				assert.Equal(t, float64(1), result[wkk.RowKeyCValue])
				// Check original fields were preserved
				assert.Equal(t, "log body content", result[wkk.RowKeyCMessage])
				assert.Equal(t, int64(1640995200000), result[wkk.RowKeyCTimestamp])
				assert.Equal(t, "info", result[wkk.NewRowKey("_cardinalhq.level")])
			},
		},
		{
			name: "MinimalRow",
			input: filereader.Row{
				wkk.RowKeyCTimestamp:        int64(1640995300000),
				wkk.NewRowKey("some_field"): "some_value",
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				// Check resource fields were added
				assert.Equal(t, "test-bucket", result[wkk.NewRowKey("resource.bucket.name")])
				assert.Equal(t, "./test-object.json.gz", result[wkk.NewRowKey("resource.file.name")])
				assert.Equal(t, "testobjectjson", result[wkk.NewRowKey("resource.file.type")])
				// Check required CardinalHQ fields were added
				assert.Equal(t, "logs", result[wkk.RowKeyCTelemetryType])
				assert.Equal(t, "log.events", result[wkk.RowKeyCName])
				assert.Equal(t, float64(1), result[wkk.RowKeyCValue])
				// Check original fields were preserved
				assert.Equal(t, int64(1640995300000), result[wkk.RowKeyCTimestamp])
				assert.Equal(t, "some_value", result[wkk.NewRowKey("some_field")])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a copy of input to verify original is modified
			row := make(filereader.Row)
			for k, v := range tt.input {
				row[k] = v
			}

			err := translator.TranslateRow(&row)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.checkFn != nil {
					tt.checkFn(t, row)
				}
			}
		})
	}
}