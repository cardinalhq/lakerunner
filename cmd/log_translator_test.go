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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

func TestLogTranslator_TranslateRow(t *testing.T) {
	// Create a test translator
	trieClusterManager := fingerprinter.NewTrieClusterManager(0.5)
	translator := &LogTranslator{
		orgID:              "test-org",
		bucket:             "test-bucket",
		objectID:           "test-object.json.gz",
		trieClusterManager: trieClusterManager,
	}

	tests := []struct {
		name    string
		input   filereader.Row
		wantErr bool
		checkFn func(t *testing.T, result filereader.Row)
	}{
		{
			name: "ValidLogWithMessage",
			input: filereader.Row{
				"message":               "test log message",
				"_cardinalhq.timestamp": int64(1640995200000), // 2022-01-01 00:00:00 UTC
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				// Check fingerprint was calculated
				assert.Contains(t, result, "_cardinalhq.fingerprint", "Expected fingerprint to be set")
				// Check resource fields were added
				assert.Equal(t, "test-bucket", result["resource.bucket.name"])
				assert.Equal(t, "./test-object.json.gz", result["resource.file.name"])
				assert.Equal(t, "testobjectjson", result["resource.file.type"])
				// Check timestamp was preserved
				assert.Equal(t, int64(1640995200000), result["_cardinalhq.timestamp"])
			},
		},
		{
			name: "ExistingFingerprintOverwritten",
			input: filereader.Row{
				"message":                 "new log message",
				"_cardinalhq.fingerprint": int64(999999), // This should be overwritten
				"_cardinalhq.timestamp":   int64(1640995200000),
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				fingerprint, ok := result["_cardinalhq.fingerprint"]
				require.True(t, ok, "Expected fingerprint to be set")
				// Should be a different fingerprint than the original
				assert.NotEqual(t, int64(999999), fingerprint, "Expected original fingerprint to be overwritten")
			},
		},
		{
			name: "NoMessageRemovesFingerprint",
			input: filereader.Row{
				"_cardinalhq.fingerprint": int64(123456), // This should be removed
				"_cardinalhq.timestamp":   int64(1640995200000),
				"level":                   "info",
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				assert.NotContains(t, result, "_cardinalhq.fingerprint", "Expected fingerprint to be removed when no message found")
			},
		},
		{
			name: "DifferentMessageFields",
			input: filereader.Row{
				"body":                  "log body content",
				"_cardinalhq.timestamp": int64(1640995200000),
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				assert.Contains(t, result, "_cardinalhq.fingerprint", "Expected fingerprint to be calculated from 'body' field")
			},
		},
		{
			name: "NoTimestampFieldAddsTimestamp",
			input: filereader.Row{
				"message":   "test message without timestamp",
				"timestamp": int64(1640995200), // Unix seconds
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				// Should have fingerprint calculated
				assert.Contains(t, result, "_cardinalhq.fingerprint", "Expected fingerprint to be set")

				// Should have timestamp converted from 'timestamp' field
				ts, ok := result["_cardinalhq.timestamp"].(int64)
				require.True(t, ok, "Expected _cardinalhq.timestamp to be int64")
				assert.Equal(t, int64(1640995200000), ts, "Expected timestamp to be converted to milliseconds") // 1640995200 * 1000
			},
		},
		{
			name: "NoTimestampAtAllUsesCurrentTime",
			input: filereader.Row{
				"message": "test message with no timestamp anywhere",
				"level":   "info",
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				// Should have fingerprint calculated
				assert.Contains(t, result, "_cardinalhq.fingerprint", "Expected fingerprint to be set")

				// Should have timestamp added (current time)
				ts, ok := result["_cardinalhq.timestamp"].(int64)
				require.True(t, ok, "Expected _cardinalhq.timestamp to be int64")
				assert.Greater(t, ts, int64(1640995200000), "Expected timestamp to be greater than test reference time")
			},
		},
		{
			name: "TimestampTypeConversion",
			input: filereader.Row{
				"message":               "test message",
				"_cardinalhq.timestamp": float64(1640995200000.0),
			},
			wantErr: false,
			checkFn: func(t *testing.T, result filereader.Row) {
				ts, ok := result["_cardinalhq.timestamp"].(int64)
				require.True(t, ok, "Expected timestamp to be int64")
				assert.Equal(t, int64(1640995200000), ts)
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

func TestLogTranslator_CalculateFingerprint(t *testing.T) {
	trieClusterManager := fingerprinter.NewTrieClusterManager(0.5)
	translator := &LogTranslator{
		orgID:              "test-org",
		bucket:             "test-bucket",
		objectID:           "test-object.json.gz",
		trieClusterManager: trieClusterManager,
	}

	tests := []struct {
		name    string
		input   filereader.Row
		want    int64
		wantErr bool
	}{
		{
			name: "MessageField",
			input: filereader.Row{
				"message": "test log message",
			},
			want:    0, // We expect some fingerprint, but exact value depends on implementation
			wantErr: false,
		},
		{
			name: "BodyField",
			input: filereader.Row{
				"body": "test log body",
			},
			want:    0, // We expect some fingerprint
			wantErr: false,
		},
		{
			name: "CardinalHQMessageField",
			input: filereader.Row{
				"_cardinalhq.message": "cardinalhq message",
			},
			want:    0, // We expect some fingerprint
			wantErr: false,
		},
		{
			name: "NoMessageFields",
			input: filereader.Row{
				"level":     "info",
				"timestamp": "2022-01-01T00:00:00Z",
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "EmptyMessage",
			input: filereader.Row{
				"message": "",
			},
			want:    0,
			wantErr: false,
		},
		{
			name: "PreferenceOrder",
			input: filereader.Row{
				"_cardinalhq.message": "preferred message",
				"message":             "secondary message",
				"body":                "tertiary message",
			},
			want:    0, // Should use _cardinalhq.message
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := translator.calculateFingerprint(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// For cases where we expect a fingerprint (non-zero), just check it's not zero
			if tt.name != "NoMessageFields" && tt.name != "EmptyMessage" {
				assert.NotZero(t, got, "expected non-zero fingerprint")
			} else {
				// For cases with no message, expect zero
				assert.Zero(t, got, "expected 0 fingerprint")
			}
		})
	}
}

func TestEnsureInt64(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
		want  int64
	}{
		{
			name:  "Int64",
			input: int64(123456789),
			want:  123456789,
		},
		{
			name:  "Float64",
			input: float64(123456789.0),
			want:  123456789,
		},
		{
			name:  "Int",
			input: int(123456789),
			want:  123456789,
		},
		{
			name:  "Int32",
			input: int32(123456789),
			want:  123456789,
		},
		{
			name:  "String",
			input: "not a number",
			want:  0,
		},
		{
			name:  "Nil",
			input: nil,
			want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureInt64(tt.input)
			assert.Equal(t, tt.want, got)
		})
	}
}
