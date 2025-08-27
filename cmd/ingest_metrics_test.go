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
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// TestHandleHistogram was moved to the proto reader - no longer needed here
// The proto reader now handles all histogram processing internally

func TestMetricWriterManager(t *testing.T) {
	tmpdir := t.TempDir()
	orgID := "test-org"
	ingestDateint := int32(20250101)
	rpfEstimate := int64(1000)
	ll := slog.Default()

	wm := newMetricWriterManager(tmpdir, orgID, ingestDateint, rpfEstimate, ll)
	require.NotNil(t, wm)
	require.Equal(t, tmpdir, wm.tmpdir)
	require.Equal(t, orgID, wm.orgID)
	require.Equal(t, ingestDateint, wm.ingestDateint)
	require.Equal(t, rpfEstimate, wm.rpfEstimate)
	require.NotNil(t, wm.writers)
	require.Equal(t, ll, wm.ll)
}

func TestMetricWriterManager_ProcessRow(t *testing.T) {
	tmpdir := t.TempDir()
	wm := newMetricWriterManager(tmpdir, "test-org", int32(20250101), int64(1000), slog.Default())

	// Test metric row
	row := filereader.Row{
		wkk.RowKeyCTimestamp:    int64(1640995200000),
		wkk.RowKeyCMetricType:   "gauge",
		wkk.RowKeyCName:         "cpu.usage",
		wkk.RowKeyCValue:        float64(75.5),
		wkk.NewRowKey("host"):   "web-server-1",
		wkk.NewRowKey("region"): "us-west-2",
	}

	err := wm.processRow(row)
	require.NoError(t, err)
}

func TestMetricWriterManager_ProcessMultipleValues(t *testing.T) {
	tmpdir := t.TempDir()
	wm := newMetricWriterManager(tmpdir, "test-org", int32(20250101), int64(1000), slog.Default())

	// Test multiple metric rows
	row1 := filereader.Row{
		wkk.RowKeyCTimestamp:    int64(1640995200000),
		wkk.RowKeyCMetricType:   "gauge",
		wkk.RowKeyCName:         "cpu.usage",
		wkk.RowKeyCValue:        float64(75.5),
		wkk.NewRowKey("host"):   "web-server-1",
		wkk.NewRowKey("region"): "us-west-2",
	}

	row2 := filereader.Row{
		wkk.RowKeyCTimestamp:    int64(1640995200000),
		wkk.RowKeyCMetricType:   "gauge",
		wkk.RowKeyCName:         "cpu.usage",
		wkk.RowKeyCValue:        float64(82.3),
		wkk.NewRowKey("host"):   "web-server-1",
		wkk.NewRowKey("region"): "us-west-2",
	}

	err := wm.processRow(row1)
	require.NoError(t, err)

	err = wm.processRow(row2)
	require.NoError(t, err)
}

func TestMetricTranslator(t *testing.T) {
	translator := &metricsprocessing.MetricTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "metrics/test.json.gz",
	}

	row := filereader.Row{
		wkk.RowKeyCName:       "cpu.usage",
		wkk.RowKeyCTimestamp:  int64(1756049235874),
		wkk.NewRowKey("host"): "web-server-1",
	}

	err := translator.TranslateRow(&row)
	require.NoError(t, err)

	require.Equal(t, "test-bucket", row[wkk.NewRowKey("resource.bucket.name")])
	require.Equal(t, "./metrics/test.json.gz", row[wkk.NewRowKey("resource.file.name")])
	require.Equal(t, "test-org", row[wkk.RowKeyCCustomerID])
	require.Equal(t, "metrics", row[wkk.RowKeyCTelemetryType])
	require.Equal(t, "cpu.usage", row[wkk.RowKeyCName])               // Original field preserved
	require.Equal(t, "web-server-1", row[wkk.NewRowKey("host")])      // Original field preserved
	require.Equal(t, int64(1756049230000), row[wkk.RowKeyCTimestamp]) // Timestamp truncated to 10s boundary

	// Check that TID was computed and added
	tid, ok := row[wkk.RowKeyCTID].(int64)
	require.True(t, ok, "TID should be computed and added as int64")
	require.NotZero(t, tid, "TID should be non-zero")
}

func TestMetricTranslator_TimestampTruncation(t *testing.T) {
	translator := &metricsprocessing.MetricTranslator{
		OrgID:    "test-org",
		Bucket:   "test-bucket",
		ObjectID: "metrics/test.json.gz",
	}

	testCases := []struct {
		name              string
		inputTimestamp    int64
		expectedTruncated int64
	}{
		{
			name:              "exact 10s boundary",
			inputTimestamp:    1640995200000, // 2022-01-01 00:00:00.000
			expectedTruncated: 1640995200000,
		},
		{
			name:              "mid-interval",
			inputTimestamp:    1640995205874, // 2022-01-01 00:00:05.874
			expectedTruncated: 1640995200000, // truncated to 2022-01-01 00:00:00.000
		},
		{
			name:              "near next boundary",
			inputTimestamp:    1640995209999, // 2022-01-01 00:00:09.999
			expectedTruncated: 1640995200000, // truncated to 2022-01-01 00:00:00.000
		},
		{
			name:              "next boundary",
			inputTimestamp:    1640995210000, // 2022-01-01 00:00:10.000
			expectedTruncated: 1640995210000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			row := filereader.Row{
				wkk.RowKeyCName:       "cpu.usage",
				wkk.RowKeyCTimestamp:  tc.inputTimestamp,
				wkk.NewRowKey("host"): "web-server-1",
			}

			err := translator.TranslateRow(&row)
			require.NoError(t, err)

			actualTimestamp, ok := row[wkk.RowKeyCTimestamp].(int64)
			require.True(t, ok, "timestamp should be int64")
			require.Equal(t, tc.expectedTruncated, actualTimestamp)
		})
	}
}
