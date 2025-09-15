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

package metricsprocessing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestReportDropAndTelemetry(t *testing.T) {
	ctx := context.Background()

	// Set up a manual reader to capture metrics
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(prev)

	// Reinitialize instruments with new provider
	initTelemetry()

	reportDrop(ctx, "test", 2)
	reportTelemetry(ctx, "test", 1, 1, 3, 4, 100, 200)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(ctx, &rm))

	var dropFound, inHistFound, outHistFound bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "lakerunner.processing.dropped":
				data := m.Data.(metricdata.Sum[int64])
				require.Equal(t, int64(2), data.DataPoints[0].Value)
				dropFound = true
			case "lakerunner.processing.bytes.in.size":
				data := m.Data.(metricdata.Histogram[int64])
				require.Equal(t, int64(100), data.DataPoints[0].Sum)
				inHistFound = true
			case "lakerunner.processing.bytes.out.size":
				data := m.Data.(metricdata.Histogram[int64])
				require.Equal(t, int64(200), data.DataPoints[0].Sum)
				outHistFound = true
			}
		}
	}

	require.True(t, dropFound, "drop metric not found")
	require.True(t, inHistFound, "bytes in histogram not found")
	require.True(t, outHistFound, "bytes out histogram not found")
}
