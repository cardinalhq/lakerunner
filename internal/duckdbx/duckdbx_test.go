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

package duckdbx

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestWithNameOption(t *testing.T) {
	var cfg Config
	WithName("foo")(&cfg)
	require.Equal(t, "foo", cfg.InstanceName)
}

func TestPollMemoryMetricsRecordsInstanceName(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	orig := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	t.Cleanup(func() { otel.SetMeterProvider(orig) })

	db, err := Open(":memory:",
		WithMetrics(10*time.Millisecond),
		WithMetricsContext(ctx),
		WithName("test-instance"),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// allow poller to run at least once
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(10 * time.Millisecond)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[int64]); ok {
				for _, dp := range g.DataPoints {
					if v, ok := dp.Attributes.Value("instance_name"); ok && v.AsString() == "test-instance" {
						found = true
					}
				}
			}
		}
	}
	require.True(t, found, "expected metric with instance_name attribute")
}
