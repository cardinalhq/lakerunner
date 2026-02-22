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

package streammanager

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	streamDisconnects metric.Int64Counter
	streamReconnects  metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryapi/streammanager")

	var err error

	streamDisconnects, err = meter.Int64Counter(
		"lakerunner.control.stream.disconnects",
		metric.WithDescription("Control stream disconnect count"),
	)
	if err != nil {
		log.Fatalf("failed to create stream.disconnects counter: %v", err)
	}

	streamReconnects, err = meter.Int64Counter(
		"lakerunner.control.stream.reconnects",
		metric.WithDescription("Successful control stream reconnect count"),
	)
	if err != nil {
		log.Fatalf("failed to create stream.reconnects counter: %v", err)
	}
}

func registerActiveStreamGauge(m *Manager) {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryapi/streammanager")
	_, err := meter.Int64ObservableGauge(
		"lakerunner.control.stream.active_count",
		metric.WithDescription("Number of active control streams"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			m.mu.RLock()
			count := len(m.workers)
			m.mu.RUnlock()
			o.Observe(int64(count))
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create stream.active_count gauge: %v", err)
	}
}

func recordDisconnect(workerID string) {
	streamDisconnects.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("worker_id", workerID),
	))
}

func recordReconnect(workerID string) {
	streamReconnects.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("worker_id", workerID),
	))
}
