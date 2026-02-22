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

package workmanager

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	executionWorkCompleted metric.Int64Counter
	executionWorkFailed    metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryworker/workmanager")

	var err error

	executionWorkCompleted, err = meter.Int64Counter(
		"lakerunner.query.execution.work_completed",
		metric.WithDescription("Number of work items completed successfully"),
	)
	if err != nil {
		log.Fatalf("failed to create execution.work_completed counter: %v", err)
	}

	executionWorkFailed, err = meter.Int64Counter(
		"lakerunner.query.execution.work_failed",
		metric.WithDescription("Number of work items that failed execution"),
	)
	if err != nil {
		log.Fatalf("failed to create execution.work_failed counter: %v", err)
	}
}

func registerExecutionQueueDepthGauge(m *Manager) {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryworker/workmanager")
	_, err := meter.Int64ObservableGauge(
		"lakerunner.query.execution.queue_depth",
		metric.WithDescription("Number of in-flight work items on the worker"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(m.InFlightCount()))
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create execution.queue_depth gauge: %v", err)
	}
}

func recordWorkCompleted() {
	executionWorkCompleted.Add(context.Background(), 1)
}

func recordWorkFailed() {
	executionWorkFailed.Add(context.Background(), 1)
}
