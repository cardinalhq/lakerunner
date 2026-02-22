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
	workDispatched metric.Int64Counter
	workAccepted   metric.Int64Counter
	workRejected   metric.Int64Counter
	reassignments  metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryapi/workmanager")

	var err error

	workDispatched, err = meter.Int64Counter(
		"lakerunner.query.scheduling.work_dispatched",
		metric.WithDescription("Number of work items dispatched to workers"),
	)
	if err != nil {
		log.Fatalf("failed to create work_dispatched counter: %v", err)
	}

	workAccepted, err = meter.Int64Counter(
		"lakerunner.query.scheduling.work_accepted",
		metric.WithDescription("Number of work items accepted by workers"),
	)
	if err != nil {
		log.Fatalf("failed to create work_accepted counter: %v", err)
	}

	workRejected, err = meter.Int64Counter(
		"lakerunner.query.scheduling.work_rejected",
		metric.WithDescription("Number of work items rejected by workers"),
	)
	if err != nil {
		log.Fatalf("failed to create work_rejected counter: %v", err)
	}

	reassignments, err = meter.Int64Counter(
		"lakerunner.query.scheduling.reassignments",
		metric.WithDescription("Number of work item reassignments"),
	)
	if err != nil {
		log.Fatalf("failed to create reassignments counter: %v", err)
	}
}

func registerQueueDepthGauge(m *Manager) {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryapi/workmanager")
	_, err := meter.Int64ObservableGauge(
		"lakerunner.query.scheduling.queue_depth",
		metric.WithDescription("Number of pending (non-terminal) work items"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(m.coord.Work.PendingCount()))
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create queue_depth gauge: %v", err)
	}
}

func recordWorkDispatched() {
	workDispatched.Add(context.Background(), 1)
}

func recordWorkAccepted() {
	workAccepted.Add(context.Background(), 1)
}

func recordWorkRejected() {
	workRejected.Add(context.Background(), 1)
}

func recordReassignment() {
	reassignments.Add(context.Background(), 1)
}
