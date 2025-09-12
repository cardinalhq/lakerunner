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

package sweeper

import (
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	logCleanupCounter    metric.Int64Counter
	logCleanupBytes      metric.Int64Counter
	metricCleanupCounter metric.Int64Counter
	metricCleanupBytes   metric.Int64Counter
	traceCleanupCounter  metric.Int64Counter
	traceCleanupBytes    metric.Int64Counter
)

func init() {
	meter := otel.Meter("lakerunner.sweeper")

	// Log cleanup metrics
	var err error
	logCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.log_cleanup_total",
		metric.WithDescription("Count of log segments processed during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create log_cleanup_total counter: %w", err))
	}

	logCleanupBytes, err = meter.Int64Counter(
		"lakerunner.sweeper.log_cleanup_bytes_total",
		metric.WithDescription("Total bytes of log segments deleted during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create log_cleanup_bytes_total counter: %w", err))
	}

	// Metric cleanup metrics
	metricCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.metric_cleanup_total",
		metric.WithDescription("Count of metric segments processed during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create metric_cleanup_total counter: %w", err))
	}

	metricCleanupBytes, err = meter.Int64Counter(
		"lakerunner.sweeper.metric_cleanup_bytes_total",
		metric.WithDescription("Total bytes of metric segments deleted during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create metric_cleanup_bytes_total counter: %w", err))
	}

	// Trace cleanup metrics
	traceCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.trace_cleanup_total",
		metric.WithDescription("Count of trace segments processed during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create trace_cleanup_total counter: %w", err))
	}

	traceCleanupBytes, err = meter.Int64Counter(
		"lakerunner.sweeper.trace_cleanup_bytes_total",
		metric.WithDescription("Total bytes of trace segments deleted during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create trace_cleanup_bytes_total counter: %w", err))
	}
}
