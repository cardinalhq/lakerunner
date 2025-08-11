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
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	slogmulti "github.com/samber/slog-multi"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/host"
	iruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	commonAttributes attribute.Set

	meter  = otel.Meter("github.com/cardinalhq/lakerunner")
	tracer = otel.Tracer("github.com/cardinalhq/lakerunner")

	myInstanceID int64

	dbExecDuration         metric.Float64Histogram
	inqueueFetchDuration   metric.Float64Histogram
	inqueueDuration        metric.Float64Histogram
	workqueueDuration      metric.Float64Histogram
	workqueueFetchDuration metric.Float64Histogram
	workqueueLag           metric.Float64Histogram
	manualGCHistogram      metric.Float64Histogram

	// existsGauge is a gauge that indicates if the service is running (1) or not (0).
	// It is set to 1, and never changes.
	existsGauge metric.Int64Gauge
)

func setupTelemetry(servicename string) (context.Context, func() error, error) {
	myInstanceID = idgen.DefaultFlakeGenerator.NextID()

	// Catch signals to stop the process as gracefully as possible.
	doneCtx, doneCancel := handleSignals(context.Background())

	f := func() error {
		return nil
	}

	// make all the counters, gauges, etc that everyone is likely to use.
	setupGlobalMetrics()

	commonAttributes = attribute.NewSet(
		attribute.Int64("instanceID", myInstanceID),
	)

	if os.Getenv("OTEL_SERVICE_NAME") != "" && os.Getenv("ENABLE_OTLP_TELEMETRY") == "true" {
		slog.Info("OpenTelemetry exporting enabled")
		slog.SetDefault(slog.New(slogmulti.Fanout(
			slog.NewTextHandler(os.Stdout, nil),
			otelslog.NewHandler(servicename),
		)).With(
			slog.String("service", servicename),
			slog.Int64("instanceID", myInstanceID),
		))

		otelShutdown, err := telemetry.SetupOTelSDK(doneCtx)
		if err != nil {
			return doneCtx, nil, fmt.Errorf("failed to setup OpenTelemetry SDK: %w", err)
		}

		if err := iruntime.Start(iruntime.WithMinimumReadMemStatsInterval(time.Second * 10)); err != nil {
			slog.Warn("failed to start runtime metrics", "error", err.Error())
		}

		if err := host.Start(); err != nil {
			slog.Warn("failed to start host metrics", "error", err.Error())
		}

		f = func() error {
			defer doneCancel()
			slog.Info("Shutting down OpenTelemetry SDK")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			return otelShutdown(ctx)
		}
	}

	return doneCtx, f, nil
}

func setupGlobalMetrics() {
	m, err := meter.Float64Histogram(
		"lakerunner.workqueue.request.delay",
		metric.WithUnit("ms"),
		metric.WithDescription("The delay in ms for a request for new work to be returned"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create queue.request.delay histogram: %w", err))
	}
	workqueueFetchDuration = m

	m, err = meter.Float64Histogram(
		"lakerunner.workqueue.duration",
		metric.WithUnit("s"),
		metric.WithDescription("The duration in seconds for a work item to be processed"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create work.duration histogram: %w", err))
	}
	workqueueDuration = m

	m, err = meter.Float64Histogram(
		"lakerunner.inqueue.request.delay",
		metric.WithUnit("s"),
		metric.WithDescription("The delay in seconds for a request for new inqueue work to be returned"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create inqueue.request.delay histogram: %w", err))
	}
	inqueueFetchDuration = m

	m, err = meter.Float64Histogram(
		"lakerunner.inqueue.duration",
		metric.WithUnit("s"),
		metric.WithDescription("The duration in seconds for an inqueue item to be processed"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create inqueue.duration histogram: %w", err))
	}
	inqueueDuration = m

	m, err = meter.Float64Histogram(
		"lakerunner.db.exec.duration",
		metric.WithUnit("s"),
		metric.WithDescription("The duration in seconds for a database update to be processed"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create db.update.duration histogram: %w", err))
	}
	dbExecDuration = m

	m, err = meter.Float64Histogram(
		"lakerunner.workqueue.lag",
		metric.WithUnit("s"),
		metric.WithDescription("The lag in seconds for a work item to be processed in the work queue"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create workqueue.lag histogram: %w", err))
	}
	workqueueLag = m

	m, err = meter.Float64Histogram(
		"lakerunner.manual_gc.duration",
		metric.WithDescription("Duration of manual garbage collection in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create manual_gc.duration histogram: %w", err))
	}
	manualGCHistogram = m

	mg, err := meter.Int64Gauge(
		"lakerunner.exists",
		metric.WithDescription("Indicates if the service is running (1) or not (0)"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create exists.gauge: %w", err))
	}
	existsGauge = mg
	mg.Record(context.Background(), 1, metric.WithAttributeSet(commonAttributes))
}

func gc() {
	n := time.Now()
	runtime.GC()
	manualGCHistogram.Record(context.Background(), time.Since(n).Seconds(), metric.WithAttributeSet(commonAttributes))
}
