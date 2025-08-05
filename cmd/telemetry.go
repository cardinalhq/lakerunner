// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/oteltools/pkg/telemetry"
	slogmulti "github.com/samber/slog-multi"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
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

		if err := runtime.Start(runtime.WithMinimumReadMemStatsInterval(time.Second)); err != nil {
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
}
