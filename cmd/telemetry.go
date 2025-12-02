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
	"time"

	"github.com/cardinalhq/oteltools/pkg/telemetry"
	slogmulti "github.com/samber/slog-multi"
	"github.com/shirou/gopsutil/v4/process"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/contrib/instrumentation/host"
	iruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

var (
	commonAttributes attribute.Set

	meter = otel.Meter("github.com/cardinalhq/lakerunner")

	myInstanceID int64
)

func setupTelemetry(servicename string, addlAttrs *attribute.Set) (context.Context, func() error, error) {
	myInstanceID = idgen.DefaultFlakeGenerator.NextID()

	// Catch signals to stop the process as gracefully as possible.
	doneCtx, doneCancel := handleSignals(context.Background())

	f := func() error {
		return nil
	}

	attrs := []attribute.KeyValue{}
	if addlAttrs != nil {
		iter := addlAttrs.Iter()
		for iter.Next() {
			attrs = append(attrs, iter.Attribute())
		}
	}
	commonAttributes = attribute.NewSet(attrs...)

	// Start process memory metrics collection
	startProcessMemoryMetrics(doneCtx)

	// Configure slog level based on DEBUG environment variables
	var opts *slog.HandlerOptions
	if os.Getenv("LAKERUNNER_DEBUG") != "" {
		opts = &slog.HandlerOptions{Level: slog.LevelDebug}
	}

	var logger *slog.Logger
	if os.Getenv("OTEL_SERVICE_NAME") != "" && os.Getenv("ENABLE_OTLP_TELEMETRY") == "true" {
		slog.Info("OpenTelemetry exporting enabled")
		logger = slog.New(slogmulti.Fanout(
			slog.NewTextHandler(os.Stdout, opts),
			otelslog.NewHandler(servicename),
		)).With(
			slog.String("service", servicename),
			slog.Int64("instanceID", myInstanceID),
		)
		slog.SetDefault(logger)

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
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, opts)).With(
			slog.String("service", servicename),
			slog.Int64("instanceID", myInstanceID),
		)
		slog.SetDefault(logger)
	}

	// Attach the configured logger to the context
	doneCtx = logctx.WithLogger(doneCtx, logger)

	return doneCtx, f, nil
}

// startProcessMemoryMetrics registers observable gauges for process memory metrics
func startProcessMemoryMetrics(_ context.Context) {
	pid := int32(os.Getpid())
	proc, err := process.NewProcess(pid)
	if err != nil {
		slog.Error("failed to get process handle", "error", err)
		return
	}

	// Register RSS observable gauge
	rssGauge, err := meter.Int64ObservableGauge(
		"lakerunner.process.memory.rss",
		metric.WithDescription("Process resident set size (physical memory)"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create RSS observable gauge", "error", err)
		return
	}

	// Register VMS observable gauge
	vmsGauge, err := meter.Int64ObservableGauge(
		"lakerunner.process.memory.vms",
		metric.WithDescription("Process virtual memory size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("failed to create VMS observable gauge", "error", err)
		return
	}

	// Register a single callback for both gauges
	_, err = meter.RegisterCallback(
		func(_ context.Context, observer metric.Observer) error {
			memInfo, err := proc.MemoryInfo()
			if err != nil {
				slog.Debug("failed to get process memory info", "error", err)
				return nil // Don't propagate error to avoid breaking metrics collection
			}

			observer.ObserveInt64(rssGauge, int64(memInfo.RSS), metric.WithAttributeSet(commonAttributes))
			observer.ObserveInt64(vmsGauge, int64(memInfo.VMS), metric.WithAttributeSet(commonAttributes))

			return nil
		},
		rssGauge,
		vmsGauge,
	)
	if err != nil {
		slog.Error("failed to register memory metrics callback", "error", err)
		return
	}
}
