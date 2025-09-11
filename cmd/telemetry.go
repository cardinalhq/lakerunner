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

	//nolint:unused
	existsGauge metric.Int64Gauge
)

func setupTelemetry(servicename string, addlAttrs *attribute.Set) (context.Context, func() error, error) {
	myInstanceID = idgen.DefaultFlakeGenerator.NextID()

	// Catch signals to stop the process as gracefully as possible.
	doneCtx, doneCancel := handleSignals(context.Background())

	f := func() error {
		return nil
	}

	// make all the counters, gauges, etc that everyone is likely to use.
	setupGlobalMetrics()

	attrs := []attribute.KeyValue{}
	if addlAttrs != nil {
		iter := addlAttrs.Iter()
		for iter.Next() {
			attrs = append(attrs, iter.Attribute())
		}
	}
	commonAttributes = attribute.NewSet(attrs...)

	// Configure slog level based on DEBUG environment variables
	var opts *slog.HandlerOptions
	if os.Getenv("DEBUG") != "" || os.Getenv("LAKERUNNER_DEBUG") != "" {
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
		// Configure slog even when OTEL is disabled
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

func setupGlobalMetrics() {
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
