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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type InqueueProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager,
	inf lrdb.Inqueue,
	ingest_dateint int32,
	rpfEstimate int64,
	loop *IngestLoopContext) error

type IngestLoopContext struct {
	ctx               context.Context
	mdb               lrdb.StoreFull
	sp                storageprofile.StorageProfileProvider
	awsmanager        *awsclient.Manager
	metricEstimator   estimator.MetricEstimator
	logEstimator      estimator.LogEstimator
	signal            string
	ll                *slog.Logger
	exemplarProcessor *exemplar.Processor
}

func NewIngestLoopContext(ctx context.Context, signal string, assumeRoleSessionName string) (*IngestLoopContext, error) {
	ll := slog.Default().With(
		slog.String("signal", signal),
		slog.String("action", "ingest"),
	)

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open LRDB store: %w", err)
	}

	awsmanager, err := awsclient.NewManager(ctx, awsclient.WithAssumeRoleSessionName(assumeRoleSessionName))
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	metricEst, err := estimator.NewMetricEstimator(ctx, mdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create metric estimator: %w", err)
	}

	logEst, err := estimator.NewLogEstimator(ctx, mdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create log estimator: %w", err)
	}

	sp, err := storageprofile.SetupStorageProfiles()
	if err != nil {
		return nil, fmt.Errorf("failed to setup storage profiles: %w", err)
	}

	config := exemplar.Config{
		Metrics: exemplar.TelemetryConfig{
			Enabled:        true,
			CacheSize:      10000,
			Expiry:         15 * time.Minute,
			ReportInterval: 5 * time.Minute,
			BatchSize:      100,
		},
		Logs: exemplar.TelemetryConfig{
			Enabled: false, // Logs not implemented yet
		},
		Traces: exemplar.TelemetryConfig{
			Enabled: false, // Traces not implemented yet
		},
	}

	exemplarProcessor := exemplar.NewMetricsProcessor(config, ll)

	exemplarProcessor.SetMetricsCallback(func(ctx context.Context, organizationID string, exemplars []*exemplar.ExemplarData) error {
		return processMetricsExemplarsDirect(ctx, organizationID, exemplars, mdb)
	})

	return &IngestLoopContext{
		ctx:               ctx,
		mdb:               mdb,
		sp:                sp,
		awsmanager:        awsmanager,
		metricEstimator:   metricEst,
		logEstimator:      logEst,
		signal:            signal,
		ll:                ll,
		exemplarProcessor: exemplarProcessor,
	}, nil
}

// Close cleans up resources
func (loop *IngestLoopContext) Close() error {
	if loop.exemplarProcessor != nil {
		return loop.exemplarProcessor.Close()
	}
	return nil
}

func IngestLoop(loop *IngestLoopContext, processingFx InqueueProcessingFunction) error {
	ctx := context.Background()

	for {
		select {
		case <-loop.ctx.Done():
			return loop.ctx.Err()
		default:
		}

		t0 := time.Now()
		shouldBackoff, didWork, err := ingestFiles(ctx, loop, processingFx)
		if err != nil {
			return err
		}

		if didWork {
			loop.ll.Info("Ingested file", slog.Duration("elapsed", time.Since(t0)))
		}

		if shouldBackoff {
			select {
			case <-loop.ctx.Done():
				return nil
			case <-time.After(workSleepTime):
			}
		}

		gc()
	}
}

func ingestFiles(
	ctx context.Context,
	loop *IngestLoopContext,
	processFx InqueueProcessingFunction,
) (bool, bool, error) {
	ctx, span := tracer.Start(ctx, "ingest", trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()
	inf, err := loop.mdb.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     myInstanceID,
		TelemetryType: loop.signal,
	})
	inqueueFetchDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil && !errors.Is(err, pgx.ErrNoRows)),
			attribute.Bool("noRows", errors.Is(err, pgx.ErrNoRows)),
		))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return true, false, nil
		}
		return true, false, fmt.Errorf("failed to claim inqueue work: %w", err)
	}

	ll := loop.ll.With(
		slog.String("id", inf.ID.String()),
		slog.Int("tries", int(inf.Tries)),
		slog.String("collectorName", inf.CollectorName),
		slog.String("organizationID", inf.OrganizationID.String()),
		slog.Int("instanceNum", int(inf.InstanceNum)),
		slog.String("bucket", inf.Bucket),
		slog.String("objectID", inf.ObjectID))

	h := workqueue.NewInqueueHandlerFromLRDB(inf, loop.mdb, maxWorkRetries, workqueue.WithLogger(ll))

	if inf.Tries > 10 {
		ll.Warn("Too many tries, deleting")
		if err := h.CompleteWork(ctx); err != nil {
			ll.Error("Failed to complete work", slog.Any("error", err))
		}
		return false, true, nil
	}

	isNew, err := h.IsNewWork(ctx)
	if err != nil {
		if retryErr := h.RetryWork(ctx); retryErr != nil {
			ll.Error("Failed to retry work", slog.Any("error", retryErr))
		}
		return true, false, err
	}
	if !isNew {
		ll.Warn("already processed, releasing")
		if err := h.CompleteWork(ctx); err != nil {
			ll.Error("Failed to complete work", slog.Any("error", err))
		}
		return true, true, nil
	}

	ll.Info("Processing")

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	// Create a temporary directory for processing
	tmpdir, err := os.MkdirTemp("", "lakerunner-ingest-*")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		if retryErr := h.RetryWork(ctx); retryErr != nil {
			ll.Error("Failed to retry work", slog.Any("error", retryErr))
		}
		return true, false, fmt.Errorf("failed to create temporary directory: %w", err)
	}
	ll.Info("Created temporary directory", slog.String("path", tmpdir))
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.String("path", tmpdir), slog.Any("error", err))
		} else {
			ll.Info("Removed temporary directory", slog.String("path", tmpdir))
		}
	}()

	var rpfEstimate int64
	switch lrdb.SignalEnum(inf.TelemetryType) {
	case lrdb.SignalEnumMetrics:
		rpfEstimate = loop.metricEstimator.Get(inf.OrganizationID, inf.InstanceNum, 10_000)
	case lrdb.SignalEnumLogs:
		rpfEstimate = loop.logEstimator.Get(inf.OrganizationID, inf.InstanceNum)
	default:
		rpfEstimate = 40_000 // Default fallback
	}
	t0 = time.Now()
	err = processFx(ctx, ll, tmpdir, loop.sp, loop.mdb, loop.awsmanager, inf, ingestDateint, rpfEstimate, loop)
	inqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("organizationID", inf.OrganizationID.String()),
			attribute.String("collectorName", inf.CollectorName),
			attribute.Int("instanceNum", int(inf.InstanceNum)),
			attribute.String("bucket", inf.Bucket),
		))
	if err != nil {
		if retryErr := h.RetryWork(ctx); retryErr != nil {
			ll.Error("Failed to retry work", slog.Any("error", retryErr))
		}
		return true, false, fmt.Errorf("Processing failed: %w", err)
	}

	if err := h.CompleteWork(ctx); err != nil {
		ll.Error("Failed to complete work", slog.Any("error", err))
	}
	return false, true, nil
}
