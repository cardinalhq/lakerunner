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

type InqueueBatchProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager,
	items []lrdb.Inqueue,
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

	cdb, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

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
	return IngestLoopWithBatch(loop, processingFx, nil)
}

func IngestLoopWithBatch(loop *IngestLoopContext, processingFx InqueueProcessingFunction, batchProcessingFx InqueueBatchProcessingFunction) error {
	ctx := context.Background()

	for {
		select {
		case <-loop.ctx.Done():
			return loop.ctx.Err()
		default:
		}

		t0 := time.Now()
		shouldBackoff, didWork, err := ingestFilesBatch(ctx, loop, processingFx, batchProcessingFx)
		if err != nil {
			return err
		}

		if didWork {
			loop.ll.Info("Ingested file(s)", slog.Duration("elapsed", time.Since(t0)))
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

// batchRowToInqueue converts ClaimInqueueWorkBatchRow to Inqueue (they have identical fields)
func batchRowToInqueue(row lrdb.ClaimInqueueWorkBatchRow) lrdb.Inqueue {
	return lrdb.Inqueue(row)
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

func ingestFilesBatch(
	ctx context.Context,
	loop *IngestLoopContext,
	processFx InqueueProcessingFunction,
	batchProcessingFx InqueueBatchProcessingFunction,
) (bool, bool, error) {
	batchSize := helpers.GetBatchSizeForSignal(loop.signal)

	if batchSize <= 1 || batchProcessingFx == nil {
		return ingestFiles(ctx, loop, processFx)
	}

	ctx, span := tracer.Start(ctx, "ingest_batch", trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()

	// First, find next available work to get org/instance
	nextWork, err := loop.mdb.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     myInstanceID,
		TelemetryType: loop.signal,
	})
	if err != nil {
		return true, false, fmt.Errorf("failed to find next work: %w", err)
	}

	// Temporarily release it back so batch claim can get it
	if err := loop.mdb.ReleaseInqueueWork(ctx, lrdb.ReleaseInqueueWorkParams{
		ID:        nextWork.ID,
		ClaimedBy: myInstanceID,
	}); err != nil {
		return true, false, fmt.Errorf("failed to release work: %w", err)
	}

	// Now claim batch from same org/instance with new parameters
	items, err := loop.mdb.ClaimInqueueWorkBatch(ctx, lrdb.ClaimInqueueWorkBatchParams{
		OrganizationID: nextWork.OrganizationID,
		InstanceNum:    nextWork.InstanceNum,
		TelemetryType:  loop.signal,
		WorkerID:       myInstanceID,
		MaxTotalSize:   helpers.GetMaxTotalSize(),
		MinTotalSize:   helpers.GetMinBatchSize(),
		MaxAgeSeconds:  int32(helpers.GetMaxAgeSeconds()),
		BatchCount:     int32(batchSize),
	})
	inqueueFetchDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil),
			attribute.Bool("noRows", len(items) == 0),
		))

	if err != nil {
		return true, false, fmt.Errorf("failed to claim batch inqueue work: %w", err)
	}

	if len(items) == 0 {
		return true, false, nil
	}

	if len(items) == 1 {
		return processSingleItem(ctx, loop, processFx, batchRowToInqueue(items[0]))
	}

	ll := loop.ll.With(
		slog.Int("batchSize", len(items)),
		slog.String("organizationID", items[0].OrganizationID.String()),
		slog.Int("instanceNum", int(items[0].InstanceNum)))

	handlers := make([]*workqueue.InqueueHandler, len(items))
	for i, item := range items {
		handlers[i] = workqueue.NewInqueueHandlerFromLRDB(batchRowToInqueue(item), loop.mdb, maxWorkRetries, workqueue.WithLogger(ll))

		if item.Tries > 10 {
			ll.Warn("Too many tries, deleting item", slog.String("itemID", item.ID.String()))
			if err := handlers[i].CompleteWork(ctx); err != nil {
				ll.Error("Failed to complete work for item with too many tries", slog.Any("error", err))
			}
			continue
		}
	}

	tmpdir, err := os.MkdirTemp("", "lakerunner-batch-ingest-")
	if err != nil {
		return true, false, fmt.Errorf("creating tmpdir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to clean up tmpdir", slog.String("tmpdir", tmpdir), slog.Any("error", err))
		}
	}()

	var rpfEstimate int64
	switch lrdb.SignalEnum(items[0].TelemetryType) {
	case lrdb.SignalEnumMetrics:
		rpfEstimate = loop.metricEstimator.Get(items[0].OrganizationID, items[0].InstanceNum, 10_000)
	case lrdb.SignalEnumLogs:
		rpfEstimate = loop.logEstimator.Get(items[0].OrganizationID, items[0].InstanceNum)
	default:
		rpfEstimate = 40_000
	}

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	t0 = time.Now()
	inqueueBatch := make([]lrdb.Inqueue, len(items))
	for i, item := range items {
		inqueueBatch[i] = batchRowToInqueue(item)
	}
	err = batchProcessingFx(ctx, ll, tmpdir, loop.sp, loop.mdb, loop.awsmanager, inqueueBatch, ingestDateint, rpfEstimate, loop)
	inqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("organizationID", items[0].OrganizationID.String()),
			attribute.String("bucket", items[0].Bucket),
			attribute.Int("batchSize", len(items)),
		))

	if err != nil {
		for _, h := range handlers {
			if retryErr := h.RetryWork(ctx); retryErr != nil {
				ll.Error("Failed to retry work", slog.Any("error", retryErr))
			}
		}
		return true, false, fmt.Errorf("Batch processing failed: %w", err)
	}

	for _, h := range handlers {
		if err := h.CompleteWork(ctx); err != nil {
			ll.Error("Failed to complete work", slog.Any("error", err))
		}
	}
	return false, true, nil
}

func processSingleItem(ctx context.Context, loop *IngestLoopContext, processFx InqueueProcessingFunction, inf lrdb.Inqueue) (bool, bool, error) {
	ll := loop.ll.With(
		slog.String("id", inf.ID.String()),
		slog.Int("tries", int(inf.Tries)),
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
		return true, false, fmt.Errorf("checking if work is new: %w", err)
	}
	if !isNew {
		ll.Info("Already processed file, skipping")
		if err := h.CompleteWork(ctx); err != nil {
			ll.Error("Failed to complete work", slog.Any("error", err))
		}
		return false, true, nil
	}

	tmpdir, err := os.MkdirTemp("", "lakerunner-ingest-")
	if err != nil {
		return true, false, fmt.Errorf("creating tmpdir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to clean up tmpdir", slog.String("tmpdir", tmpdir), slog.Any("error", err))
		}
	}()

	var rpfEstimate int64
	switch lrdb.SignalEnum(inf.TelemetryType) {
	case lrdb.SignalEnumMetrics:
		rpfEstimate = loop.metricEstimator.Get(inf.OrganizationID, inf.InstanceNum, 10_000)
	case lrdb.SignalEnumLogs:
		rpfEstimate = loop.logEstimator.Get(inf.OrganizationID, inf.InstanceNum)
	default:
		rpfEstimate = 40_000
	}

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	t0 := time.Now()
	err = processFx(ctx, ll, tmpdir, loop.sp, loop.mdb, loop.awsmanager, inf, ingestDateint, rpfEstimate, loop)
	inqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("organizationID", inf.OrganizationID.String()),
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
