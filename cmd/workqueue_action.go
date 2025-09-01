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
	"math"
	"os"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type RunqueueProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	args any,
) error

// Legacy WorkResult enum - kept for compatibility with old implementations
type WorkResult int

const (
	WorkResultSuccess WorkResult = iota
	WorkResultTryAgainLater
	WorkResultInterrupted
)

// Legacy function type for old implementations
type LegacyRunqueueProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	args any,
) (WorkResult, error)

type RunqueueLoopContext struct {
	ctx             context.Context
	wqm             lockmgr.WorkQueueManager
	mdb             lrdb.StoreFull
	sp              storageprofile.StorageProfileProvider
	awsmanager      *awsclient.Manager
	metricEstimator estimator.MetricEstimator
	logEstimator    estimator.LogEstimator
	traceEstimator  estimator.TraceEstimator
	signal          string
	action          string
	ll              *slog.Logger
	processedItems  *int64
	lastLogTime     *time.Time
}

func NewRunqueueLoopContext(ctx context.Context, signal string, action string) (*RunqueueLoopContext, error) {
	ll := slog.Default().With(
		slog.String("signal", signal),
		slog.String("action", action),
	)

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open LRDB store: %w", err)
	}

	awsmanager, err := awsclient.NewManager(ctx)
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

	traceEst, err := estimator.NewTraceEstimator(ctx, mdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace estimator: %w", err)
	}

	cdb, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	freqs, err := frequenciesToRequest(signal, action)
	if err != nil {
		return nil, fmt.Errorf("failed to get frequencies for signal %s and action %s: %w", signal, action, err)
	}

	wqm := lockmgr.NewWorkQueueManager(mdb, myInstanceID, lrdb.SignalEnum(signal), lrdb.ActionEnum(action), freqs, math.MinInt32)
	go wqm.Run(ctx)

	var processedItems int64
	var lastLogTime time.Time

	loopCtx := &RunqueueLoopContext{
		ctx:             ctx,
		wqm:             wqm,
		mdb:             mdb,
		sp:              sp,
		awsmanager:      awsmanager,
		metricEstimator: metricEst,
		logEstimator:    logEst,
		traceEstimator:  traceEst,
		signal:          signal,
		action:          action,
		ll:              ll,
		processedItems:  &processedItems,
		lastLogTime:     &lastLogTime,
	}

	// Start periodic activity logging
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		var totalProcessed int64
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				processedCount := atomic.SwapInt64(loopCtx.processedItems, 0)
				if processedCount > 0 && time.Since(*loopCtx.lastLogTime) >= 20*time.Second {
					totalProcessed += processedCount
					ll.Info("Processing activity", slog.Int64("itemsProcessed", processedCount), slog.Int64("totalProcessed", totalProcessed))
					*loopCtx.lastLogTime = time.Now()
				}
			}
		}
	}()

	return loopCtx, nil
}

func RunqueueLoop(loop *RunqueueLoopContext, pfx RunqueueProcessingFunction, args any) error {
	// Two contexts: shutdown context for work processing, no-cancel context for work item management
	shutdownCtx := loop.ctx                        // Cancelled on SIGTERM/SIGINT
	workMgmtCtx := context.WithoutCancel(loop.ctx) // Never cancelled, allows work item cleanup

	// Set up graceful shutdown handler
	defer func() {
		slog.Info("Waiting for outstanding work to complete")
		shutdownTimeout := 30 * time.Second
		waitCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := loop.wqm.WaitForOutstandingWork(waitCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				slog.Warn("Timeout waiting for outstanding work to complete", slog.Duration("timeout", shutdownTimeout))
			} else {
				slog.Error("Error waiting for outstanding work", slog.Any("error", err))
			}
		} else {
			slog.Info("All outstanding work completed")
		}
	}()

	for {
		// Check for shutdown signal
		select {
		case <-shutdownCtx.Done():
			slog.Info("Shutdown signal received, stopping work requests")
			return nil
		default:
		}

		err := workqueueProcess(shutdownCtx, workMgmtCtx, loop, pfx, args)
		if err != nil {
			// Check if this is a worker interruption
			if IsWorkerInterrupted(err) {
				slog.Info("Worker loop interrupted gracefully")
				return nil
			}
			// Regular error - log and continue polling
			slog.Error("Work processing failed, continuing to poll", slog.Any("error", err))
		}

		gc()

		// Poll for work again in 2 seconds, exiting if cancelled
		select {
		case <-shutdownCtx.Done():
			slog.Info("Shutdown signal received during polling wait")
			return nil
		case <-time.After(2 * time.Second):
		}

	}
}

func frequenciesToRequest(signal, action string) ([]int32, error) {
	switch signal {
	case "logs":
		if action == "compact" {
			return []int32{-1}, nil
		}
		return nil, errors.New("unknown action for logs signal: " + action)
	case "traces":
		if action == "compact" {
			return []int32{-1}, nil
		}
		return nil, errors.New("unknown action for traces signal: " + action)
	case "metrics":
		switch action {
		case "compact":
			return helpers.AcceptedMetricFrequencies, nil
		case "rollup":
			freqs := make([]int32, 0, len(helpers.RollupSources))
			for k := range helpers.RollupSources {
				freqs = append(freqs, k)
			}
			return freqs, nil
		}
		return nil, errors.New("unknown action for metrics signal: " + action)
	default:
		return nil, errors.New("unknown signal type: " + signal)
	}
}

func workqueueProcess(
	workerCtx context.Context,
	workMgmtCtx context.Context,
	loop *RunqueueLoopContext,
	pfx RunqueueProcessingFunction,
	args any) error {

	ctx, span := tracer.Start(workerCtx, "workqueueProcess", trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()
	inf, err := loop.wqm.RequestWork(workMgmtCtx)
	workqueueFetchDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil && !errors.Is(err, pgx.ErrNoRows)),
			attribute.Bool("errorIsNoRows", errors.Is(err, pgx.ErrNoRows)),
		))
	if err != nil || inf == nil {
		return err
	}

	workLag := max(time.Since(inf.RunnableAt()), 0)
	workqueueLag.Record(ctx, workLag.Seconds(),
		metric.WithAttributeSet(commonAttributes),
	)

	ll := loop.ll.With(
		slog.Int64("workQueueID", inf.ID()),
		slog.Int("tries", int(inf.Tries())),
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
	)

	ll.Info("Starting work queue item",
		slog.Int("priority", int(inf.Priority())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Time("runnableAt", inf.RunnableAt()),
		slog.Duration("workLag", workLag),
	)

	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		if failErr := inf.Fail(); failErr != nil {
			ll.Error("Failed to release work item after temp dir error", slog.Any("error", failErr))
		}
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.Any("error", err))
		}
	}()

	var recordsPerFile int64
	switch inf.Signal() {
	case lrdb.SignalEnumMetrics:
		recordsPerFile = loop.metricEstimator.Get(inf.OrganizationID(), inf.InstanceNum(), inf.FrequencyMs())
	case lrdb.SignalEnumLogs:
		recordsPerFile = loop.logEstimator.Get(inf.OrganizationID(), inf.InstanceNum())
	case lrdb.SignalEnumTraces:
		recordsPerFile = loop.traceEstimator.Get(inf.OrganizationID(), inf.InstanceNum())
	default:
	}
	if recordsPerFile <= 0 {
		recordsPerFile = 40_000 // Default for all signals
	}
	t0 = time.Now()
	err = pfx(workerCtx, ll, tmpdir, loop.awsmanager, loop.sp, loop.mdb, inf, recordsPerFile, args)
	workqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil),
		))

	if err != nil {
		// Check for worker interruption - release without penalty
		if IsWorkerInterrupted(err) {
			ll.Info("Work interrupted gracefully, releasing work item without penalty")
			if failErr := inf.Fail(); failErr != nil {
				ll.Error("Failed to release interrupted work item", slog.Any("error", failErr))
			}
			return err
		}

		// Regular error - fail with penalty for retry
		ll.Error("Work failed, will retry later", slog.Any("error", err))
		if failErr := inf.Fail(); failErr != nil {
			ll.Error("Failed to release work item after error", slog.Any("error", failErr))
		}
		return err
	}

	// Success - complete the work item
	if completeErr := inf.Complete(); completeErr != nil {
		return completeErr
	}
	atomic.AddInt64(loop.processedItems, 1)
	return nil
}
