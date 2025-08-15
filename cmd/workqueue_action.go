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
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/estimator"
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
) (WorkResult, error)

type WorkResult int

const (
	WorkResultSuccess WorkResult = iota
	WorkResultTryAgainLater
)

type RunqueueLoopContext struct {
	ctx        context.Context
	wqm        lockmgr.WorkQueueManager
	mdb        lrdb.StoreFull
	sp         storageprofile.StorageProfileProvider
	awsmanager *awsclient.Manager
	estimator  estimator.Estimator
	signal     string
	action     string
	ll         *slog.Logger
}

func NewRunqueueLoopContext(ctx context.Context, signal string, action string, assumeRoleSessionName string) (*RunqueueLoopContext, error) {
	ll := slog.Default().With(
		slog.String("signal", signal),
		slog.String("action", action),
	)

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open LRDB store: %w", err)
	}

	awsmanager, err := awsclient.NewManager(ctx, awsclient.WithAssumeRoleSessionName(assumeRoleSessionName))
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	est, err := estimator.NewEstimator(ctx, mdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create estimator: %w", err)
	}

	sp, err := storageprofile.SetupStorageProfiles()
	if err != nil {
		return nil, fmt.Errorf("failed to setup storage profiles: %w", err)
	}

	freqs, err := frequenciesToRequest(signal, action)
	if err != nil {
		return nil, fmt.Errorf("failed to get frequencies for signal %s and action %s: %w", signal, action, err)
	}

	wqm := lockmgr.NewWorkQueueManager(mdb, myInstanceID, lrdb.SignalEnum(signal), lrdb.ActionEnum(action), freqs, math.MinInt32)
	go wqm.Run(ctx)

	return &RunqueueLoopContext{
		ctx:        ctx,
		wqm:        wqm,
		mdb:        mdb,
		sp:         sp,
		awsmanager: awsmanager,
		estimator:  est,
		signal:     signal,
		action:     action,
		ll:         ll,
	}, nil
}

func RunqueueLoop(loop *RunqueueLoopContext, pfx RunqueueProcessingFunction) error {
	ctx := context.Background()

	for {
		select {
		case <-loop.ctx.Done():
			return loop.ctx.Err()
		default:
		}

		t0 := time.Now()
		shouldBackoff, didWork, err := workqueueProcess(ctx, loop, pfx)
		if err != nil {
			return err
		}

		if didWork {
			loop.ll.Info("Completed work", slog.Duration("elapsed", time.Since(t0)))
		}

		if shouldBackoff {
			select {
			case <-loop.ctx.Done():
				return loop.ctx.Err()
			case <-time.After(workSleepTime):
			}
		}

		gc()
	}
}

func frequenciesToRequest(signal, action string) ([]int32, error) {
	switch signal {
	case "logs":
		if action == "compact" {
			return []int32{-1}, nil
		}
		return nil, errors.New("unknown action for logs signal: " + action)
	case "metrics":
		switch action {
		case "compact":
			return acceptedMetricFrequencies, nil
		case "rollup":
			freqs := make([]int32, 0, len(rollupSources))
			for k := range rollupSources {
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
	ctx context.Context,
	loop *RunqueueLoopContext,
	pfx RunqueueProcessingFunction) (bool, bool, error) {

	ctx, span := tracer.Start(ctx, "workqueueProcess", trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()
	inf, err := loop.wqm.RequestWork()
	workqueueFetchDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil && !errors.Is(err, pgx.ErrNoRows)),
			attribute.Bool("errorIsNoRows", errors.Is(err, pgx.ErrNoRows)),
		))
	if err != nil || inf == nil {
		return true, false, err
	}
	defer func() {
		if err := inf.Fail(); err != nil {
			loop.ll.Error("Failed to release work item", slog.Any("error", err))
		}
	}()

	orgAttrs := attribute.NewSet(
		attribute.String("organizationID", inf.OrganizationID().String()),
		attribute.Int64("instanceNum", int64(inf.InstanceNum())),
		attribute.Int64("priority", int64(inf.Priority())),
		attribute.Int64("frequencyMs", int64(inf.FrequencyMs())),
	)

	workLag := max(time.Since(inf.RunnableAt()), 0)
	workqueueLag.Record(ctx, workLag.Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributeSet(orgAttrs),
	)

	ll := loop.ll.With(
		slog.Int64("workQueueID", inf.ID()),
		slog.Int("tries", int(inf.Tries())),
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
	)

	ll.Info("Processing work queue item",
		slog.Int("priority", int(inf.Priority())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Time("runnableAt", inf.RunnableAt()),
		slog.Duration("workLag", workLag),
	)

	tmpdir, err := os.MkdirTemp("", "lakerunner-workqueue-*")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
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

	estBytesPerRecord := loop.estimator.Get(inf.OrganizationID(), inf.InstanceNum(), inf.Signal()).EstimatedRecordCount
	t0 = time.Now()
	result, err := pfx(ctx, ll, tmpdir, loop.awsmanager, loop.sp, loop.mdb, inf, estBytesPerRecord)
	workqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributeSet(orgAttrs),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil),
			attribute.Int("result", int(result)),
		))
	switch result {
	case WorkResultSuccess:
		return false, true, inf.Complete()
	case WorkResultTryAgainLater:
		return true, false, inf.Fail()
	default:
		ll.Error("Unexpected work result", slog.Int("result", int(result)), slog.Any("error", err))
		return true, false, inf.Fail()
	}
}
