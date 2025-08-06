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
	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/pkg/lockmgr"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

type RunqueueProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	estBytesPerRecord float64,
) (WorkResult, error)

type WorkResult int

const (
	WorkResultSuccess WorkResult = iota
	WorkResultTryAgainLater
)

func RunqueueLoop(doneCtx context.Context, sp storageprofile.StorageProfileProvider, signal string, action string, assumeRoleSessionName string, pfx RunqueueProcessingFunction) error {
	ctx := context.Background()

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return err
	}

	est, err := estimator.NewEstimator(doneCtx, mdb)
	if err != nil {
		return fmt.Errorf("failed to create estimator: %w", err)
	}

	freqs, err := frequenciesToRequest(signal, action)
	if err != nil {
		return fmt.Errorf("failed to get frequencies for signal %s and action %s: %w", signal, action, err)
	}

	wqm := lockmgr.NewWorkQueueManager(mdb, myInstanceID, lrdb.SignalEnum(signal), lrdb.ActionEnum(action), freqs, math.MinInt32)
	go wqm.Run(ctx)

	awsmanager, err := awsclient.NewManager(ctx, awsclient.WithAssumeRoleSessionName(assumeRoleSessionName))
	if err != nil {
		return err
	}

	ll := slog.Default().With(
		slog.String("signal", signal),
	)

	for {
		select {
		case <-doneCtx.Done():
			return doneCtx.Err()
		default:
		}

		t0 := time.Now()
		shouldBackoff, didWork, err := workqueueProcess(ctx, slog.Default(), signal, action, sp, mdb, wqm, awsmanager, est, pfx)
		if err != nil {
			return err
		}

		if didWork {
			ll.Info("Completed work", slog.Duration("elapsed", time.Since(t0)))
		}

		if shouldBackoff {
			select {
			case <-doneCtx.Done():
				return doneCtx.Err()
			case <-time.After(workSleepTime):
			}
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
	ll *slog.Logger,
	signal string,
	action string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	wqm lockmgr.WorkQueueManager,
	awsmanager *awsclient.Manager,
	est estimator.Estimator,
	pfx RunqueueProcessingFunction) (bool, bool, error) {

	ctx, span := tracer.Start(ctx, "workqueueProcess",
		trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()
	inf, err := wqm.RequestWork()
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
			ll.Error("Failed to release work item", slog.Any("error", err))
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

	ll = ll.With(
		slog.Int64("workQueueID", inf.ID()),
		slog.Int("tries", int(inf.Tries())),
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())))

	ll.Info("Processing work queue item",
		slog.String("signal", signal),
		slog.String("action", action),
		slog.Int("priority", int(inf.Priority())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())))

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

	estBytesPerRecord := est.Get(inf.OrganizationID(), inf.InstanceNum(), inf.Signal()).AvgBytesPerRecord
	t0 = time.Now()
	result, err := pfx(ctx, ll, tmpdir, awsmanager, sp, mdb, inf, estBytesPerRecord)
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
