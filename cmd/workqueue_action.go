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
	"errors"
	"fmt"
	"log/slog"
	"math"
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
		slog.String("action", "ingest"),
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
	attrs := attribute.NewSet(
		attribute.String("signal", signal),
		attribute.String("action", action),
	)

	ctx, span := tracer.Start(ctx, "workqueueProcess",
		trace.WithAttributes(
			append(commonAttributes.ToSlice(), attrs.ToSlice()...)...))
	defer span.End()

	t0 := time.Now()
	inf, err := wqm.RequestWork()
	workqueueFetchDuration.Record(ctx, time.Since(t0).Milliseconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributeSet(attrs),
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
	workqueueLag.Record(ctx, workLag.Milliseconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributeSet(attrs),
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

	estBytesPerRecord := est.Get(inf.OrganizationID(), inf.InstanceNum(), inf.Signal()).AvgBytesPerRecord
	t0 = time.Now()
	result, err := pfx(ctx, ll, awsmanager, sp, mdb, inf, estBytesPerRecord)
	workqueueDuration.Record(ctx, time.Since(t0).Milliseconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributeSet(attrs),
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
