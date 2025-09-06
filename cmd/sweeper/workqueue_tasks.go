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

package sweeper

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const (
	gcBatchLimit int32         = 1000
	gcBatchDelay time.Duration = 500 * time.Millisecond
	gcPeriod     time.Duration = time.Hour
	gcCutoffAge  time.Duration = 10 * 24 * time.Hour
)

func runWorkqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Calculate stale expiration based on heartbeat interval
	// Items are considered stale after missing the configured number of heartbeats
	staleExpirationTime := time.Duration(workqueue.StaleExpiryMultiplier) * workqueue.DefaultHeartbeatInterval
	expired, err := mdb.WorkQueueCleanup(ctx, staleExpirationTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire objects", slog.Any("error", err))
		return err
	}
	for _, obj := range expired {
		ll.Info("Expired work/lock", slog.Any("work", obj))
		workQueueExpiryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("signal", string(obj.Signal)),
			attribute.String("action", string(obj.Action)),
		))
	}

	// Clean up orphaned signal locks
	deleted, err := mdb.WorkQueueOrphanedSignalLockCleanup(ctx, 1000)
	if err != nil {
		ll.Error("Failed to cleanup orphaned signal locks", slog.Any("error", err))
		return err
	}
	signalLockCleanupCounter.Add(ctx, int64(deleted))
	if deleted > 0 {
		ll.Info("Cleaned up orphaned signal locks", slog.Int("deleted", int(deleted)))
	}

	return nil
}

func runInqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// TODO: Replace with Kafka consumer lag monitoring
	return nil
}

func runMCQExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Calculate cutoff time for MCQ items based on heartbeat logic
	// Items are considered stale if they haven't heartbeated for 5 minutes
	// This allows for ~5 missed heartbeats (1 minute interval) plus buffer
	mcqStaleTimeout := 5 * time.Minute
	cutoffTime := time.Now().Add(-mcqStaleTimeout)

	expired, err := mdb.McqCleanupExpired(ctx, &cutoffTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire MCQ objects", slog.Any("error", err))
		return err
	}
	for _, obj := range expired {
		ll.Info("Expired MCQ item",
			slog.Int64("id", obj.ID),
			slog.String("organization_id", obj.OrganizationID.String()),
			slog.Int("dateint", int(obj.Dateint)),
			slog.Int("frequency_ms", int(obj.FrequencyMs)))
		mcqExpiryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
	}
	return nil
}

func runMRQExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Calculate cutoff time for MRQ items based on heartbeat logic
	// Items are considered stale if they haven't heartbeated for 5 minutes
	// This allows for ~5 missed heartbeats (1 minute interval) plus buffer
	mrqStaleTimeout := 5 * time.Minute

	count, err := mdb.MrqReclaimTimeouts(ctx, lrdb.MrqReclaimTimeoutsParams{
		MaxAge:  mrqStaleTimeout,
		MaxRows: 1000, // Reasonable batch size
	})
	if err != nil {
		ll.Error("Failed to reclaim MRQ timeouts", slog.Any("error", err))
		return err
	}
	if count > 0 {
		ll.Info("Reclaimed MRQ timeout items", slog.Int64("count", count))
		mrqExpiryCounter.Add(ctx, count)
	}
	return nil
}

func workqueueGCLoop(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	runOnce := func() {
		cutoff := time.Now().Add(-gcCutoffAge).UTC()

		for {
			if ctx.Err() != nil {
				return
			}

			deleted, err := mdb.WorkQueueGC(ctx, lrdb.WorkQueueGCParams{
				Cutoff:  cutoff,
				Maxrows: gcBatchLimit,
			})
			if err != nil {
				ll.Error("WorkQueueGC failed", slog.Any("error", err))
				return
			}

			if deleted == 0 {
				return
			}

			ll.Info("WorkQueueGC deleted rows", slog.Int("deleted", int(deleted)))

			if deleted < gcBatchLimit {
				return
			}

			if sleepCtx(ctx, gcBatchDelay) {
				return
			}
		}
	}

	runOnce()

	t := time.NewTicker(gcPeriod)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			runOnce()
		}
	}
}
