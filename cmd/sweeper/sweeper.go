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
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	tracer                  = otel.Tracer("github.com/cardinalhq/lakerunner/cmd/sweeper")
	legacyTableSyncCounter  metric.Int64Counter
	legacyTableSyncDuration metric.Float64Histogram
	metricEstimateCounter   metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/cmd/sweeper")

	var err error
	legacyTableSyncCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.legacy_table_sync_total",
		metric.WithDescription("Count of legacy table synchronization runs"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create legacy_table_sync_total counter: %w", err))
	}

	legacyTableSyncDuration, err = meter.Float64Histogram(
		"lakerunner.sweeper.legacy_table_sync_duration_seconds",
		metric.WithDescription("Duration of legacy table synchronization runs in seconds"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create legacy_table_sync_duration_seconds histogram: %w", err))
	}

	metricEstimateCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.metric_estimate_update_total",
		metric.WithDescription("Count of metric estimate updates processed"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create metric_estimate_update_total counter: %w", err))
	}
}

const (
	legacyTablesSyncPeriod     time.Duration = 5 * time.Minute
	metricEstimateUpdatePeriod time.Duration = 10 * time.Minute
	pubsubCleanupPeriod        time.Duration = 120 * time.Minute
	workQueueCleanupPeriod     time.Duration = 2 * time.Minute
	workQueueHeartbeatTimeout  time.Duration = 5 * time.Minute
)

type sweeper struct {
	instanceID       int64
	sp               storageprofile.StorageProfileProvider
	syncLegacyTables bool
	cfg              *config.Config
}

func New(instanceID int64, syncLegacyTables bool, cfg *config.Config) *sweeper {
	cdb, err := configdb.ConfigDBStore(context.Background())
	if err != nil {
		slog.Error("Failed to connect to configdb", slog.Any("error", err))
		os.Exit(1)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	// Check environment variable if flag is not set
	if !syncLegacyTables {
		if envVal, exists := os.LookupEnv("SYNC_LEGACY_TABLES"); exists {
			if parsed, err := strconv.ParseBool(envVal); err == nil {
				syncLegacyTables = parsed
			}
		}
	}

	return &sweeper{
		instanceID:       instanceID,
		sp:               sp,
		syncLegacyTables: syncLegacyTables,
		cfg:              cfg,
	}
}

func (cmd *sweeper) Run(doneCtx context.Context) error {
	ctx, cancel := context.WithCancel(doneCtx)
	defer cancel()

	mdb, err := lrdb.LRDBStore(ctx)
	if err != nil {
		return err
	}

	// Always initialize cdb as it's needed for cleanup loops
	var cdb configdb.QuerierFull
	cdb, err = configdb.ConfigDBStore(ctx)
	if err != nil {
		return err
	}

	configservice.NewGlobal(cdb, 5*time.Minute)

	var cdbPool *pgxpool.Pool
	if cmd.syncLegacyTables {
		cdbPool, err = configdb.ConnectToConfigDB(ctx)
		if err != nil {
			return err
		}
	}

	cmgr, err := cloudstorage.NewCloudManagers(ctx)
	if err != nil {
		return err
	}

	slog.Info("Starting sweeper",
		slog.Int64("instanceID", cmd.instanceID),
		slog.Bool("syncLegacyTables", cmd.syncLegacyTables))

	var wg sync.WaitGroup
	errCh := make(chan error, 10)

	// Periodic: legacy table sync if enabled
	if cmd.syncLegacyTables {
		wg.Go(func() {
			slog.Info("Starting legacy table sync goroutine", slog.Duration("period", legacyTablesSyncPeriod))
			if err := periodicLoop(ctx, "legacy_table_sync", legacyTablesSyncPeriod, func(c context.Context) error {
				return runLegacyTablesSync(c, cdbPool)
			}); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		})
	}

	// Periodic: metric estimate updates
	wg.Go(func() {
		if err := periodicLoop(ctx, "metric_estimate_update", metricEstimateUpdatePeriod, func(c context.Context) error {
			return runMetricEstimateUpdate(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Periodic: log estimate updates
	wg.Go(func() {
		if err := periodicLoop(ctx, "log_estimate_update", metricEstimateUpdatePeriod, func(c context.Context) error {
			return runLogEstimateUpdate(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Periodic: trace estimate updates
	wg.Go(func() {
		if err := periodicLoop(ctx, "trace_estimate_update", metricEstimateUpdatePeriod, func(c context.Context) error {
			return runTraceEstimateUpdate(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Metric segment cleanup
	wg.Go(func() {
		if err := runScheduledCleanupLoop(ctx, cmd.sp, mdb, cdb, cmgr, "metric"); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Log segment cleanup
	wg.Go(func() {
		if err := runScheduledCleanupLoop(ctx, cmd.sp, mdb, cdb, cmgr, "log"); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Trace segment cleanup
	wg.Go(func() {
		if err := runScheduledCleanupLoop(ctx, cmd.sp, mdb, cdb, cmgr, "trace"); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// PubSub message history cleanup
	wg.Go(func() {
		slog.Info("Starting PubSub history cleanup goroutine", slog.Duration("period", pubsubCleanupPeriod))
		if err := periodicLoop(ctx, "pubsub_cleanup", pubsubCleanupPeriod, func(c context.Context) error {
			return runPubSubHistoryCleanup(c, mdb, cmd.cfg)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Data expiry cleanup
	wg.Go(func() {
		slog.Info("Starting data expiry cleanup goroutine", slog.Duration("period", expiryCleanupPeriod))
		if err := periodicLoop(ctx, "expiry_cleanup", expiryCleanupPeriod, func(c context.Context) error {
			return runExpiryCleanup(c, cdb, mdb, cmd.cfg)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Work queue cleanup (releases work from dead workers)
	wg.Go(func() {
		slog.Info("Starting work queue cleanup goroutine",
			slog.Duration("period", workQueueCleanupPeriod),
			slog.Duration("heartbeatTimeout", workQueueHeartbeatTimeout))
		if err := periodicLoop(ctx, "work_queue_cleanup", workQueueCleanupPeriod, func(c context.Context) error {
			return runWorkQueueCleanup(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	})

	// Wait for cancellation or the first hard error
	select {
	case <-ctx.Done():
		// graceful shutdown
	case err := <-errCh:
		cancel()
		wg.Wait()
		return err
	}
	wg.Wait()
	return ctx.Err()
}

// periodicLoop runs f immediately, then on a ticker every period. Never more than once per period.
func periodicLoop(ctx context.Context, taskName string, period time.Duration, f func(context.Context) error) error {
	runTask := func() {
		ctx, span := tracer.Start(ctx, "sweeper."+taskName, trace.WithAttributes(
			attribute.String("task", taskName),
		))
		start := time.Now()

		if err := f(ctx); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "task failed")
			slog.Error("periodic task error", slog.String("task", taskName), slog.Any("error", err))
		}

		span.SetAttributes(attribute.Float64("duration_seconds", time.Since(start).Seconds()))
		span.End()
	}

	// Run immediately
	runTask()

	t := time.NewTicker(period)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			runTask()
		}
	}
}
