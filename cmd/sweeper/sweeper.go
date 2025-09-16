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
	"go.opentelemetry.io/otel/metric"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

var (
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
)

type sweeper struct {
	instanceID       int64
	sp               storageprofile.StorageProfileProvider
	syncLegacyTables bool
}

func New(instanceID int64, syncLegacyTables bool) *sweeper {
	cdb, err := dbopen.ConfigDBStore(context.Background())
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
	}
}

func (cmd *sweeper) Run(doneCtx context.Context) error {
	ctx, cancel := context.WithCancel(doneCtx)
	defer cancel()

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return err
	}

	// Always initialize cdb as it's needed for cleanup loops
	var cdb configdb.QuerierFull
	cdb, err = dbopen.ConfigDBStore(ctx)
	if err != nil {
		return err
	}

	var cdbPool *pgxpool.Pool
	if cmd.syncLegacyTables {
		cdbPool, err = dbopen.ConnectToConfigDB(ctx)
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			slog.Info("Starting legacy table sync goroutine", slog.Duration("period", legacyTablesSyncPeriod))
			if err := periodicLoop(ctx, legacyTablesSyncPeriod, func(c context.Context) error {
				return runLegacyTablesSync(c, cdb, cdbPool)
			}); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}

	// Periodic: metric estimate updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := periodicLoop(ctx, metricEstimateUpdatePeriod, func(c context.Context) error {
			return runMetricEstimateUpdate(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Periodic: log estimate updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := periodicLoop(ctx, metricEstimateUpdatePeriod, func(c context.Context) error {
			return runLogEstimateUpdate(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Periodic: trace estimate updates
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := periodicLoop(ctx, metricEstimateUpdatePeriod, func(c context.Context) error {
			return runTraceEstimateUpdate(c, mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Metric segment cleanup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runScheduledCleanupLoop(ctx, cmd.sp, mdb, cdb, cmgr, "metric"); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Log segment cleanup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runScheduledCleanupLoop(ctx, cmd.sp, mdb, cdb, cmgr, "log"); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Trace segment cleanup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := runScheduledCleanupLoop(ctx, cmd.sp, mdb, cdb, cmgr, "trace"); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

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

// Runs f immediately, then on a ticker every period. Never more than once per period.
func periodicLoop(ctx context.Context, period time.Duration, f func(context.Context) error) error {
	if err := f(ctx); err != nil {
		slog.Error("periodic task error", slog.Any("error", err))
	}

	t := time.NewTicker(period)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := f(ctx); err != nil {
				slog.Error("periodic task error", slog.Any("error", err))
				// keep going; periodic tasks should be resilient
			}
		}
	}
}
