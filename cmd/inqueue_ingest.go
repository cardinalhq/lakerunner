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
	"sync/atomic"
	"time"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type IngestLoopContext struct {
	mdb               lrdb.StoreFull
	sp                storageprofile.StorageProfileProvider
	cloudManagers     cloudstorage.ClientProvider
	metricEstimator   estimator.MetricEstimator
	logEstimator      estimator.LogEstimator
	signal            string
	exemplarProcessor *exemplar.Processor
	processedItems    *int64
	lastLogTime       *time.Time
}

func NewIngestLoopContext(ctx context.Context, signal string) (*IngestLoopContext, error) {
	ll := slog.Default().With(
		slog.String("signal", signal),
		slog.String("action", "ingest"),
	)
	ctx = logctx.WithLogger(ctx, ll)

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open LRDB store: %w", err)
	}

	cmgr, err := cloudstorage.NewCloudManagers(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create cloud managers: %w", err)
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
			Enabled:        os.Getenv("EXEMPLAR_METRICS_DISABLED") != "false",
			CacheSize:      10000,
			Expiry:         15 * time.Minute,
			ReportInterval: 5 * time.Minute,
			BatchSize:      100,
		},
		Logs: exemplar.TelemetryConfig{
			Enabled:        true,
			CacheSize:      10000,
			Expiry:         15 * time.Minute,
			ReportInterval: 5 * time.Minute,
			BatchSize:      100,
		},
		Traces: exemplar.TelemetryConfig{
			Enabled: false, // Traces not implemented yet
		},
	}

	exemplarProcessor := exemplar.NewProcessor(config)

	// Set callbacks for enabled telemetry types
	if config.Metrics.Enabled {
		exemplarProcessor.SetMetricsCallback(func(ctx context.Context, organizationID string, exemplars []*exemplar.ExemplarData) error {
			return processMetricsExemplarsDirect(ctx, organizationID, exemplars, mdb)
		})
	}

	if config.Logs.Enabled {
		exemplarProcessor.SetLogsCallback(func(ctx context.Context, organizationID string, exemplars []*exemplar.ExemplarData) error {
			return processLogsExemplarsDirect(ctx, organizationID, exemplars, mdb)
		})
	}

	var processedItems int64
	var lastLogTime time.Time

	loopCtx := &IngestLoopContext{
		mdb:               mdb,
		sp:                sp,
		cloudManagers:     cmgr,
		metricEstimator:   metricEst,
		logEstimator:      logEst,
		signal:            signal,
		exemplarProcessor: exemplarProcessor,
		processedItems:    &processedItems,
		lastLogTime:       &lastLogTime,
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

// Close cleans up resources
func (loop *IngestLoopContext) Close() error {
	if loop.exemplarProcessor != nil {
		return loop.exemplarProcessor.Close()
	}
	return nil
}
