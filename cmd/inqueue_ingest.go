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

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/heartbeat"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// InqueueHeartbeatStore defines the minimal interface needed for inqueue heartbeat operations
type InqueueHeartbeatStore interface {
	TouchInqueueWork(ctx context.Context, params lrdb.TouchInqueueWorkParams) error
}

// newInqueueHeartbeater creates a new heartbeater for the given claimed inqueue items
func newInqueueHeartbeater(db InqueueHeartbeatStore, workerID int64, items []uuid.UUID) *heartbeat.Heartbeater {
	if len(items) == 0 {
		// Return a no-op heartbeater for empty items
		return heartbeat.New(func(ctx context.Context) error {
			return nil // No-op
		}, time.Minute, slog.Default().With("component", "inqueue_heartbeater", "worker_id", workerID, "item_count", 0))
	}

	heartbeatFunc := func(ctx context.Context) error {
		return db.TouchInqueueWork(ctx, lrdb.TouchInqueueWorkParams{
			Ids:       items,
			ClaimedBy: workerID,
		})
	}

	logger := slog.Default().With("component", "inqueue_heartbeater", "worker_id", workerID, "item_count", len(items))
	return heartbeat.New(heartbeatFunc, time.Minute, logger)
}

type InqueueBatchProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	cloudManagers *cloudstorage.CloudManagers,
	items []lrdb.Inqueue,
	ingest_dateint int32,
	rpfEstimate int64,
	loop *IngestLoopContext) error

type IngestLoopContext struct {
	ctx               context.Context
	mdb               lrdb.StoreFull
	sp                storageprofile.StorageProfileProvider
	cloudManagers     *cloudstorage.CloudManagers
	metricEstimator   estimator.MetricEstimator
	logEstimator      estimator.LogEstimator
	signal            string
	ll                *slog.Logger
	exemplarProcessor *exemplar.Processor
	processedItems    *int64
	lastLogTime       *time.Time
}

func NewIngestLoopContext(ctx context.Context, signal string) (*IngestLoopContext, error) {
	ll := slog.Default().With(
		slog.String("signal", signal),
		slog.String("action", "ingest"),
	)

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open LRDB store: %w", err)
	}

	cloudManagers, err := cloudstorage.NewCloudManagers(ctx)
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

	var processedItems int64
	var lastLogTime time.Time

	loopCtx := &IngestLoopContext{
		ctx:               ctx,
		mdb:               mdb,
		sp:                sp,
		cloudManagers:     cloudManagers,
		metricEstimator:   metricEst,
		logEstimator:      logEst,
		signal:            signal,
		ll:                ll,
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

func IngestLoopWithBatch(loop *IngestLoopContext, _ interface{}, batchProcessingFx InqueueBatchProcessingFunction) error {
	ctx := context.Background()

	for {
		select {
		case <-loop.ctx.Done():
			return loop.ctx.Err()
		default:
		}

		shouldBackoff, workWasProcessed, err := ingestFilesBatch(ctx, loop, batchProcessingFx)
		if err != nil {
			// Check if this is a worker interruption
			if IsWorkerInterrupted(err) {
				slog.Info("Ingest loop interrupted gracefully")
				return loop.ctx.Err() // Return the original context cancellation error
			}
			return err
		}

		// Only backoff if no work was available - if work was processed, poll immediately
		if shouldBackoff {
			select {
			case <-loop.ctx.Done():
				return nil
			case <-time.After(workSleepTime):
			}
		} else if workWasProcessed {
			// Work was processed successfully - poll immediately for more
			// Just check context and continue loop without delay
			select {
			case <-loop.ctx.Done():
				return nil
			default:
			}
		}
	}
}

func ingestFilesBatch(
	ctx context.Context,
	loop *IngestLoopContext,
	batchProcessingFx InqueueBatchProcessingFunction,
) (bool, bool, error) {
	ctx, span := tracer.Start(ctx, "ingest_batch", trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()

	// Use new bundle claiming with lock
	bundle, err := loop.mdb.ClaimInqueueBundleWithLock(ctx, lrdb.InqueueBundleParams{
		WorkerID:      myInstanceID,
		Signal:        loop.signal,
		TargetSize:    helpers.GetMinBatchSize(),
		GracePeriod:   time.Duration(helpers.GetMaxAgeSeconds()) * time.Second,
		DeferInterval: 10 * time.Second,
		Jitter:        5 * time.Second,
		MaxAttempts:   5,
		MaxCandidates: int32(helpers.GetBatchSizeForSignal(loop.signal)),
	})
	inqueueFetchDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.Bool("hasError", err != nil),
			attribute.Bool("noRows", len(bundle.Items) == 0),
		))

	if err != nil {
		return true, false, fmt.Errorf("failed to claim batch inqueue work: %w", err)
	}

	items := bundle.Items
	if len(items) == 0 {
		return true, false, nil
	}

	for _, item := range items {
		lag := time.Since(item.QueueTs).Seconds()
		inqueueLag.Record(ctx, lag,
			metric.WithAttributeSet(commonAttributes),
			metric.WithAttributes(
				attribute.String("signal", item.Signal),
			))
	}

	// Safety check: ensure all records in the batch are for the same organization
	if err := validateBatchOrganizationConsistency(items); err != nil {
		return true, false, err
	}

	ll := loop.ll.With(
		slog.Int("batchSize", len(items)),
		slog.String("organizationID", items[0].OrganizationID.String()),
		slog.Int("instanceNum", int(items[0].InstanceNum)))

	handlers := make([]*workqueue.InqueueHandler, len(items))
	var newWorkItems []lrdb.InqueueGetBundleItemsRow
	var newWorkHandlers []*workqueue.InqueueHandler

	for i, item := range items {
		// Convert to Inqueue type for handler
		inqItem := lrdb.Inqueue{
			ID:             item.ID,
			QueueTs:        item.QueueTs,
			Priority:       item.Priority,
			OrganizationID: item.OrganizationID,
			CollectorName:  item.CollectorName,
			InstanceNum:    item.InstanceNum,
			Bucket:         item.Bucket,
			ObjectID:       item.ObjectID,
			Signal:         item.Signal,
			Tries:          item.Tries,
			ClaimedBy:      item.ClaimedBy,
			ClaimedAt:      item.ClaimedAt,
			HeartbeatedAt:  item.HeartbeatedAt,
			FileSize:       item.FileSize,
		}
		handlers[i] = workqueue.NewInqueueHandlerFromLRDB(inqItem, loop.mdb, maxWorkRetries, workqueue.WithLogger(ll))

		if item.Tries > 10 {
			ll.Warn("Too many tries, deleting item", slog.String("itemID", item.ID.String()))
			if err := handlers[i].CompleteWork(ctx); err != nil {
				ll.Error("Failed to complete work for item with too many tries", slog.Any("error", err))
			}
			continue
		}

		// Check if this work is new (not already processed)
		isNew, err := handlers[i].IsNewWork(ctx)
		if err != nil {
			ll.Error("Failed to check if work is new", slog.Any("error", err), slog.String("itemID", item.ID.String()))
			if retryErr := handlers[i].RetryWork(ctx); retryErr != nil {
				ll.Error("Failed to retry work", slog.Any("error", retryErr))
			}
			continue
		}

		if !isNew {
			ll.Info("Already processed file, skipping", slog.String("itemID", item.ID.String()))
			if err := handlers[i].CompleteWork(ctx); err != nil {
				ll.Error("Failed to complete work for already processed item", slog.Any("error", err))
			}
			continue
		}

		// This item is new work, add to processing lists
		newWorkItems = append(newWorkItems, item)
		newWorkHandlers = append(newWorkHandlers, handlers[i])
	}

	// If no new work remains, we're done
	if len(newWorkItems) == 0 {
		ll.Info("No new work items in batch, all were already processed or had too many tries")
		return false, true, nil
	}

	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		return true, false, fmt.Errorf("creating tmpdir: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to clean up tmpdir", slog.Any("error", err))
		}
	}()

	var rpfEstimate int64
	switch lrdb.SignalEnum(newWorkItems[0].Signal) {
	case lrdb.SignalEnumMetrics:
		rpfEstimate = loop.metricEstimator.Get(newWorkItems[0].OrganizationID, newWorkItems[0].InstanceNum, 10_000)
	case lrdb.SignalEnumLogs:
		rpfEstimate = loop.logEstimator.Get(newWorkItems[0].OrganizationID, newWorkItems[0].InstanceNum)
	default:
		rpfEstimate = 40_000
	}

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	// Start heartbeating for new work items only
	newWorkItemIDs := make([]uuid.UUID, len(newWorkItems))
	for i, item := range newWorkItems {
		newWorkItemIDs[i] = item.ID
	}
	heartbeater := newInqueueHeartbeater(loop.mdb, myInstanceID, newWorkItemIDs)
	cancel := heartbeater.Start(ctx)
	defer cancel() // Ensure heartbeating stops when processing completes

	t0 = time.Now()
	inqueueBatch := make([]lrdb.Inqueue, len(newWorkItems))
	for i, item := range newWorkItems {
		inqueueBatch[i] = lrdb.Inqueue{
			ID:             item.ID,
			QueueTs:        item.QueueTs,
			Priority:       item.Priority,
			OrganizationID: item.OrganizationID,
			CollectorName:  item.CollectorName,
			InstanceNum:    item.InstanceNum,
			Bucket:         item.Bucket,
			ObjectID:       item.ObjectID,
			Signal:         item.Signal,
			Tries:          item.Tries,
			ClaimedBy:      item.ClaimedBy,
			ClaimedAt:      item.ClaimedAt,
			HeartbeatedAt:  item.HeartbeatedAt,
			FileSize:       item.FileSize,
		}
	}
	err = batchProcessingFx(ctx, ll, tmpdir, loop.sp, loop.mdb, loop.cloudManagers, inqueueBatch, ingestDateint, rpfEstimate, loop)
	inqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("bucket", newWorkItems[0].Bucket),
			attribute.Int("batchSize", len(newWorkItems)),
		))

	if err != nil {
		for _, h := range newWorkHandlers {
			if retryErr := h.RetryWork(ctx); retryErr != nil {
				ll.Error("Failed to retry work", slog.Any("error", retryErr))
			}
		}
		return true, false, fmt.Errorf("Batch processing failed: %w", err)
	}

	for _, h := range newWorkHandlers {
		if err := h.CompleteWork(ctx); err != nil {
			ll.Error("Failed to complete work", slog.Any("error", err))
		} else {
			atomic.AddInt64(loop.processedItems, 1)
		}
	}
	return false, true, nil
}

// validateBatchOrganizationConsistency ensures all items in a batch belong to the same organization
func validateBatchOrganizationConsistency(items []lrdb.InqueueGetBundleItemsRow) error {
	if len(items) <= 1 {
		return nil // Single item or empty batch is always safe
	}

	expectedOrgID := items[0].OrganizationID
	for i, item := range items {
		if item.OrganizationID != expectedOrgID {
			return fmt.Errorf("batch safety check failed: item %d has organization ID %s, expected %s",
				i, item.OrganizationID.String(), expectedOrgID.String())
		}
	}
	return nil
}
