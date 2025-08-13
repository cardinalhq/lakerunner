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
	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/helpers"
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

type IngestLoopContext struct {
	ctx               context.Context
	mdb               lrdb.StoreFull
	sp                storageprofile.StorageProfileProvider
	awsmanager        *awsclient.Manager
	estimator         estimator.Estimator
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

	est, err := estimator.NewEstimator(ctx, mdb)
	if err != nil {
		return nil, fmt.Errorf("failed to create estimator: %w", err)
	}

	sp, err := storageprofile.SetupStorageProfiles()
	if err != nil {
		return nil, fmt.Errorf("failed to setup storage profiles: %w", err)
	}

	// Create exemplar processor optimized for batch processing
	// - Large cache size for cross-file deduplication
	// - Longer expiry to catch duplicates across files
	// - Frequent cleanup to prevent memory explosion
	exemplarProcessor := exemplar.NewProcessor(
		100000,         // cache size - large enough for cross-file deduplication
		10*time.Minute, // expiry - long enough to catch duplicates across files
		30*time.Second, // report interval - frequent cleanup for testing
		ll,             // logger
		func(ctx context.Context, organizationID string, exemplars []*exemplar.ExemplarData) error {
			// No-op callback - will be replaced by service-specific callbacks
			return nil
		},
	)

	return &IngestLoopContext{
		ctx:               ctx,
		mdb:               mdb,
		sp:                sp,
		awsmanager:        awsmanager,
		estimator:         est,
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
	ctx := context.Background()

	for {
		select {
		case <-loop.ctx.Done():
			return loop.ctx.Err()
		default:
		}

		t0 := time.Now()
		shouldBackoff, didWork, err := ingestFiles(ctx, loop, processingFx)
		if err != nil {
			return err
		}

		if didWork {
			loop.ll.Info("Ingested file", slog.Duration("elapsed", time.Since(t0)))
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
		slog.String("collectorName", inf.CollectorName),
		slog.String("organizationID", inf.OrganizationID.String()),
		slog.Int("instanceNum", int(inf.InstanceNum)),
		slog.String("bucket", inf.Bucket),
		slog.String("objectID", inf.ObjectID))

	h := NewInqueueHandler(ctx, ll, loop.mdb, inf)

	if inf.Tries > 10 {
		ll.Warn("Too many tries, deleting")
		h.CompleteWork()
		return false, true, nil
	}

	isNew, err := h.IsNewWork()
	if err != nil {
		h.RetryWork()
		return true, false, err
	}
	if !isNew {
		ll.Warn("already processed, releasing")
		h.CompleteWork()
		return true, true, nil
	}

	ll.Info("Processing")

	ingestDateint, _ := helpers.MSToDateintHour(time.Now().UTC().UnixMilli())

	// Create a temporary directory for processing
	tmpdir, err := os.MkdirTemp("", "lakerunner-ingest-*")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		h.RetryWork()
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

	rpfEstimate := loop.estimator.Get(inf.OrganizationID, inf.InstanceNum, lrdb.SignalEnum(inf.TelemetryType)).EstimatedRecordCount
	t0 = time.Now()
	err = processFx(ctx, ll, tmpdir, loop.sp, loop.mdb, loop.awsmanager, inf, ingestDateint, rpfEstimate, loop)
	inqueueDuration.Record(ctx, time.Since(t0).Seconds(),
		metric.WithAttributeSet(commonAttributes),
		metric.WithAttributes(
			attribute.String("organizationID", inf.OrganizationID.String()),
			attribute.String("collectorName", inf.CollectorName),
			attribute.Int("instanceNum", int(inf.InstanceNum)),
			attribute.String("bucket", inf.Bucket),
		))
	if err != nil {
		h.RetryWork()
		return true, false, fmt.Errorf("Processing failed: %w", err)
	}

	h.CompleteWork()
	return false, true, nil
}
