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
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

type InqueueProcessingFunction func(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager,
	inf lrdb.Inqueue,
	ingest_dateint int32) error

func IngestLoop(doneCtx context.Context, sp storageprofile.StorageProfileProvider, signal string, assumeRoleSessionName string, processingFx InqueueProcessingFunction) error {
	ctx := context.Background()

	metadataStore, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return err
	}

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
		shouldBackoff, didWork, err := ingestFiles(ctx, ll, sp, metadataStore, awsmanager, signal, processingFx)
		if err != nil {
			return err
		}

		if didWork {
			ll.Info("Ingested file", slog.Duration("elapsed", time.Since(t0)))
		}

		if shouldBackoff {
			select {
			case <-doneCtx.Done():
				return nil
			case <-time.After(workSleepTime):
			}
		}
	}
}

func ingestFiles(
	ctx context.Context,
	ll *slog.Logger,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager,
	signalType string,
	processFx InqueueProcessingFunction,
) (bool, bool, error) {
	ctx, span := tracer.Start(ctx, "ingest", trace.WithAttributes(commonAttributes.ToSlice()...))
	defer span.End()

	t0 := time.Now()
	inf, err := mdb.ClaimInqueueWork(ctx, lrdb.ClaimInqueueWorkParams{
		ClaimedBy:     myInstanceID,
		TelemetryType: signalType,
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

	ll = ll.With(
		slog.String("id", inf.ID.String()),
		slog.Int("tries", int(inf.Tries)),
		slog.String("collectorName", inf.CollectorName),
		slog.String("organizationID", inf.OrganizationID.String()),
		slog.Int("instanceNum", int(inf.InstanceNum)),
		slog.String("bucket", inf.Bucket),
		slog.String("objectID", inf.ObjectID))

	h := NewInqueueHandler(ctx, ll, mdb, inf)

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

	t0 = time.Now()
	err = processFx(ctx, ll, tmpdir, sp, mdb, awsmanager, inf, ingestDateint)
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
