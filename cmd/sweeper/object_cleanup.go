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
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// Aggressive loop for object cleanup.
// If work was done: tiny delay; else a slightly longer pause. Errors are logged and retried.
func objectCleanerLoop(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider) error {
	const (
		delayIfNoWork = 5 * time.Second
		delayIfError  = 5 * time.Second
	)

	ll := logctx.FromContext(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		didWork, err := runObjCleaner(ctx, sp, mdb, cmgr)
		switch {
		case err != nil:
			ll.Error("Failed to run object cleaner", slog.Any("error", err))
			if stop := sleepCtx(ctx, delayIfError); stop {
				return ctx.Err()
			}
		case didWork:
			// run right away again
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		default:
			if stop := sleepCtx(ctx, delayIfNoWork); stop {
				return ctx.Err()
			}
		}
	}
}

func runObjCleaner(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider) (bool, error) {
	const maxrows = 1000
	objs, err := mdb.ObjectCleanupGet(ctx, maxrows)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if len(objs) == 0 {
		return false, nil
	}

	didwork := len(objs) == maxrows

	jobs := make(chan lrdb.ObjectCleanupGetRow, len(objs))
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range jobs {
				cleanupObj(ctx, sp, mdb, cmgr, obj)
			}
		}()
	}
	for _, obj := range objs {
		jobs <- obj
	}
	close(jobs)
	wg.Wait()

	return didwork, nil
}

func cleanupObj(ctx context.Context, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, cmgr cloudstorage.ClientProvider, obj lrdb.ObjectCleanupGetRow) {
	ll := logctx.FromContext(ctx).With(
		slog.String("objectID", obj.ObjectID),
		slog.String("bucketID", obj.BucketID),
		slog.String("organizationID", obj.OrganizationID.String()),
		slog.Int("instanceNum", int(obj.InstanceNum)),
	)

	ctx = logctx.WithLogger(ctx, ll)

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, obj.OrganizationID, obj.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, mdb, obj.ID)
		return
	}

	if profile.Bucket != obj.BucketID {
		ll.Error("Storage profile bucket mismatch", slog.String("profileBucket", profile.Bucket))
		failWork(ctx, mdb, obj.ID)
		return
	}

	storageClient, err := cloudstorage.NewClient(ctx, cmgr, profile)
	if err != nil {
		ll.Error("Failed to get storage client", slog.Any("error", err))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, mdb, obj.ID)
		return
	}

	if err := storageClient.DeleteObject(ctx, profile.Bucket, obj.ObjectID); err != nil {
		ll.Error("Failed to delete S3 object", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, mdb, obj.ID)
		return
	}

	if err := mdb.ObjectCleanupComplete(ctx, obj.ID); err != nil {
		ll.Error("Failed to mark object cleanup complete", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, mdb, obj.ID)
		return
	}

	objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", "success"),
		attribute.String("bucket", obj.BucketID),
		attribute.String("organization_id", obj.OrganizationID.String()),
	))
}

func failWork(ctx context.Context, mdb lrdb.StoreFull, id uuid.UUID) {
	ll := logctx.FromContext(ctx)

	if err := mdb.ObjectCleanupFail(ctx, id); err != nil {
		ll.Error("Failed to mark object cleanup failed", slog.Any("error", err), slog.String("objectID", id.String()))
	}
}
