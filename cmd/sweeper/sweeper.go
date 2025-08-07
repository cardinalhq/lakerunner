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
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type sweeper struct {
	instanceID            int64
	assumeRoleSessionName string
	sp                    storageprofile.StorageProfileProvider
}

func New(instanceID int64, assumeRoleSessionName string) *sweeper {
	sp, err := storageprofile.SetupStorageProfiles()
	if err != nil {
		slog.Error("Failed to setup storage profiles", slog.Any("error", err))
		os.Exit(1)
	}
	return &sweeper{
		instanceID:            instanceID,
		assumeRoleSessionName: assumeRoleSessionName,
		sp:                    sp,
	}
}

func (cmd *sweeper) Run(doneCtx context.Context) error {
	ctx := context.Background()

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return err
	}

	awsmanager, err := awsclient.NewManager(ctx,
		awsclient.WithAssumeRoleSessionName(cmd.assumeRoleSessionName),
	)
	if err != nil {
		return err
	}

	slog.Info("Starting sweeper", slog.Int64("instanceID", cmd.instanceID))

	for {
		didWork, err := sweep(ctx, slog.Default(), cmd.sp, mdb, awsmanager)
		if err != nil {
			return err
		}

		nextTime := 1 * time.Minute
		if didWork {
			nextTime = 1 * time.Second
		}

		select {
		case <-doneCtx.Done():
			return doneCtx.Err()
		case <-time.After(nextTime):
		}
	}
}

func sweep(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager) (bool, error) {
	didWork, err := runObjCleaner(ctx, ll, sp, mdb, awsmanager)
	if err != nil {
		ll.Error("Failed to run object cleaner", slog.Any("error", err))
		return false, err
	}

	err = runWorkqueueExpiry(ctx, ll, mdb)
	if err != nil {
		ll.Error("Failed to run expiry", slog.Any("error", err))
		return false, err
	}

	err = runInqueueExpiry(ctx, ll, mdb)
	if err != nil {
		ll.Error("Failed to run inqueue expiry", slog.Any("error", err))
		return false, err
	}

	return didWork, nil
}

func runObjCleaner(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager) (bool, error) {
	objs, err := mdb.ObjectCleanupGet(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	if len(objs) == 0 {
		return false, nil
	}

	jobs := make(chan lrdb.ObjectCleanupGetRow)
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for obj := range jobs {
				cleanupObj(ctx, ll, sp, mdb, awsmanager, obj)
			}
		}()
	}
	for _, obj := range objs {
		jobs <- obj
	}
	close(jobs)
	wg.Wait()

	return true, nil
}

func cleanupObj(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager, obj lrdb.ObjectCleanupGetRow) {
	profile, err := sp.Get(ctx, obj.OrganizationID, obj.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	if profile.Role == "" {
		if !profile.Hosted {
			ll.Error("No role on non-hosted profile")
			failWork(ctx, ll, mdb, obj.ID)
			return
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	err = s3helper.DeleteS3Object(ctx, s3client, profile.Bucket, obj.ObjectID)
	if err != nil {
		ll.Error("Failed to delete S3 object", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	err = mdb.ObjectCleanupComplete(ctx, obj.ID)
	if err != nil {
		ll.Error("Failed to mark object cleanup complete", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}
	ll.Info("Successfully cleaned up object", slog.Any("request", obj))
}

func failWork(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull, id uuid.UUID) {
	if err := mdb.ObjectCleanupFail(ctx, id); err != nil {
		ll.Error("Failed to mark object cleanup failed", slog.Any("error", err), slog.String("objectID", id.String()))
	}
}

func runWorkqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	expired, err := mdb.WorkQueueCleanup(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire objects", slog.Any("error", err))
		return err
	}

	if len(expired) == 0 {
		return nil
	}

	for _, obj := range expired {
		ll.Info("Expired work/lock", slog.Any("work", obj))
	}

	return nil
}

func runInqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	err := mdb.CleanupInqueueWork(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire objects", slog.Any("error", err))
		return err
	}

	return nil
}
