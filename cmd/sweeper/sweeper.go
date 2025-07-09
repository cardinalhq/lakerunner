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
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
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
		select {
		case <-doneCtx.Done():
			return doneCtx.Err()
		default:
		}

		err := sweep(ctx, slog.Default(), cmd.sp, mdb, awsmanager)
		if err != nil {
			return err
		}

		select {
		case <-doneCtx.Done():
			return doneCtx.Err()
		case <-time.After(1 * time.Minute):
		}
	}
}

func sweep(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager) error {
	err := runObjCleaner(ctx, ll, sp, mdb, awsmanager)
	if err != nil {
		ll.Error("Failed to run object cleaner", slog.Any("error", err))
		return err
	}

	err = runWorkqueueExpiry(ctx, ll, mdb)
	if err != nil {
		ll.Error("Failed to run expiry", slog.Any("error", err))
		return err
	}
	return nil
}

func runObjCleaner(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager) error {
	objs, err := mdb.ObjectCleanupGet(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if len(objs) == 0 {
		return nil
	}

	var wg sync.WaitGroup

	for _, obj := range objs {
		wg.Add(1)
		go cleanupObj(ctx, ll, sp, mdb, awsmanager, obj, &wg)
	}
	wg.Wait()

	return nil
}

func cleanupObj(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager, obj lrdb.ObjectCleanupGetRow, wg *sync.WaitGroup) {
	defer wg.Done()

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
