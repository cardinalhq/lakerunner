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
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const (
	gcBatchLimit int32         = 1000
	gcBatchDelay time.Duration = 500 * time.Millisecond
	gcPeriod     time.Duration = time.Hour
	gcCutoffAge  time.Duration = 10 * 24 * time.Hour
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
	ctx, cancel := context.WithCancel(doneCtx)
	defer cancel()

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

	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// Aggressive object delete loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := objectCleanerLoop(ctx, slog.Default(), cmd.sp, mdb, awsmanager); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Periodic: workqueue expiry
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := periodicLoop(ctx, time.Minute, func(c context.Context) error {
			return runWorkqueueExpiry(c, slog.Default(), mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Periodic: inqueue expiry
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := periodicLoop(ctx, time.Minute, func(c context.Context) error {
			return runInqueueExpiry(c, slog.Default(), mdb)
		}); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()

	// Periodic: workqueue GC
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := workqueueGCLoop(ctx, slog.Default(), mdb); err != nil && !errors.Is(err, context.Canceled) {
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
	if err := f(ctx); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Error("periodic task error", slog.Any("error", err))
	}

	t := time.NewTicker(period)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := f(ctx); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				slog.Error("periodic task error", slog.Any("error", err))
				// keep going; periodic tasks should be resilient
			}
		}
	}
}

// Aggressive loop for object cleanup.
// If work was done: tiny delay; else a slightly longer pause. Errors are logged and retried.
func objectCleanerLoop(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager) error {
	const (
		delayIfNoWork = 5 * time.Second
		delayIfError  = 5 * time.Second
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		didWork, err := runObjCleaner(ctx, ll, sp, mdb, awsmanager)
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

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return true
	case <-t.C:
		return false
	}
}

func runObjCleaner(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager) (bool, error) {
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
				cleanupObj(ctx, ll, sp, mdb, awsmanager, obj)
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

func cleanupObj(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, awsmanager *awsclient.Manager, obj lrdb.ObjectCleanupGetRow) {
	profile, err := sp.Get(ctx, obj.OrganizationID, obj.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	if err := s3helper.DeleteS3Object(ctx, s3client, profile.Bucket, obj.ObjectID); err != nil {
		ll.Error("Failed to delete S3 object", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	if err := mdb.ObjectCleanupComplete(ctx, obj.ID); err != nil {
		ll.Error("Failed to mark object cleanup complete", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}
}

func failWork(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull, id uuid.UUID) {
	if err := mdb.ObjectCleanupFail(ctx, id); err != nil {
		ll.Error("Failed to mark object cleanup failed", slog.Any("error", err), slog.String("objectID", id.String()))
	}
}

func runWorkqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Use default of 20 minutes for lock_ttl_dead
	lockTtlDead := pgtype.Interval{Microseconds: (20 * time.Minute).Microseconds(), Valid: true}
	expired, err := mdb.WorkQueueCleanup(ctx, lockTtlDead)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire objects", slog.Any("error", err))
		return err
	}
	for _, obj := range expired {
		ll.Info("Expired work/lock", slog.Any("work", obj))
	}

	// Clean up orphaned signal locks
	deleted, err := mdb.WorkQueueOrphanedSignalLockCleanup(ctx, 1000)
	if err != nil {
		ll.Error("Failed to cleanup orphaned signal locks", slog.Any("error", err))
		return err
	}
	if deleted > 0 {
		ll.Info("Cleaned up orphaned signal locks", slog.Int("deleted", int(deleted)))
	}

	return nil
}

func runInqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	if err := mdb.CleanupInqueueWork(ctx); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire objects", slog.Any("error", err))
		return err
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
