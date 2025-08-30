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
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/internal/workqueue"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	objectCleanupCounter     metric.Int64Counter
	legacyTableSyncCounter   metric.Int64Counter
	workQueueExpiryCounter   metric.Int64Counter
	inqueueExpiryCounter     metric.Int64Counter
	signalLockCleanupCounter metric.Int64Counter
	legacyTableSyncDuration  metric.Float64Histogram
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/cmd/sweeper")

	var err error
	objectCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.object_cleanup_total",
		metric.WithDescription("Count of objects processed during cleanup"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create object_cleanup_total counter: %w", err))
	}

	legacyTableSyncCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.legacy_table_sync_total",
		metric.WithDescription("Count of legacy table synchronization runs"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create legacy_table_sync_total counter: %w", err))
	}

	workQueueExpiryCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.workqueue_expiry_total",
		metric.WithDescription("Count of work queue items expired due to staleness"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create workqueue_expiry_total counter: %w", err))
	}

	inqueueExpiryCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.inqueue_expiry_total",
		metric.WithDescription("Count of inqueue items expired due to staleness"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create inqueue_expiry_total counter: %w", err))
	}

	signalLockCleanupCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.signal_lock_cleanup_total",
		metric.WithDescription("Count of orphaned signal locks cleaned up"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create signal_lock_cleanup_total counter: %w", err))
	}

	legacyTableSyncDuration, err = meter.Float64Histogram(
		"lakerunner.sweeper.legacy_table_sync_duration_seconds",
		metric.WithDescription("Duration of legacy table synchronization runs in seconds"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create legacy_table_sync_duration_seconds histogram: %w", err))
	}
}

const (
	gcBatchLimit           int32         = 1000
	gcBatchDelay           time.Duration = 500 * time.Millisecond
	gcPeriod               time.Duration = time.Hour
	gcCutoffAge            time.Duration = 10 * 24 * time.Hour
	legacyTablesSyncPeriod time.Duration = 5 * time.Minute
)

type sweeper struct {
	instanceID            int64
	assumeRoleSessionName string
	sp                    storageprofile.StorageProfileProvider
	syncLegacyTables      bool
}

func New(instanceID int64, assumeRoleSessionName string, syncLegacyTables bool) *sweeper {
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
		instanceID:            instanceID,
		assumeRoleSessionName: assumeRoleSessionName,
		sp:                    sp,
		syncLegacyTables:      syncLegacyTables,
	}
}

func (cmd *sweeper) Run(doneCtx context.Context) error {
	ctx, cancel := context.WithCancel(doneCtx)
	defer cancel()

	mdb, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return err
	}

	var cdb configdb.QuerierFull
	var cdbPool *pgxpool.Pool
	if cmd.syncLegacyTables {
		cdb, err = dbopen.ConfigDBStore(ctx)
		if err != nil {
			return err
		}
		cdbPool, err = dbopen.ConnectToConfigDB(ctx)
		if err != nil {
			return err
		}
	}

	// No need to create awsmanager globally - we'll create providers per-profile

	slog.Info("Starting sweeper",
		slog.Int64("instanceID", cmd.instanceID),
		slog.Bool("syncLegacyTables", cmd.syncLegacyTables))

	var wg sync.WaitGroup
	errCh := make(chan error, 4)

	// Aggressive object delete loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := objectCleanerLoop(ctx, slog.Default(), cmd.sp, mdb); err != nil && !errors.Is(err, context.Canceled) {
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

	// Periodic: legacy table sync if enabled
	if cmd.syncLegacyTables {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := periodicLoop(ctx, legacyTablesSyncPeriod, func(c context.Context) error {
				return runLegacyTablesSync(c, slog.Default(), cdb, cdbPool)
			}); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}

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
func objectCleanerLoop(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull) error {
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

		didWork, err := runObjCleaner(ctx, ll, sp, mdb)
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

func runObjCleaner(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull) (bool, error) {
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
				cleanupObj(ctx, ll, sp, mdb, obj)
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

func cleanupObj(ctx context.Context, ll *slog.Logger, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull, obj lrdb.ObjectCleanupGetRow) {
	ll = ll.With(
		slog.String("objectID", obj.ObjectID),
		slog.String("bucketID", obj.BucketID),
		slog.String("organizationID", obj.OrganizationID.String()),
		slog.Int("instanceNum", int(obj.InstanceNum)),
	)

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, obj.OrganizationID, obj.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	if profile.Bucket != obj.BucketID {
		ll.Error("Storage profile bucket mismatch", slog.String("profileBucket", profile.Bucket))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	objectStoreClient, err := cloudprovider.GetObjectStoreClientForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get object store client", slog.Any("error", err))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	if err := objectStoreClient.DeleteObject(ctx, profile.Bucket, obj.ObjectID); err != nil {
		ll.Error("Failed to delete S3 object", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	if err := mdb.ObjectCleanupComplete(ctx, obj.ID); err != nil {
		ll.Error("Failed to mark object cleanup complete", slog.Any("error", err), slog.String("objectID", obj.ObjectID))
		objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", "failure"),
			attribute.String("bucket", obj.BucketID),
			attribute.String("organization_id", obj.OrganizationID.String()),
		))
		failWork(ctx, ll, mdb, obj.ID)
		return
	}

	objectCleanupCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("status", "success"),
		attribute.String("bucket", obj.BucketID),
		attribute.String("organization_id", obj.OrganizationID.String()),
	))
}

func failWork(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull, id uuid.UUID) {
	if err := mdb.ObjectCleanupFail(ctx, id); err != nil {
		ll.Error("Failed to mark object cleanup failed", slog.Any("error", err), slog.String("objectID", id.String()))
	}
}

func runWorkqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Calculate stale expiration based on heartbeat interval
	// Items are considered stale after missing the configured number of heartbeats
	staleExpirationTime := time.Duration(workqueue.StaleExpiryMultiplier) * workqueue.DefaultHeartbeatInterval
	lockTtlDead := pgtype.Interval{Microseconds: staleExpirationTime.Microseconds(), Valid: true}
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
		workQueueExpiryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("signal", string(obj.Signal)),
			attribute.String("action", string(obj.Action)),
		))
	}

	// Clean up orphaned signal locks
	deleted, err := mdb.WorkQueueOrphanedSignalLockCleanup(ctx, 1000)
	if err != nil {
		ll.Error("Failed to cleanup orphaned signal locks", slog.Any("error", err))
		return err
	}
	signalLockCleanupCounter.Add(ctx, int64(deleted))
	if deleted > 0 {
		ll.Info("Cleaned up orphaned signal locks", slog.Int("deleted", int(deleted)))
	}

	return nil
}

func runInqueueExpiry(ctx context.Context, ll *slog.Logger, mdb lrdb.StoreFull) error {
	// Calculate cutoff time for inqueue items - 5 minutes is the default timeout for claimed work
	inqueueStaleTimeout := 5 * time.Minute
	cutoffTime := time.Now().Add(-inqueueStaleTimeout)

	expired, err := mdb.CleanupInqueueWork(ctx, &cutoffTime)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		ll.Error("Failed to expire inqueue objects", slog.Any("error", err))
		return err
	}
	for _, obj := range expired {
		ll.Info("Expired inqueue item", slog.Any("item", obj))
		inqueueExpiryCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("signal", obj.TelemetryType),
		))
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

func runLegacyTablesSync(ctx context.Context, ll *slog.Logger, cdb configdb.QuerierFull, cdbPool *pgxpool.Pool) (err error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		legacyTableSyncDuration.Record(ctx, duration)
		status := "success"
		if err != nil {
			status = "failure"
		}
		legacyTableSyncCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("status", status),
		))
	}()

	ll.Info("Starting legacy table sync")

	// Get all storage profiles from c_ tables
	profiles, err := cdb.GetAllCStorageProfilesForSync(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Info("No legacy storage profiles found")
			return nil
		}
		return err
	}

	if len(profiles) == 0 {
		ll.Info("No legacy storage profiles to sync")
		return nil
	}

	ll.Info("Found legacy storage profiles to sync", slog.Int("count", len(profiles)))

	// Start a transaction for atomic sync
	closed := false
	tx, err := cdbPool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if !closed {
			if rbErr := tx.Rollback(ctx); rbErr != nil {
				ll.Warn("Failed to rollback transaction", slog.Any("error", rbErr))
			}
		}
	}()

	qtx := configdb.New(tx)

	// Clear existing bucket management tables (mirror mode)
	if err = qtx.ClearBucketPrefixMappings(ctx); err != nil {
		return
	}
	if err = qtx.ClearOrganizationBuckets(ctx); err != nil {
		return
	}
	if err = qtx.ClearBucketConfigurations(ctx); err != nil {
		return
	}

	// Group profiles by bucket to ensure 1:1 mapping
	bucketToProfile := make(map[string]configdb.GetAllCStorageProfilesForSyncRow)
	bucketToOrgs := make(map[string][]uuid.UUID)

	for _, profile := range profiles {
		// Use first profile found for each bucket (1:1 mapping enforced)
		if _, exists := bucketToProfile[profile.BucketName]; !exists {
			bucketToProfile[profile.BucketName] = profile
		}

		bucketToOrgs[profile.BucketName] = append(bucketToOrgs[profile.BucketName], profile.OrganizationID)
	}

	// Create bucket configurations and organization mappings
	for bucketName, profile := range bucketToProfile {
		// Create/update bucket configuration
		_, err = qtx.UpsertBucketConfiguration(ctx, configdb.UpsertBucketConfigurationParams{
			BucketName:    bucketName,
			CloudProvider: profile.CloudProvider,
			Region:        profile.Region,
			Endpoint:      nil,
			Role:          profile.Role,
			UsePathStyle:  true,
			InsecureTls:   false,
		})
		if err != nil {
			return
		}

		ll.Info("Synced bucket configuration",
			slog.String("bucket", bucketName),
			slog.String("provider", profile.CloudProvider),
			slog.String("region", profile.Region))

	}

	// Sync organization buckets from c_collectors to our organization_buckets table
	ll.Info("Syncing organization buckets from c_collectors table")
	if err = qtx.SyncOrganizationBuckets(ctx); err != nil {
		err = fmt.Errorf("failed to sync organization buckets: %w", err)
		return
	}
	ll.Info("Successfully synced organization buckets")

	// Sync organizations from c_organizations to our organizations table
	ll.Info("Syncing organizations from c_organizations table")
	if err = qtx.SyncOrganizations(ctx); err != nil {
		err = fmt.Errorf("failed to sync organizations: %w", err)
		return
	}
	ll.Info("Successfully synced organizations")

	// Sync organization API keys from c_organization_api_keys to our organization tables
	if err = syncOrganizationAPIKeys(ctx, ll, qtx); err != nil {
		return
	}

	// Commit the transaction
	if err = tx.Commit(ctx); err != nil {
		return
	}
	closed = true

	ll.Info("Legacy table sync completed successfully",
		slog.Int("bucketsSync", len(bucketToProfile)),
		slog.Int("totalProfiles", len(profiles)))

	return nil
}

func syncOrganizationAPIKeys(ctx context.Context, ll *slog.Logger, qtx *configdb.Queries) error {
	ll.Info("Starting organization API keys sync")

	// Get all organization API keys from c_ table
	cAPIKeys, err := qtx.GetAllCOrganizationAPIKeysForSync(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			ll.Info("No legacy organization API keys found")
			return nil
		}
		return err
	}

	if len(cAPIKeys) == 0 {
		ll.Info("No legacy organization API keys to sync")
		return nil
	}

	ll.Info("Found legacy organization API keys to sync", slog.Int("count", len(cAPIKeys)))

	// Clear existing organization API key tables (mirror mode)
	if err := qtx.ClearOrganizationAPIKeyMappings(ctx); err != nil {
		return err
	}
	if err := qtx.ClearOrganizationAPIKeys(ctx); err != nil {
		return err
	}

	// Sync API keys
	for _, cAPIKey := range cAPIKeys {
		if !cAPIKey.OrganizationID.Valid || cAPIKey.ApiKey == nil {
			ll.Warn("Skipping API key with invalid data",
				slog.Bool("hasOrgID", cAPIKey.OrganizationID.Valid),
				slog.Bool("hasAPIKey", cAPIKey.ApiKey != nil))
			continue
		}

		orgID := cAPIKey.OrganizationID.Bytes
		orgUUID, err := uuid.FromBytes(orgID[:])
		if err != nil {
			ll.Warn("Skipping API key with invalid organization UUID", slog.Any("error", err))
			continue
		}

		apiKey := *cAPIKey.ApiKey
		keyHash := hashAPIKey(apiKey)

		// Create organization API key
		var name string
		if cAPIKey.Name != nil && *cAPIKey.Name != "" {
			name = *cAPIKey.Name
		} else {
			name = fmt.Sprintf("synced-key-%s", apiKey[:min(8, len(apiKey))])
		}

		apiKeyRow, err := qtx.UpsertOrganizationAPIKey(ctx, configdb.UpsertOrganizationAPIKeyParams{
			KeyHash:     keyHash,
			Name:        name,
			Description: nil,
		})
		if err != nil {
			return fmt.Errorf("failed to sync organization API key for %s: %w", orgUUID, err)
		}

		// Create organization API key mapping
		if err := qtx.UpsertOrganizationAPIKeyMapping(ctx, configdb.UpsertOrganizationAPIKeyMappingParams{
			ApiKeyID:       apiKeyRow.ID,
			OrganizationID: orgUUID,
		}); err != nil {
			return fmt.Errorf("failed to create API key mapping: %w", err)
		}

		ll.Info("Synced organization API key",
			slog.String("org_id", orgUUID.String()),
			slog.String("key_name", name))
	}

	ll.Info("Organization API keys sync completed successfully", slog.Int("keysSync", len(cAPIKeys)))
	return nil
}

func hashAPIKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", h)
}
