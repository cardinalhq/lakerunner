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
	"log/slog"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// TraceCleanupWorkItem implements WorkItem for trace segment cleanup
type TraceCleanupWorkItem struct {
	OrganizationID   uuid.UUID
	DateInt          int32
	NextRunTime      time.Time
	ConsecutiveEmpty int
	sp               storageprofile.StorageProfileProvider
	mdb              lrdb.StoreFull
	cmgr             cloudstorage.ClientProvider
}

func (w *TraceCleanupWorkItem) Perform(ctx context.Context) time.Duration {
	ll := logctx.FromContext(ctx).With(
		slog.String("org_id", w.OrganizationID.String()),
		slog.Int("dateint", int(w.DateInt)),
		slog.String("signal_type", "trace"))

	ageThreshold := time.Now().Add(-time.Hour)

	segments, err := w.mdb.TraceSegmentCleanupGet(ctx, lrdb.TraceSegmentCleanupGetParams{
		OrganizationID: w.OrganizationID,
		Dateint:        w.DateInt,
		AgeThreshold:   ageThreshold,
		MaxRows:        1000,
	})
	if err != nil {
		ll.Error("Failed to get trace segments for cleanup", slog.Any("error", err))
		w.ConsecutiveEmpty++
		return w.calculateBackoff()
	}

	if len(segments) == 0 {
		w.ConsecutiveEmpty++
		return w.calculateBackoff()
	}

	// Process segments with batching for better performance
	processed := 0
	totalBytes := int64(0)
	s3ObjectsToDelete := make(map[InstanceKey][]string) // instanceKey -> []objectKeys
	dbRecordsToDelete := make([]lrdb.TraceSegmentCleanupDeleteParams, 0, len(segments))

	// Prepare batches
	for _, segment := range segments {
		profile, err := w.sp.GetStorageProfileForOrganizationAndInstance(ctx, segment.OrganizationID, segment.InstanceNum)
		if err != nil {
			ll.Error("Failed to get storage profile", slog.Any("error", err))
			continue
		}

		objectKey := helpers.MakeDBObjectID(
			segment.OrganizationID,
			profile.CollectorName,
			segment.Dateint,
			getHourFromTimestamp(segment.TsRangeLower),
			segment.SegmentID,
			"traces",
		)

		// Group S3 deletions by instance
		instanceKey := InstanceKey{
			OrganizationID: segment.OrganizationID,
			InstanceNum:    segment.InstanceNum,
		}
		s3ObjectsToDelete[instanceKey] = append(s3ObjectsToDelete[instanceKey], objectKey)

		// Collect DB deletion params
		dbRecordsToDelete = append(dbRecordsToDelete, lrdb.TraceSegmentCleanupDeleteParams{
			OrganizationID: segment.OrganizationID,
			Dateint:        segment.Dateint,
			SegmentID:      segment.SegmentID,
			InstanceNum:    segment.InstanceNum,
		})

		totalBytes += segment.FileSize
		processed++
	}

	// Execute S3 deletions using batch operations
	s3DeletedCount := 0
	s3FailedCount := 0
	for instanceKey, objectKeys := range s3ObjectsToDelete {
		// Batch delete objects for this instance using the typed key
		deleted, _, failed, err := batchDeleteSegmentObjects(ctx, w.sp, w.cmgr, instanceKey.OrganizationID, instanceKey.InstanceNum, objectKeys)
		if err != nil {
			ll.Error("Failed to batch delete objects", slog.Any("error", err), slog.Int("object_count", len(objectKeys)))
			s3FailedCount += len(objectKeys)
		} else {
			s3DeletedCount += deleted
			s3FailedCount += failed
		}
	}

	// Execute database deletions
	dbDeletedCount := 0
	for _, params := range dbRecordsToDelete {
		if err := w.mdb.TraceSegmentCleanupDelete(ctx, params); err != nil {
			ll.Error("Failed to delete segment from database", slog.Any("error", err))
		} else {
			dbDeletedCount++
		}
	}

	// Update metrics
	if dbDeletedCount > 0 {
		traceCleanupCounter.Add(ctx, int64(dbDeletedCount), metric.WithAttributes(
			attribute.String("organization_id", w.OrganizationID.String()),
		))
		traceCleanupBytes.Add(ctx, totalBytes, metric.WithAttributes(
			attribute.String("organization_id", w.OrganizationID.String()),
		))

		ll.Info("Completed trace segment cleanup batch",
			slog.Int("segments_processed", dbDeletedCount),
			slog.Int("s3_objects_deleted", s3DeletedCount),
			slog.Int("s3_objects_failed", s3FailedCount),
			slog.Int64("bytes_cleaned", totalBytes),
			slog.String("org_id", w.OrganizationID.String()),
			slog.Int("dateint", int(w.DateInt)))
	}

	processed = dbDeletedCount // Use actual successful deletions

	if processed > 0 {
		w.ConsecutiveEmpty = 0
		if processed >= 1000 {
			// Hit the limit, likely more work available - retry very quickly
			return 2 * time.Second
		}
		return time.Minute // Normal retry for some work
	}

	w.ConsecutiveEmpty++
	return w.calculateBackoff()
}

func (w *TraceCleanupWorkItem) GetNextRunTime() time.Time  { return w.NextRunTime }
func (w *TraceCleanupWorkItem) SetNextRunTime(t time.Time) { w.NextRunTime = t }
func (w *TraceCleanupWorkItem) GetKey() string             { return makeOrgDateintKey(w.OrganizationID, w.DateInt) }

func (w *TraceCleanupWorkItem) calculateBackoff() time.Duration {
	switch {
	case w.ConsecutiveEmpty <= 2:
		return time.Minute
	case w.ConsecutiveEmpty <= 5:
		return 5 * time.Minute
	case w.ConsecutiveEmpty <= 10:
		return 15 * time.Minute
	case w.ConsecutiveEmpty <= 20:
		return 30 * time.Minute
	default:
		return time.Hour
	}
}
