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
	"fmt"
	"log/slog"
	"strings"
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

// MetricCleanupWorkItem implements WorkItem for metric segment cleanup
type MetricCleanupWorkItem struct {
	OrganizationID   uuid.UUID
	DateInt          int32
	NextRunTime      time.Time
	ConsecutiveEmpty int
	sp               storageprofile.StorageProfileProvider
	mdb              lrdb.StoreFull
	cmgr             cloudstorage.ClientProvider
}

func (w *MetricCleanupWorkItem) Perform(ctx context.Context) time.Duration {
	ll := logctx.FromContext(ctx).With(
		slog.String("org_id", w.OrganizationID.String()),
		slog.Int("dateint", int(w.DateInt)),
		slog.String("signal_type", "metric"))

	ageThreshold := time.Now().Add(-time.Hour)

	segments, err := w.mdb.MetricSegmentCleanupGet(ctx, lrdb.MetricSegmentCleanupGetParams{
		OrganizationID: w.OrganizationID,
		Dateint:        w.DateInt,
		AgeThreshold:   ageThreshold,
		MaxRows:        1000,
	})
	if err != nil {
		ll.Error("Failed to get metric segments for cleanup", slog.Any("error", err))
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
	s3ObjectsToDelete := make(map[string][]string) // instanceKey -> []objectKeys
	dbRecordsToDelete := make([]lrdb.MetricSegmentCleanupDeleteParams, 0, len(segments))

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
			"metrics",
		)

		// Group S3 deletions by instance
		instanceKey := fmt.Sprintf("%s-%d", segment.OrganizationID.String(), segment.InstanceNum)
		s3ObjectsToDelete[instanceKey] = append(s3ObjectsToDelete[instanceKey], objectKey)

		// Collect DB deletion params
		dbRecordsToDelete = append(dbRecordsToDelete, lrdb.MetricSegmentCleanupDeleteParams{
			OrganizationID: segment.OrganizationID,
			Dateint:        segment.Dateint,
			FrequencyMs:    segment.FrequencyMs,
			SegmentID:      segment.SegmentID,
			InstanceNum:    segment.InstanceNum,
		})

		totalBytes += segment.FileSize
		processed++
	}

	// Execute S3 deletions
	s3DeletedCount := 0
	for instanceKey, objectKeys := range s3ObjectsToDelete {
		parts := strings.Split(instanceKey, "-")
		if len(parts) != 2 {
			continue
		}
		orgID, _ := uuid.Parse(parts[0])
		instanceNum := int16(0)

		for _, segment := range segments {
			if segment.OrganizationID == orgID {
				instanceNum = segment.InstanceNum
				break
			}
		}

		for _, objectKey := range objectKeys {
			if err := deleteSegmentObject(ctx, w.sp, w.cmgr, orgID, instanceNum, objectKey); err == nil {
				s3DeletedCount++
			}
		}
	}

	// Execute database deletions
	dbDeletedCount := 0
	for _, params := range dbRecordsToDelete {
		if err := w.mdb.MetricSegmentCleanupDelete(ctx, params); err != nil {
			ll.Error("Failed to delete segment from database", slog.Any("error", err))
		} else {
			dbDeletedCount++
		}
	}

	// Update metrics
	if dbDeletedCount > 0 {
		metricCleanupCounter.Add(ctx, int64(dbDeletedCount), metric.WithAttributes(
			attribute.String("organization_id", w.OrganizationID.String()),
		))
		metricCleanupBytes.Add(ctx, totalBytes, metric.WithAttributes(
			attribute.String("organization_id", w.OrganizationID.String()),
		))

		ll.Info("Completed metric segment cleanup batch",
			slog.Int("segments_processed", dbDeletedCount),
			slog.Int("s3_objects_deleted", s3DeletedCount),
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

func (w *MetricCleanupWorkItem) GetNextRunTime() time.Time  { return w.NextRunTime }
func (w *MetricCleanupWorkItem) SetNextRunTime(t time.Time) { w.NextRunTime = t }
func (w *MetricCleanupWorkItem) GetKey() string {
	return makeOrgDateintKey(w.OrganizationID, w.DateInt)
}

func (w *MetricCleanupWorkItem) calculateBackoff() time.Duration {
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
