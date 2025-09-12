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

// LogCleanupWorkItem implements WorkItem for log segment cleanup
type LogCleanupWorkItem struct {
	OrganizationID   uuid.UUID
	DateInt          int32
	NextRunTime      time.Time
	ConsecutiveEmpty int
	sp               storageprofile.StorageProfileProvider
	mdb              lrdb.StoreFull
	cmgr             cloudstorage.ClientProvider
}

func (w *LogCleanupWorkItem) Perform(ctx context.Context) time.Duration {
	ll := logctx.FromContext(ctx).With(
		slog.String("org_id", w.OrganizationID.String()),
		slog.Int("dateint", int(w.DateInt)),
		slog.String("signal_type", "log"))

	ageThreshold := time.Now().Add(-time.Hour)

	segments, err := w.mdb.LogSegmentCleanupGet(ctx, lrdb.LogSegmentCleanupGetParams{
		OrganizationID: w.OrganizationID,
		Dateint:        w.DateInt,
		AgeThreshold:   ageThreshold,
		MaxRows:        1000,
	})
	if err != nil {
		ll.Error("Failed to get log segments for cleanup", slog.Any("error", err))
		w.ConsecutiveEmpty++
		return w.calculateBackoff()
	}

	if len(segments) == 0 {
		w.ConsecutiveEmpty++
		return w.calculateBackoff()
	}

	// Process segments (simplified version)
	processed := 0
	for _, segment := range segments {
		// Get storage profile and delete from storage/database
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
			"logs",
		)

		if err := deleteSegmentObject(ctx, w.sp, w.cmgr, segment.OrganizationID, segment.InstanceNum, objectKey); err != nil {
			continue
		}

		// Delete from database
		if err := w.mdb.LogSegmentCleanupDelete(ctx, lrdb.LogSegmentCleanupDeleteParams{
			OrganizationID: segment.OrganizationID,
			Dateint:        segment.Dateint,
			SegmentID:      segment.SegmentID,
			InstanceNum:    segment.InstanceNum,
		}); err != nil {
			ll.Error("Failed to delete segment from database", slog.Any("error", err))
			continue
		}

		processed++

		// Update metrics
		logCleanupCounter.Add(ctx, 1, metric.WithAttributes(
			attribute.String("organization_id", segment.OrganizationID.String()),
		))
		logCleanupBytes.Add(ctx, segment.FileSize, metric.WithAttributes(
			attribute.String("organization_id", segment.OrganizationID.String()),
		))
	}

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

func (w *LogCleanupWorkItem) GetNextRunTime() time.Time  { return w.NextRunTime }
func (w *LogCleanupWorkItem) SetNextRunTime(t time.Time) { w.NextRunTime = t }
func (w *LogCleanupWorkItem) GetKey() string             { return makeOrgDateintKey(w.OrganizationID, w.DateInt) }

func (w *LogCleanupWorkItem) calculateBackoff() time.Duration {
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
