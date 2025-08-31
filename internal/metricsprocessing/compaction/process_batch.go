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

package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func processBatch(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	claimedWork []lrdb.ClaimMetricCompactionWorkRow,
) error {
	if len(claimedWork) == 0 {
		return nil
	}

	// Generate batch ID and enhance logger
	batchID := idgen.GenerateShortBase32ID()
	ll = ll.With(slog.String("batchID", batchID))

	// Log work items we're processing once at the top
	workItemIDs := make([]int64, len(claimedWork))
	for i, work := range claimedWork {
		workItemIDs[i] = work.ID
	}
	ll.Debug("Processing compaction batch",
		slog.Int("workItemCount", len(claimedWork)),
		slog.Any("workItemIDs", workItemIDs))

	// Safety check: All work items in a batch must have identical grouping fields
	firstItem := claimedWork[0]
	for i, item := range claimedWork {
		if item.OrganizationID != firstItem.OrganizationID ||
			item.Dateint != firstItem.Dateint ||
			item.FrequencyMs != firstItem.FrequencyMs ||
			item.InstanceNum != firstItem.InstanceNum {
			ll.Error("Inconsistent work batch detected - all items must have same org/dateint/frequency/instance",
				slog.Int("itemIndex", i),
				slog.String("expectedOrg", firstItem.OrganizationID.String()),
				slog.String("actualOrg", item.OrganizationID.String()),
				slog.Int("expectedDateint", int(firstItem.Dateint)),
				slog.Int("actualDateint", int(item.Dateint)),
				slog.Int64("expectedFreq", firstItem.FrequencyMs),
				slog.Int64("actualFreq", item.FrequencyMs),
				slog.Int("expectedInstance", int(firstItem.InstanceNum)),
				slog.Int("actualInstance", int(item.InstanceNum)))
			return fmt.Errorf("inconsistent work batch: item %d has different grouping fields", i)
		}
	}

	if !helpers.IsWantedFrequency(int32(firstItem.FrequencyMs)) {
		ll.Debug("Skipping compaction for unwanted frequency", slog.Int64("frequencyMs", firstItem.FrequencyMs))
		return nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	tmpdir, err := os.MkdirTemp("", "work-")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.Any("error", err))
		}
	}()

	ll.Info("Starting metric compaction batch",
		slog.String("organizationID", firstItem.OrganizationID.String()),
		slog.Int("instanceNum", int(firstItem.InstanceNum)),
		slog.Int("dateint", int(firstItem.Dateint)),
		slog.Int64("frequencyMs", firstItem.FrequencyMs),
		slog.Int("batchSize", len(claimedWork)))

	segments, err := fetchMetricSegs(ctx, mdb, claimedWork)
	if err != nil {
		ll.Error("Failed to fetch metric segments for compaction", slog.Any("error", err))
		return err
	}

	// Filter out any segments that are already compacted (safety check)
	validSegments := make([]lrdb.MetricSeg, 0, len(segments))
	for _, seg := range segments {
		if seg.Compacted {
			ll.Warn("Found already compacted segment in work batch - upstream issue detected",
				slog.Int64("segmentID", seg.SegmentID),
				slog.String("organizationID", seg.OrganizationID.String()),
				slog.Int("dateint", int(seg.Dateint)),
				slog.Int64("frequencyMs", int64(seg.FrequencyMs)),
				slog.Int("instanceNum", int(seg.InstanceNum)))
			continue
		}
		validSegments = append(validSegments, seg)
	}

	return coordinate(ctx, ll, mdb, tmpdir, firstItem, profile, s3client, validSegments)
}

// fetchMetricSegs retrieves the MetricSeg records corresponding to the claimed work items.
// All work items must have the same organization, dateint, frequency, and instance.
func fetchMetricSegs(ctx context.Context, db compactionStore, claimedWork []lrdb.ClaimMetricCompactionWorkRow) ([]lrdb.MetricSeg, error) {
	if len(claimedWork) == 0 {
		return nil, nil
	}

	firstItem := claimedWork[0]

	// Extract segment IDs from claimed work
	segmentIDs := make([]int64, len(claimedWork))
	for i, item := range claimedWork {
		segmentIDs[i] = item.SegmentID
	}

	// Query actual segments from database
	segments, err := db.GetMetricSegsForCompactionWork(ctx, lrdb.GetMetricSegsForCompactionWorkParams{
		OrganizationID: firstItem.OrganizationID,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    int32(firstItem.FrequencyMs),
		InstanceNum:    firstItem.InstanceNum,
		SegmentIds:     segmentIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metric segments: %w", err)
	}

	return segments, nil
}
