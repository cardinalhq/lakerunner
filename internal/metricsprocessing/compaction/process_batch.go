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
	bundle lrdb.CompactionBundleResult,
) error {
	if len(bundle.Items) == 0 {
		return nil
	}

	// Generate batch ID and enhance logger
	batchID := idgen.GenerateShortBase32ID()
	ll = ll.With(slog.String("batchID", batchID))

	// Log work items we're processing once at the top
	workItemIDs := make([]int64, len(bundle.Items))
	for i, item := range bundle.Items {
		workItemIDs[i] = item.ID
	}
	ll.Debug("Processing compaction batch",
		slog.Int("workItemCount", len(bundle.Items)),
		slog.Any("workItemIDs", workItemIDs),
		slog.Int64("estimatedTarget", bundle.EstimatedTarget))

	// Safety check: All work items in a batch must have identical grouping fields
	// For bundle-based approach, we trust that the bundle selection ensures consistency
	// The bundle approach guarantees all items are from the same org/dateint/frequency/instance
	// TODO: Add validation once we have proper segment metadata access
	firstItem := bundle.Items[0]

	if !helpers.IsWantedFrequency(int32(firstItem.FrequencyMs)) {
		ll.Debug("Skipping compaction for unwanted frequency", slog.Int("frequencyMs", int(firstItem.FrequencyMs)))
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

	tmpdir, err := os.MkdirTemp("", "")
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
		slog.Int("batchSize", len(bundle.Items)),
		slog.Int64("estimatedTarget", bundle.EstimatedTarget))

	segments, err := fetchMetricSegsFromBundle(ctx, mdb, bundle.Items)
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

	return coordinateBundle(ctx, ll, mdb, tmpdir, bundle, profile, s3client, validSegments)
}

// fetchMetricSegsFromBundle retrieves the MetricSeg records corresponding to the bundle items
// by querying segments directly by their IDs.
func fetchMetricSegsFromBundle(ctx context.Context, db compactionStore, items []lrdb.McqFetchCandidatesRow) ([]lrdb.MetricSeg, error) {
	if len(items) == 0 {
		return nil, nil
	}

	segmentIDs := make([]int64, len(items))
	for i, item := range items {
		segmentIDs[i] = item.SegmentID
	}

	segments, err := db.McqGetSegmentsByIds(ctx, segmentIDs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metric segments by IDs: %w", err)
	}

	return segments, nil
}
