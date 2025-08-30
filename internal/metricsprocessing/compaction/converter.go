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

	"github.com/cardinalhq/lakerunner/lrdb"
)

func FetchMetricSegsForCompaction(ctx context.Context, db CompactionStore, claimedWork []lrdb.ClaimMetricCompactionWorkRow) ([]lrdb.MetricSeg, error) {
	if len(claimedWork) == 0 {
		return nil, nil
	}

	// All work items must have same org/dateint/frequency/instance (safety check should ensure this)
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
