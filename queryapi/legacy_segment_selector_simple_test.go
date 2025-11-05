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

package queryapi

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// TestSelectSegmentsFromLegacyFilter_SimpleOr tests a simple OR with two branches
// where only one branch matches. This is the core bug: OR should return union, not intersection.
func TestSelectSegmentsFromLegacyFilter_SimpleOr(t *testing.T) {
	orgID := uuid.New()
	dih := DateIntHours{DateInt: 20251104}
	start := time.Date(2025, 11, 4, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 11, 4, 23, 59, 59, 0, time.UTC)
	startTs := start.UnixMilli()
	endTs := end.UnixMilli()

	// Simple OR: level=="error" OR level=="debug"
	// We'll provide segments matching "error" but not "debug"
	filter := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			Filter{
				K:  "log.level",
				V:  []string{"error"},
				Op: "eq",
			},
			Filter{
				K:  "log.level",
				V:  []string{"debug"},
				Op: "eq",
			},
		},
	}

	//  For "eq" on non-full-value dimensions, we use trigram matching
	// Pattern for "error" → lots of trigrams like "err", "rro", "ror"
	// We'll provide a segment that has those trigrams

	fakeRows := []lrdb.ListLogSegmentsForQueryRow{
		// Trigrams for "error"
		{
			Fingerprint: computeFingerprint("log_level", "err"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: computeFingerprint("log_level", "rro"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: computeFingerprint("log_level", "ror"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		// NOTE: No trigrams for "debug", simulating that no segments match it
	}

	var lookup SegmentLookupFunc = func(ctx context.Context, p lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
		return fakeRows, nil
	}

	segs, err := SelectSegmentsFromLegacyFilter(
		context.Background(),
		dih,
		filter,
		startTs, endTs,
		orgID,
		lookup,
	)
	if err != nil {
		t.Fatalf("SelectSegmentsFromLegacyFilter error: %v", err)
	}

	// The key test: even though "debug" matches nothing, the OR should still return
	// the segment matching "error"
	if len(segs) != 1 {
		t.Errorf("Expected 1 segment (matching 'error'), got %d", len(segs))
	}

	if len(segs) > 0 && segs[0].SegmentID != 101 {
		t.Errorf("Expected segment 101, got %d", segs[0].SegmentID)
	}
}
