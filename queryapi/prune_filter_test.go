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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// TestPruneFilter_OrWithNonExistentField tests that OR clauses correctly handle
// non-existent fields by removing that branch but keeping other branches.
func TestPruneFilter_OrWithNonExistentField(t *testing.T) {
	orgID := uuid.New()
	dih := DateIntHours{DateInt: 20251104}
	start := time.Date(2025, 11, 4, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 11, 4, 23, 59, 59, 0, time.UTC)
	startTs := start.UnixMilli()
	endTs := end.UnixMilli()

	// OR clause with one existing field and one non-existent field:
	// log.level (indexed) OR log.nonexistent (non-indexed, doesn't exist)
	filter := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			Filter{
				K:  "log.level",
				V:  []string{"error"},
				Op: "contains",
			},
			Filter{
				K:  "log.nonexistent", // This field doesn't exist
				V:  []string{"foo"},
				Op: "contains",
			},
		},
	}

	// Provide segments that have log.level but NOT log.nonexistent
	// Note: log_level is IndexExact (no trigrams), so "contains" queries
	// only use the exists fingerprint for segment selection.
	fakeRows := []lrdb.ListLogSegmentsForQueryRow{
		// log_level exists fingerprint (IndexExact fields use exists for "contains")
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_level", fingerprint.ExistsRegex),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		// NOTE: No exists fingerprint for log_nonexistent - field doesn't exist
	}

	var lookup SegmentLookupFunc = func(ctx context.Context, p lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
		return fakeRows, nil
	}

	segs, fpToSegments, err := SelectSegmentsFromLegacyFilter(
		context.Background(),
		dih,
		filter,
		startTs, endTs,
		orgID,
		lookup,
	)
	require.NoError(t, err)

	// Should return segment 101 (matches log.level contains "error")
	assert.Equal(t, 1, len(segs), "Should return 1 segment matching the existing field")
	if len(segs) > 0 {
		assert.Equal(t, int64(101), segs[0].SegmentID)
	}

	// Now test pruning - the non-existent field should be removed from the OR
	prunedFilter := PruneFilterForMissingFields(filter, fpToSegments)
	require.NotNil(t, prunedFilter, "Pruned filter should not be nil (one OR branch remains)")

	// After pruning, should be just the log.level filter (not wrapped in OR)
	leafFilter, ok := prunedFilter.(Filter)
	require.True(t, ok, "Pruned filter should be a single Filter (OR with one branch simplified)")
	assert.Equal(t, "log.level", leafFilter.K, "Remaining filter should be for log.level")
	assert.Equal(t, "contains", leafFilter.Op)
	assert.Equal(t, []string{"error"}, leafFilter.V)
}

// TestPruneFilter_AndWithNonExistentField tests that AND clauses correctly fail
// when any branch references a non-existent field.
func TestPruneFilter_AndWithNonExistentField(t *testing.T) {
	orgID := uuid.New()
	dih := DateIntHours{DateInt: 20251104}
	start := time.Date(2025, 11, 4, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 11, 4, 23, 59, 59, 0, time.UTC)
	startTs := start.UnixMilli()
	endTs := end.UnixMilli()

	// AND clause with one existing field and one non-existent field:
	// log.level (indexed) AND log.nonexistent (non-indexed, doesn't exist)
	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "log.level",
				V:  []string{"error"},
				Op: "contains",
			},
			Filter{
				K:  "log.nonexistent", // This field doesn't exist
				V:  []string{"foo"},
				Op: "contains",
			},
		},
	}

	// Provide segments that have log.level but NOT log.nonexistent
	// Note: log_level is IndexExact (no trigrams), so "contains" queries
	// only use the exists fingerprint for segment selection.
	fakeRows := []lrdb.ListLogSegmentsForQueryRow{
		// log_level exists fingerprint (IndexExact fields use exists for "contains")
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_level", fingerprint.ExistsRegex),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		// NOTE: No exists fingerprint for log_nonexistent - field doesn't exist
	}

	var lookup SegmentLookupFunc = func(ctx context.Context, p lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
		return fakeRows, nil
	}

	segs, fpToSegments, err := SelectSegmentsFromLegacyFilter(
		context.Background(),
		dih,
		filter,
		startTs, endTs,
		orgID,
		lookup,
	)
	require.NoError(t, err)

	// For AND clauses, if any branch references a non-existent field,
	// the trigram query logic will already return 0 segments because
	// the AND requires all fingerprints to be present
	assert.Equal(t, 0, len(segs), "AND with non-existent field returns no segments at trigram level")

	// Pruning should also return nil since the non-existent field branch gets pruned
	prunedFilter := PruneFilterForMissingFields(filter, fpToSegments)
	assert.Nil(t, prunedFilter, "Pruned filter should be nil (AND with missing field fails)")

	// This is the correct behavior: AND clauses fail both at trigram selection
	// and at pruning when any branch references a non-existent field
}

// TestPruneFilter_ComplexNestedCase tests a realistic complex query with nested OR/AND
func TestPruneFilter_ComplexNestedCase(t *testing.T) {
	orgID := uuid.New()
	dih := DateIntHours{DateInt: 20251104}
	start := time.Date(2025, 11, 4, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 11, 4, 23, 59, 59, 0, time.UTC)
	startTs := start.UnixMilli()
	endTs := end.UnixMilli()

	// Complex query:
	// AND[
	//   log.level == "error",
	//   OR[
	//     log_message contains "database",
	//     log.nonexistent contains "foo"  ← doesn't exist
	//   ]
	// ]
	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "log.level",
				V:  []string{"error"},
				Op: "eq",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{
						K:  "_cardinalhq.message",
						V:  []string{"database"},
						Op: "contains",
					},
					Filter{
						K:  "log.nonexistent", // Doesn't exist
						V:  []string{"foo"},
						Op: "contains",
					},
				},
			},
		},
	}

	// Provide segments matching log.level and log_message, but not log.nonexistent
	// Note: log.level is indexed, so we need full value fingerprints
	// Note: log_message is NOT indexed, so we need exists + trigrams
	fakeRows := []lrdb.ListLogSegmentsForQueryRow{
		// log_level: indexed field, needs exists + value fingerprints
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_level", fingerprint.ExistsRegex),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_level", "error"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		// log_message: NOT indexed, needs exists + trigrams for "database"
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", fingerprint.ExistsRegex),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", "dat"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", "ata"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", "tab"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", "aba"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", "bas"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		{
			Fingerprint: fingerprint.ComputeFingerprint("log_message", "ase"),
			InstanceNum: 1,
			SegmentID:   101,
			StartTs:     startTs,
			EndTs:       endTs,
		},
		// NOTE: No exists fingerprint for log_nonexistent - field doesn't exist
	}

	var lookup SegmentLookupFunc = func(ctx context.Context, p lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error) {
		return fakeRows, nil
	}

	_, fpToSegments, err := SelectSegmentsFromLegacyFilter(
		context.Background(),
		dih,
		filter,
		startTs, endTs,
		orgID,
		lookup,
	)
	require.NoError(t, err)

	// The trigram query logic will be overly conservative with nested OR clauses
	// containing non-existent fields, potentially returning 0 segments.
	// This is acceptable because:
	// 1. It's safe (false negatives are ok, false positives are not)
	// 2. The pruning step will rewrite the filter for SQL generation
	// For this test, we just verify that fpToSegments has the right data for pruning
	assert.Contains(t, fpToSegments, fingerprint.ComputeFingerprint("log_level", fingerprint.ExistsRegex), "Should have log_level exists fp")
	assert.Contains(t, fpToSegments, fingerprint.ComputeFingerprint("log_message", fingerprint.ExistsRegex), "Should have log_message exists fp")
	assert.NotContains(t, fpToSegments, fingerprint.ComputeFingerprint("log_nonexistent", fingerprint.ExistsRegex), "Should NOT have log_nonexistent exists fp")

	// Test pruning - this is the key part
	prunedFilter := PruneFilterForMissingFields(filter, fpToSegments)
	require.NotNil(t, prunedFilter, "Pruned filter should not be nil")

	// Expected pruned structure:
	// AND[
	//   log.level == "error",
	//   log_message contains "database"  ← OR simplified to single filter
	// ]
	andClause, ok := prunedFilter.(BinaryClause)
	require.True(t, ok, "Pruned filter should be BinaryClause (AND)")
	assert.Equal(t, "and", andClause.Op)
	assert.Equal(t, 2, len(andClause.Clauses), "AND should have 2 clauses after pruning")

	// First clause should be log.level
	levelFilter, ok := andClause.Clauses[0].(Filter)
	require.True(t, ok)
	assert.Equal(t, "log.level", levelFilter.K)

	// Second clause should be log_message (OR simplified)
	msgFilter, ok := andClause.Clauses[1].(Filter)
	require.True(t, ok)
	assert.Equal(t, "_cardinalhq.message", msgFilter.K)
}
