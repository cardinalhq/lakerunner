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

package logcrunch

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// PackingStrategy represents different approaches to grouping segments
type PackingStrategy int

const (
	GreedyStrategy PackingStrategy = iota // Current approach
	BestFitStrategy                       // Find best fitting group for each segment
	FirstFitStrategy                      // Find first group that fits
	SortedBestFitStrategy                 // Sort by size, then best fit
	LargestFirstStrategy                  // Pack largest segments first
)

// PackingResult contains the results of a packing strategy
type PackingResult struct {
	Strategy     PackingStrategy
	Groups       [][]lrdb.GetLogSegmentsForCompactionRow
	GroupCount   int
	UtilizationPercentage float64 // How well groups fill the available capacity
	LargestWaste int64            // Largest unused capacity in any group
}

// packSegmentsWithStrategy applies different packing strategies to the same segments
func packSegmentsWithStrategy(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64, strategy PackingStrategy) PackingResult {
	// Filter segments like the original function
	validSegments := filterAndValidateSegments(segments)
	
	var groups [][]lrdb.GetLogSegmentsForCompactionRow
	
	switch strategy {
	case GreedyStrategy:
		groups = packGreedy(validSegments, estimatedRecordCount)
	case BestFitStrategy:
		groups = packBestFit(validSegments, estimatedRecordCount)
	case FirstFitStrategy:
		groups = packFirstFit(validSegments, estimatedRecordCount)
	case SortedBestFitStrategy:
		groups = packSortedBestFit(validSegments, estimatedRecordCount)
	case LargestFirstStrategy:
		groups = packLargestFirst(validSegments, estimatedRecordCount)
	default:
		groups = packGreedy(validSegments, estimatedRecordCount)
	}
	
	// Calculate utilization metrics
	utilization, largestWaste := calculateUtilization(groups, estimatedRecordCount)
	
	return PackingResult{
		Strategy:              strategy,
		Groups:               groups,
		GroupCount:           len(groups),
		UtilizationPercentage: utilization,
		LargestWaste:         largestWaste,
	}
}

// filterAndValidateSegments applies the same filtering as original PackSegments
func filterAndValidateSegments(segments []lrdb.GetLogSegmentsForCompactionRow) []lrdb.GetLogSegmentsForCompactionRow {
	// Apply same filtering as PackSegments
	segments = filterSegments(segments)
	if len(segments) == 0 {
		return segments
	}
	
	segments = filterHourConformingSegments(segments)
	if len(segments) == 0 {
		return segments
	}
	
	// Additional validation from PackSegments
	validSegments := make([]lrdb.GetLogSegmentsForCompactionRow, 0, len(segments))
	var hour int64 = -1
	
	for _, seg := range segments {
		if seg.StartTs >= seg.EndTs {
			continue
		}
		if seg.EndTs == -9223372036854775808 {
			continue
		}
		
		endMinusOne := seg.EndTs - 1
		segStartHour := hourFromMillis(seg.StartTs)
		segEndHour := hourFromMillis(endMinusOne)
		
		if segStartHour != segEndHour {
			continue
		}
		
		if hour == -1 {
			hour = segStartHour
		} else if segStartHour != hour {
			continue
		}
		
		validSegments = append(validSegments, seg)
	}
	
	return validSegments
}

// packGreedy implements the current greedy strategy
func packGreedy(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) [][]lrdb.GetLogSegmentsForCompactionRow {
	groups := make([][]lrdb.GetLogSegmentsForCompactionRow, 0, len(segments)/2+1)
	current := make([]lrdb.GetLogSegmentsForCompactionRow, 0, 8)
	var sumRecords int64
	
	for _, seg := range segments {
		rc := seg.RecordCount
		if len(current) == 0 || sumRecords+rc <= estimatedRecordCount {
			current = append(current, seg)
			sumRecords += rc
			continue
		}
		groups = append(groups, current)
		current = []lrdb.GetLogSegmentsForCompactionRow{seg}
		sumRecords = rc
	}
	if len(current) > 0 {
		groups = append(groups, current)
	}
	return groups
}

// packBestFit finds the best fitting existing group for each segment
func packBestFit(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) [][]lrdb.GetLogSegmentsForCompactionRow {
	var groups [][]lrdb.GetLogSegmentsForCompactionRow
	groupSums := []int64{}
	
	for _, seg := range segments {
		rc := seg.RecordCount
		bestGroupIdx := -1
		bestRemainingSpace := estimatedRecordCount + 1 // Larger than possible
		
		// Find the group with the least remaining space that can still fit this segment
		for i, groupSum := range groupSums {
			if groupSum+rc <= estimatedRecordCount {
				remainingSpace := estimatedRecordCount - (groupSum + rc)
				if remainingSpace < bestRemainingSpace {
					bestRemainingSpace = remainingSpace
					bestGroupIdx = i
				}
			}
		}
		
		if bestGroupIdx != -1 {
			// Add to existing group
			groups[bestGroupIdx] = append(groups[bestGroupIdx], seg)
			groupSums[bestGroupIdx] += rc
		} else {
			// Create new group
			groups = append(groups, []lrdb.GetLogSegmentsForCompactionRow{seg})
			groupSums = append(groupSums, rc)
		}
	}
	
	return groups
}

// packFirstFit finds the first group that can fit each segment
func packFirstFit(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) [][]lrdb.GetLogSegmentsForCompactionRow {
	var groups [][]lrdb.GetLogSegmentsForCompactionRow
	groupSums := []int64{}
	
	for _, seg := range segments {
		rc := seg.RecordCount
		placed := false
		
		// Find the first group that can fit this segment
		for i, groupSum := range groupSums {
			if groupSum+rc <= estimatedRecordCount {
				groups[i] = append(groups[i], seg)
				groupSums[i] += rc
				placed = true
				break
			}
		}
		
		if !placed {
			// Create new group
			groups = append(groups, []lrdb.GetLogSegmentsForCompactionRow{seg})
			groupSums = append(groupSums, rc)
		}
	}
	
	return groups
}

// packSortedBestFit sorts segments by size descending, then applies best fit
func packSortedBestFit(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) [][]lrdb.GetLogSegmentsForCompactionRow {
	// Sort segments by record count descending
	sortedSegments := make([]lrdb.GetLogSegmentsForCompactionRow, len(segments))
	copy(sortedSegments, segments)
	sort.Slice(sortedSegments, func(i, j int) bool {
		return sortedSegments[i].RecordCount > sortedSegments[j].RecordCount
	})
	
	return packBestFit(sortedSegments, estimatedRecordCount)
}

// packLargestFirst sorts by size descending and uses greedy packing
func packLargestFirst(segments []lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) [][]lrdb.GetLogSegmentsForCompactionRow {
	// Sort segments by record count descending
	sortedSegments := make([]lrdb.GetLogSegmentsForCompactionRow, len(segments))
	copy(sortedSegments, segments)
	sort.Slice(sortedSegments, func(i, j int) bool {
		return sortedSegments[i].RecordCount > sortedSegments[j].RecordCount
	})
	
	return packGreedy(sortedSegments, estimatedRecordCount)
}

// calculateUtilization computes how efficiently groups use their available capacity
func calculateUtilization(groups [][]lrdb.GetLogSegmentsForCompactionRow, estimatedRecordCount int64) (float64, int64) {
	if len(groups) == 0 {
		return 0, 0
	}
	
	totalUsed := int64(0)
	totalCapacity := int64(len(groups)) * estimatedRecordCount
	largestWaste := int64(0)
	
	for _, group := range groups {
		groupSum := int64(0)
		for _, seg := range group {
			groupSum += seg.RecordCount
		}
		totalUsed += groupSum
		waste := estimatedRecordCount - groupSum
		if waste > largestWaste {
			largestWaste = waste
		}
	}
	
	return float64(totalUsed) / float64(totalCapacity) * 100, largestWaste
}

// TestPackingStrategyComparison compares different packing strategies
func TestPackingStrategyComparison(t *testing.T) {
	// Use the same production data from the other test
	productionSegments := []lrdb.GetLogSegmentsForCompactionRow{
		// Very small segments (1-10 records)
		{SegmentID: 298474784809812455, StartTs: 1755741609646, EndTs: 1755741613411, RecordCount: 4, FileSize: 16457},
		{SegmentID: 298474816099320295, StartTs: 1755741637915, EndTs: 1755741637953, RecordCount: 2, FileSize: 16306},
		{SegmentID: 298474833514070503, StartTs: 1755741643093, EndTs: 1755741643136, RecordCount: 2, FileSize: 16138},
		{SegmentID: 298474885036900839, StartTs: 1755741669645, EndTs: 1755741669646, RecordCount: 1, FileSize: 15990},
		{SegmentID: 298474881480131047, StartTs: 1755741673188, EndTs: 1755741673333, RecordCount: 13, FileSize: 17334},
		{SegmentID: 298474935167222247, StartTs: 1755741697915, EndTs: 1755741703117, RecordCount: 7, FileSize: 17304},
		{SegmentID: 298474985800860135, StartTs: 1755741729645, EndTs: 1755741729646, RecordCount: 1, FileSize: 15990},
		{SegmentID: 298474986891379175, StartTs: 1755741733195, EndTs: 1755741733420, RecordCount: 3, FileSize: 16375},

		// Small to medium segments (100-1000 records)
		{SegmentID: 298474768468804071, StartTs: 1755741600001, EndTs: 1755741609642, RecordCount: 594, FileSize: 71692},
		{SegmentID: 298474819286991335, StartTs: 1755741614640, EndTs: 1755741638941, RecordCount: 638, FileSize: 67062},
		{SegmentID: 298474852891754983, StartTs: 1755741635333, EndTs: 1755741659033, RecordCount: 662, FileSize: 67623},
		{SegmentID: 298474869366981095, StartTs: 1755741644936, EndTs: 1755741669129, RecordCount: 893, FileSize: 75646},
		{SegmentID: 298474882302214631, StartTs: 1755741657039, EndTs: 1755741678151, RecordCount: 556, FileSize: 65548},

		// Medium segments (1000-2000 records)
		{SegmentID: 298474769995530727, StartTs: 1755741600005, EndTs: 1755741608644, RecordCount: 1346, FileSize: 78081},
		{SegmentID: 298474786906964455, StartTs: 1755741600029, EndTs: 1755741619738, RecordCount: 1087, FileSize: 76721},
		{SegmentID: 298474785967440359, StartTs: 1755741601236, EndTs: 1755741619005, RecordCount: 1589, FileSize: 93291},
		{SegmentID: 298474798550352359, StartTs: 1755741603929, EndTs: 1755741629735, RecordCount: 1089, FileSize: 73296},
		{SegmentID: 298474835795771879, StartTs: 1755741624129, EndTs: 1755741647939, RecordCount: 1316, FileSize: 90518},
		{SegmentID: 298474834889802215, StartTs: 1755741624931, EndTs: 1755741648936, RecordCount: 2184, FileSize: 105720},

		// Larger segments (2000+ records)
		{SegmentID: 298474799942861287, StartTs: 1755741604534, EndTs: 1755741629010, RecordCount: 2138, FileSize: 113018},
		{SegmentID: 298474817575715303, StartTs: 1755741614029, EndTs: 1755741639941, RecordCount: 2599, FileSize: 116663},
		{SegmentID: 298474852287775207, StartTs: 1755741634253, EndTs: 1755741658236, RecordCount: 2316, FileSize: 115292},
		{SegmentID: 298474868477788647, StartTs: 1755741642851, EndTs: 1755741669145, RecordCount: 2328, FileSize: 111414},
	}

	// Test different thresholds
	thresholds := []struct {
		name      string
		threshold int64
	}{
		{"High threshold (20k records)", 20000},
		{"Medium threshold (10k records)", 10000},
		{"Low threshold (5k records)", 5000},
	}

	strategies := []PackingStrategy{
		GreedyStrategy,
		BestFitStrategy,
		FirstFitStrategy,
		SortedBestFitStrategy,
		LargestFirstStrategy,
	}

	strategyNames := map[PackingStrategy]string{
		GreedyStrategy:        "Greedy (current)",
		BestFitStrategy:       "Best Fit",
		FirstFitStrategy:      "First Fit",
		SortedBestFitStrategy: "Sorted Best Fit",
		LargestFirstStrategy:  "Largest First",
	}

	for _, threshold := range thresholds {
		t.Run(threshold.name, func(t *testing.T) {
			t.Logf("=== Comparing strategies with %s ===", threshold.name)
			
			var results []PackingResult
			
			// Test each strategy
			for _, strategy := range strategies {
				result := packSegmentsWithStrategy(productionSegments, threshold.threshold, strategy)
				results = append(results, result)
				
				t.Logf("%s: %d groups, %.1f%% utilization, largest waste: %d records",
					strategyNames[strategy], result.GroupCount, result.UtilizationPercentage, result.LargestWaste)
			}
			
			// Find best strategy by different metrics
			bestByGroups := findBestByMetric(results, func(r PackingResult) float64 { return -float64(r.GroupCount) })
			bestByUtilization := findBestByMetric(results, func(r PackingResult) float64 { return r.UtilizationPercentage })
			bestByWaste := findBestByMetric(results, func(r PackingResult) float64 { return -float64(r.LargestWaste) })
			
			t.Logf("Best by group count: %s (%d groups)", strategyNames[bestByGroups.Strategy], bestByGroups.GroupCount)
			t.Logf("Best by utilization: %s (%.1f%%)", strategyNames[bestByUtilization.Strategy], bestByUtilization.UtilizationPercentage)
			t.Logf("Best by least waste: %s (%d waste)", strategyNames[bestByWaste.Strategy], bestByWaste.LargestWaste)
			
			// Ensure all strategies produce valid results
			for _, result := range results {
				assert.Greater(t, result.GroupCount, 0, "Strategy %s should produce at least one group", strategyNames[result.Strategy])
				assert.LessOrEqual(t, result.UtilizationPercentage, 100.0, "Utilization should not exceed 100%%")
				
				// Verify no group exceeds threshold
				for i, group := range result.Groups {
					groupSum := int64(0)
					for _, seg := range group {
						groupSum += seg.RecordCount
					}
					assert.LessOrEqual(t, groupSum, threshold.threshold, 
						"Group %d in strategy %s exceeds threshold", i, strategyNames[result.Strategy])
				}
			}
		})
	}
}

// findBestByMetric finds the result with the best metric score (higher is better)
func findBestByMetric(results []PackingResult, metric func(PackingResult) float64) PackingResult {
	if len(results) == 0 {
		return PackingResult{}
	}
	
	best := results[0]
	bestScore := metric(best)
	
	for _, result := range results[1:] {
		score := metric(result)
		if score > bestScore {
			best = result
			bestScore = score
		}
	}
	
	return best
}