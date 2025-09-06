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

package metricsprocessing

import (
	"context"

	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ConvertResultsToProcessedSegments converts parquet writer results to ProcessedSegments
// This is used when we already have the metadata available from the context
func ConvertResultsToProcessedSegments(
	ctx context.Context,
	results []parquetwriter.Result,
	pctx ProcessingContext,
	collectorName string,
) (ProcessedSegments, error) {
	segments := make(ProcessedSegments, 0, len(results))

	for _, result := range results {
		segment, err := NewProcessedSegment(ctx, result, pctx.OrganizationID, collectorName)
		if err != nil {
			return nil, err
		}
		segments = append(segments, segment)
	}

	return segments, nil
}

// GetRollupTargetFrequency calculates the target frequency for rollup based on source frequency
func GetRollupTargetFrequency(sourceFrequencyMs int32) int32 {
	switch sourceFrequencyMs {
	case 1000: // 1s -> 10s
		return 10000
	case 10000: // 10s -> 1m
		return 60000
	case 60000: // 1m -> 5m
		return 300000
	case 300000: // 5m -> 30m
		return 1800000
	case 1800000: // 30m -> 1h
		return 3600000
	default:
		// For any other frequency, multiply by 10 up to 1 hour max
		targetFreq := sourceFrequencyMs * 10
		if targetFreq > 3600000 {
			return 3600000
		}
		return targetFreq
	}
}

// GetCompactionTargetRecords returns the target number of records for compaction
func GetCompactionTargetRecords(frequencyMs int32) int64 {
	// Based on frequency, return target records per file
	switch {
	case frequencyMs <= 1000: // 1s frequency
		return 500000
	case frequencyMs <= 10000: // 10s frequency
		return 300000
	case frequencyMs <= 60000: // 1m frequency
		return 200000
	default: // 5m+ frequency
		return 100000
	}
}

// GetRollupTargetRecords returns the target number of records for rollup
func GetRollupTargetRecords(targetFrequencyMs int32) int64 {
	// Based on target frequency, return target records per file
	switch {
	case targetFrequencyMs <= 10000: // 10s frequency
		return 400000
	case targetFrequencyMs <= 60000: // 1m frequency
		return 250000
	case targetFrequencyMs <= 300000: // 5m frequency
		return 150000
	default: // 30m+ frequency
		return 100000
	}
}

// GetCreatedByForAction returns the appropriate CreatedBy value for an action
func GetCreatedByForAction(action string) lrdb.CreatedBy {
	switch action {
	case "compact":
		return lrdb.CreatedByCompact
	case "rollup":
		return lrdb.CreateByRollup
	default:
		return lrdb.CreatedByIngest
	}
}
