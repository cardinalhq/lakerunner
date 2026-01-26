// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package config

import "time"

// MetricRollupFrequencies defines the rollup frequency transitions
var MetricRollupFrequencies = map[int32]int32{
	10_000:    60_000,    // 10 seconds -> 1 minute
	60_000:    300_000,   // 1 minute -> 5 minutes
	300_000:   1_200_000, // 5 minutes -> 20 minutes
	1_200_000: 3_600_000, // 20 minutes -> 1 hour
}

// RollupAccumulationTimes defines the maximum accumulation time for each source frequency
var RollupAccumulationTimes = map[int32]time.Duration{
	10_000:    90 * time.Second,  // 10s->60s: wait max 90 seconds
	60_000:    200 * time.Second, // 60s->300s: wait max 200 seconds
	300_000:   5 * time.Minute,   // 5m->20m: wait max 5 minutes
	1_200_000: 5 * time.Minute,   // 20m->1h: wait max 5 minutes
}

// GetRollupAccumulationTime returns the accumulation time for a given source frequency
func GetRollupAccumulationTime(sourceFrequencyMs int32) time.Duration {
	if duration, exists := RollupAccumulationTimes[sourceFrequencyMs]; exists {
		return duration
	}
	// Default to 5 minutes for unknown frequencies
	return 5 * time.Minute
}

// GetTargetRollupFrequency returns the target frequency for a given source frequency
func GetTargetRollupFrequency(sourceFrequencyMs int32) (int32, bool) {
	target, exists := MetricRollupFrequencies[sourceFrequencyMs]
	return target, exists
}

// IsRollupSourceFrequency checks if a frequency is a valid source for rollups
func IsRollupSourceFrequency(frequencyMs int32) bool {
	_, exists := MetricRollupFrequencies[frequencyMs]
	return exists
}

// RollupSourceFrequencies maps target frequency back to source frequency
var RollupSourceFrequencies = map[int32]int32{
	60_000:    10_000,    // 1 minute is from 10 seconds
	300_000:   60_000,    // 5 minutes is from 1 minute
	1_200_000: 300_000,   // 20 minutes is from 5 minutes
	3_600_000: 1_200_000, // 1 hour is from 20 minutes
}

// AcceptedMetricFrequencies contains all valid metric frequencies
var AcceptedMetricFrequencies = []int32{
	10_000,    // 10 seconds
	60_000,    // 1 minute
	300_000,   // 5 minutes
	1_200_000, // 20 minutes
	3_600_000, // 1 hour
}
