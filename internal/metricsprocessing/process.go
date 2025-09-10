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

// Rollup frequency mappings to avoid import cycle with config package
var rollupFrequencyMappings = map[int32]int32{
	10_000:    60_000,    // 10 seconds -> 1 minute
	60_000:    300_000,   // 1 minute -> 5 minutes
	300_000:   1_200_000, // 5 minutes -> 20 minutes
	1_200_000: 3_600_000, // 20 minutes -> 1 hour
}

// isRollupSourceFrequency checks if a frequency is a valid source for rollups
func isRollupSourceFrequency(frequencyMs int32) bool {
	_, exists := rollupFrequencyMappings[frequencyMs]
	return exists
}

// getTargetRollupFrequency returns the target frequency for a given source frequency
func getTargetRollupFrequency(sourceFrequencyMs int32) (int32, bool) {
	target, exists := rollupFrequencyMappings[sourceFrequencyMs]
	return target, exists
}
