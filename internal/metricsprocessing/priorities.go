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
	"slices"
)

var (
	// RollupTo maps source frequency to target rollup frequency
	RollupTo = map[int32]int32{
		10_000:    60_000,    // 10sec -> 1min
		60_000:    300_000,   // 1min -> 5min
		300_000:   1_200_000, // 5min -> 20min
		1_200_000: 3_600_000, // 20min -> 1hour
	}

	// frequencyPriorities maps frequency to base priority value
	frequencyPriorities = map[int32]int32{
		10_000:    800,
		60_000:    600,
		300_000:   400,
		1_200_000: 200,
		3_600_000: 0,
	}
)

// GetFrequencyPriority returns the base priority for a given frequency
func GetFrequencyPriority(frequencyMs int32) int32 {
	if priority, exists := frequencyPriorities[frequencyMs]; exists {
		return priority
	}
	return 0
}

// GetCompactionPriority returns the priority for compaction work at a given frequency
func GetCompactionPriority(frequencyMs int32) int32 {
	return GetFrequencyPriority(frequencyMs) + 200
}

// GetRollupPriority returns the priority for rollup work at a given frequency
func GetRollupPriority(frequencyMs int32) int32 {
	return GetFrequencyPriority(frequencyMs) + 100
}

// GetAllFrequencies returns all frequencies involved in the rollup process
func GetAllFrequencies() []int32 {
	freqSet := make(map[int32]bool)

	for sourceFreq, targetFreq := range RollupTo {
		freqSet[sourceFreq] = true
		freqSet[targetFreq] = true
	}

	var frequencies []int32
	for freq := range freqSet {
		frequencies = append(frequencies, freq)
	}

	slices.Sort(frequencies)
	return frequencies
}
