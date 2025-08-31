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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetAllFrequencies(t *testing.T) {
	frequencies := GetAllFrequencies()

	// Should contain all expected frequencies
	expected := []int32{10000, 60000, 300000, 1200000, 3600000}
	assert.Len(t, frequencies, len(expected))

	// Convert to map for easier checking
	freqMap := make(map[int32]bool)
	for _, freq := range frequencies {
		freqMap[freq] = true
	}

	for _, expectedFreq := range expected {
		assert.True(t, freqMap[expectedFreq], "Should contain frequency %d", expectedFreq)
	}
}

func TestGetFrequencyPriority(t *testing.T) {
	tests := []struct {
		name      string
		frequency int32
		expected  int32
	}{
		{"10 seconds - highest priority", 10_000, 800},
		{"1 minute", 60_000, 600},
		{"5 minutes", 300_000, 400},
		{"20 minutes", 1_200_000, 200},
		{"1 hour - lowest priority", 3_600_000, 0},
		{"invalid frequency", 999_999, 0},
		{"zero frequency", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetFrequencyPriority(tt.frequency)
			assert.Equal(t, tt.expected, result)
		})
	}
}
