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
