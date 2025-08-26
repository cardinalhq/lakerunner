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

package helpers

import (
	"slices"

	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	// Map the current frequency into the ones we feed to.
	RollupNotifications = map[int32]int32{
		10_000:    60_000,    // 10 seconds feeds 1m
		60_000:    300_000,   // 1 minute feeds 5m
		300_000:   1_200_000, // 5 minutes feeds 15m
		1_200_000: 3_600_000, // 20 minutes feeds 1h
	}

	// Map our current frequency into the one we feed from.
	RollupSources = map[int32]int32{
		60_000:    10_000,    // 1 minute is from 10 seconds
		300_000:   60_000,    // 5 minutes is from 1 minute
		1_200_000: 300_000,   // 20 minutes is from 5 minutes
		3_600_000: 1_200_000, // 1 hour is from 20 minutes
	}

	AcceptedMetricFrequencies = []int32{
		10_000,    // 10 seconds
		60_000,    // 1 minute
		300_000,   // 5 minutes
		1_200_000, // 20 minutes
		3_600_000, // 1 hour
	}
)

func IsWantedFrequency(frequency int32) bool {
	return slices.Contains(AcceptedMetricFrequencies, frequency)
}

// AllRolledUp returns true all rows are rolledup.
// If the slice is empty, the return value is junk.
func AllRolledUp(rows []lrdb.MetricSeg) bool {
	for _, row := range rows {
		if !row.Rolledup {
			return false
		}
	}
	return true
}
