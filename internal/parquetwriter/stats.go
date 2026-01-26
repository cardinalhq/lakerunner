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

package parquetwriter

import "github.com/cardinalhq/lakerunner/pipeline"

// StatsAccumulator collects statistics for a single output file.
type StatsAccumulator interface {
	// Add is called once per row written to this file.
	Add(row pipeline.Row)

	// Finalize is called exactly once after the last row.
	// It should return the accumulated statistics for this file.
	Finalize() any
}

// StatsProvider creates StatsAccumulators for collecting file-level statistics.
type StatsProvider interface {
	// NewAccumulator creates a new accumulator for a single output file.
	NewAccumulator() StatsAccumulator
}
