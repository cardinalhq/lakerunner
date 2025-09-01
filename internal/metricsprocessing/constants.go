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

const (
	// DefaultRecordsPerFileCompaction is the default records per file for compaction operations
	DefaultRecordsPerFileCompaction = int64(40_000)

	// DefaultRecordsPerFileRollup is the default records per file for rollup operations
	DefaultRecordsPerFileRollup = int64(10_000)
)
