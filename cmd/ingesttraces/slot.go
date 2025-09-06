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

package ingesttraces

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// NumTracePartitions is the number of partitions/slots for trace processing.
// Can be configured via LAKERUNNER_TRACE_PARTITIONS environment variable, defaults to 1.
// Compaction compacts all files in a slot - so increase this to increase parallelism.
// However, more slots means more individual files, so for smaller customers it's better to keep it low.
// This should be configured from the parent cmd package that uses this.
var NumTracePartitions = 1

// SetNumTracePartitions sets the number of trace partitions
func SetNumTracePartitions(n int) {
	if n > 0 {
		NumTracePartitions = n
	}
}

// determineSlot determines which partition slot a trace should go to.
// This ensures that the same trace ID always goes to the same slot for consistency.
func DetermineTraceSlot(traceID string, dateint int32, orgID string) int {
	// Create a unique key combining trace ID, dateint, and orgID
	key := fmt.Sprintf("%s_%d_%s", traceID, dateint, orgID)

	// Hash the key to get a deterministic slot assignment
	h := sha256.Sum256([]byte(key))

	// Use the first 2 bytes of the hash to get a 16-bit number, then modulo by partition count
	return int(binary.BigEndian.Uint16(h[:])) % NumTracePartitions
}
