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
	"log/slog"
	"os"
	"strconv"
)

// NumTracePartitions is the number of partitions/slots for trace processing.
// Can be configured via LAKERUNNER_TRACE_PARTITIONS environment variable, defaults to 1.
// Compaction compacts all files in a slot - so increase this to increase parallelism.
// However, more slots means more individual files, so for smaller customers it's better to keep it low.
var NumTracePartitions = func() int {
	if partitionsStr := os.Getenv("LAKERUNNER_TRACE_PARTITIONS"); partitionsStr != "" {
		if partitions, err := strconv.Atoi(partitionsStr); err == nil && partitions > 0 {
			return partitions
		}
		// Log warning if invalid value, fall back to default
		slog.Warn("Invalid LAKERUNNER_TRACE_PARTITIONS value, using default",
			"value", partitionsStr, "default", 1)
	}
	return 1
}()

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
