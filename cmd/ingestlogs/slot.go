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

package ingestlogs

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strconv"
)

// NumLogPartitions is the number of partitions/slots for log processing.
// Can be configured via LAKERUNNER_LOG_PARTITIONS environment variable, defaults to 1.
// Compaction compacts all files in a slot - so increase this to increase parallelism.
// However, more slots means more individual files, so for smaller customers it's better to keep it low.
var NumLogPartitions = func() int {
	if partitionsStr := os.Getenv("LAKERUNNER_LOG_PARTITIONS"); partitionsStr != "" {
		if partitions, err := strconv.Atoi(partitionsStr); err == nil && partitions > 0 {
			return partitions
		}
		// Log warning if invalid value, fall back to default
		slog.Warn("Invalid LAKERUNNER_LOG_PARTITIONS value, using default",
			"value", partitionsStr, "default", 1)
	}
	return 1
}()

// DetermineLogSlot determines which partition slot a log should go to.
// This ensures that logs with similar content characteristics go to the same slot for consistency.
func DetermineLogSlot(fingerprint int64, dateint int32, orgID string) int {
	// Create a unique key combining fingerprint, dateint, and orgID
	key := fmt.Sprintf("%d_%d_%s", fingerprint, dateint, orgID)

	// Hash the key
	hash := sha256.Sum256([]byte(key))

	// Convert first 8 bytes of hash to uint64
	hashValue := binary.BigEndian.Uint64(hash[:8])

	// Return slot index
	return int(hashValue % uint64(NumLogPartitions))
}
