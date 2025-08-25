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

package ingestmetrics

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"strconv"
)

// NumMetricPartitions is the number of partitions/slots for metric processing.
// Can be configured via LAKERUNNER_METRIC_PARTITIONS environment variable, defaults to 1.
// Compaction compacts all files in a slot - so increase this to increase parallelism.
// However, more slots means more individual files, so for smaller customers it's better to keep it low.
var NumMetricPartitions = func() int {
	if partitionsStr := os.Getenv("LAKERUNNER_METRIC_PARTITIONS"); partitionsStr != "" {
		if partitions, err := strconv.Atoi(partitionsStr); err == nil && partitions > 0 {
			return partitions
		}
		// Log warning if invalid value, fall back to default
		slog.Warn("Invalid LAKERUNNER_METRIC_PARTITIONS value, using default",
			"value", partitionsStr, "default", 1)
	}
	return 1
}()

// DetermineMetricSlot determines which partition slot a metric should go to.
// This ensures that metrics with similar characteristics go to the same slot for consistency.
func DetermineMetricSlot(frequencyMs int32, dateint int32, orgID string) int {
	// Create a unique key combining frequency, dateint, and orgID
	key := fmt.Sprintf("%d_%d_%s", frequencyMs, dateint, orgID)

	// Hash the key
	hash := sha256.Sum256([]byte(key))

	// Convert first 8 bytes of hash to uint64
	hashValue := binary.BigEndian.Uint64(hash[:8])

	// Return slot index
	return int(hashValue % uint64(NumMetricPartitions))
}
