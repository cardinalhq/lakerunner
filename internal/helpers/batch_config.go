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
	"os"
	"strconv"
	"strings"
)

const (
	DefaultTargetSizeBytes = 1024 * 1024 // 1MB
	DefaultMaxBatchSize    = 20
)

func GetBatchSizeForSignal(signal string) int {
	envVar := "LAKERUNNER_" + strings.ToUpper(signal) + "_BATCH_SIZE"
	if value := os.Getenv(envVar); value != "" {
		if size, err := strconv.Atoi(value); err == nil && size > 0 {
			return size
		}
	}
	return 1 // Default to single item (current behavior)
}

func GetTargetSizeBytes() int64 {
	if value := os.Getenv("LAKERUNNER_TARGET_SIZE_BYTES"); value != "" {
		if size, err := strconv.ParseInt(value, 10, 64); err == nil && size > 0 {
			return size
		}
	}
	return DefaultTargetSizeBytes
}

func GetMaxBatchSize() int {
	if value := os.Getenv("LAKERUNNER_MAX_BATCH_SIZE"); value != "" {
		if size, err := strconv.Atoi(value); err == nil && size > 0 {
			return size
		}
	}
	return DefaultMaxBatchSize
}
