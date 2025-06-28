// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

func diskUsageLoop(ctx context.Context) {
	diskUsage()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.Tick(5 * time.Minute):
			diskUsage()
		}
	}
}
func diskUsage() {
	diskstats, err := helpers.DiskUsage(os.TempDir())
	if err != nil {
		// Log the error but continue the loop
		slog.Error("Failed to get disk usage stats", "error", err)
		return
	}
	slog.Info("Disk usage stats",
		"totalBytes", diskstats.TotalBytes,
		"freeBytes", diskstats.FreeBytes,
		"usedBytes", diskstats.UsedBytes,
		"freePercent", float64(diskstats.FreeBytes)/float64(diskstats.TotalBytes)*100,
		"totalInodes", diskstats.TotalInodes,
		"freeInodes", diskstats.FreeInodes,
		"usedInodes", diskstats.UsedInodes,
		"freeInodesPercent", float64(diskstats.FreeInodes)/float64(diskstats.TotalInodes)*100,
	)
}
