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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/helpers"
)

var (
	spaceMeter = otel.Meter("github.com/cardinalhq/lakerunner/scratchspace")

	totalBytes metric.Int64Gauge
	freeBytes  metric.Int64Gauge
	usedBytes  metric.Int64Gauge

	failcount    = 0
	hasSucceeded = false
)

func diskUsageLoop(ctx context.Context) {
	m, err := spaceMeter.Int64Gauge("scratchspace.total_bytes",
		metric.WithDescription("Total bytes total in the scratch space"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("Failed to create available bytes gauge", slog.Any("error", err))
		return
	}
	totalBytes = m

	m, err = spaceMeter.Int64Gauge("scratchspace.free_bytes",
		metric.WithDescription("Free bytes in the scratch space"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("Failed to create free bytes gauge", slog.Any("error", err))
		return
	}
	freeBytes = m

	m, err = spaceMeter.Int64Gauge("scratchspace.used_bytes",
		metric.WithDescription("Used bytes in the scratch space"),
		metric.WithUnit("By"),
	)
	if err != nil {
		slog.Error("Failed to create used bytes gauge", slog.Any("error", err))
		return
	}
	usedBytes = m

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
	if !hasSucceeded && failcount > 10 {
		return
	}

	diskstats, err := helpers.DiskUsage(os.TempDir())
	if err != nil {
		failcount++
		if failcount > 10 && !hasSucceeded {
			slog.Error("Failed to get disk usage stats multiple times, stopping further attempts", slog.Int("failcount", failcount))
			return
		}
		slog.Error("Failed to get disk usage stats", "error", err, "failcount", failcount)
		return
	}
	hasSucceeded = true

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

	totalBytes.Record(context.Background(), int64(diskstats.TotalBytes))
	freeBytes.Record(context.Background(), int64(diskstats.FreeBytes))
	usedBytes.Record(context.Background(), int64(diskstats.UsedBytes))
}
