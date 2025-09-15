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

package duckdbx

import (
	"context"
	"time"

	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	logger = slog.Default()
	meter  = otel.Meter("github.com/cardinalhq/lakerunner/duckdbx")
)

func (d *DB) pollMemoryMetrics(ctx context.Context) {
	dbSizeGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.database_size",
		metric.WithDescription("DuckDB database size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		logger.Error("failed to create database_size metric", "error", err)
		return
	}

	blockSizeGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.block_size",
		metric.WithDescription("DuckDB block size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		logger.Error("failed to create block_size metric", "error", err)
		return
	}

	totalBlocksGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.total_blocks",
		metric.WithDescription("DuckDB total blocks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		logger.Error("failed to create total_blocks metric", "error", err)
		return
	}

	usedBlocksGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.used_blocks",
		metric.WithDescription("DuckDB used blocks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		logger.Error("failed to create used_blocks metric", "error", err)
		return
	}

	freeBlocksGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.free_blocks",
		metric.WithDescription("DuckDB free blocks"),
		metric.WithUnit("1"),
	)
	if err != nil {
		logger.Error("failed to create free_blocks metric", "error", err)
		return
	}

	walSizeGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.wal_size",
		metric.WithDescription("DuckDB WAL size"),
		metric.WithUnit("By"),
	)
	if err != nil {
		logger.Error("failed to create wal_size metric", "error", err)
		return
	}

	memoryUsageGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.memory_usage",
		metric.WithDescription("DuckDB memory usage"),
		metric.WithUnit("By"),
	)
	if err != nil {
		logger.Error("failed to create memory_usage metric", "error", err)
		return
	}

	memoryLimitGauge, err := meter.Int64Gauge("lakerunner.duckdb.memory.memory_limit",
		metric.WithDescription("DuckDB memory limit"),
		metric.WithUnit("By"),
	)
	if err != nil {
		logger.Error("failed to create memory_limit metric", "error", err)
		return
	}

	for {
		conn, err := d.db.Conn(ctx)
		if err != nil {
			if err.Error() == "sql: database is closed" {
				logger.Info("database is closed, stopping memory metrics polling")
				return
			}
			logger.Error("failed to get connection for memory metrics", "error", err)
			return
		}

		stats, err := GetDuckDBMemoryStats(conn)
		_ = conn.Close()
		if err != nil {
			logger.Error("failed to get memory stats", "error", err)
			return
		}

		for _, stat := range stats {
			attributes := []attribute.KeyValue{
				attribute.String("database_name", stat.DatabaseName),
				attribute.String("database_type", "duckdb"),
			}
			if d.config.InstanceName != "" {
				attributes = append(attributes, attribute.String("instance_name", d.config.InstanceName))
			}
			attr := metric.WithAttributeSet(attribute.NewSet(attributes...))
			dbSizeGauge.Record(ctx, stat.DatabaseSize, attr)
			blockSizeGauge.Record(ctx, stat.BlockSize, attr)
			totalBlocksGauge.Record(ctx, stat.TotalBlocks, attr)
			usedBlocksGauge.Record(ctx, stat.UsedBlocks, attr)
			freeBlocksGauge.Record(ctx, stat.FreeBlocks, attr)
			walSizeGauge.Record(ctx, stat.WALSize, attr)
			memoryUsageGauge.Record(ctx, stat.MemoryUsage, attr)
			memoryLimitGauge.Record(ctx, stat.MemoryLimit, attr)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(d.config.MetricsPeriod):
		}
	}
}
