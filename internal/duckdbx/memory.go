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

package duckdbx

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
)

type returnedMemoryStats struct {
	DatabaseName string
	DatabaseSize string
	BlockSize    int64
	TotalBlocks  int64
	UsedBlocks   int64
	FreeBlocks   int64
	WALSize      string
	MemoryUsage  string
	MemoryLimit  string
}

type MemoryStats struct {
	DatabaseName string
	DatabaseSize int64
	BlockSize    int64
	TotalBlocks  int64
	UsedBlocks   int64
	FreeBlocks   int64
	WALSize      int64
	MemoryUsage  int64
	MemoryLimit  int64
}

func GetDuckDBMemoryStats(conn *sql.Conn) ([]MemoryStats, error) {
	rows, err := conn.QueryContext(context.Background(), "PRAGMA database_size")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var stats []returnedMemoryStats
	for rows.Next() {
		var stat returnedMemoryStats
		if err := rows.Scan(&stat.DatabaseName, &stat.DatabaseSize, &stat.BlockSize, &stat.TotalBlocks, &stat.UsedBlocks, &stat.FreeBlocks, &stat.WALSize, &stat.MemoryUsage, &stat.MemoryLimit); err != nil {
			return nil, err
		}
		stats = append(stats, stat)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	ret := make([]MemoryStats, len(stats))
	for _, stat := range stats {
		ret = append(ret, MemoryStats{
			DatabaseName: stat.DatabaseName,
			DatabaseSize: parseSize(stat.DatabaseSize),
			BlockSize:    stat.BlockSize,
			TotalBlocks:  stat.TotalBlocks,
			UsedBlocks:   stat.UsedBlocks,
			FreeBlocks:   stat.FreeBlocks,
			WALSize:      parseSize(stat.WALSize),
			MemoryUsage:  parseSize(stat.MemoryUsage),
			MemoryLimit:  parseSize(stat.MemoryLimit),
		})
	}

	return ret, nil
}

// Parse strings like "0 bytes", "1.2 MB, "3.1 GiB" into int64 bytes.
func parseSize(sizeStr string) int64 {
	parts := strings.Split(sizeStr, " ")
	if len(parts) == 0 {
		return 0
	}
	if len(parts) == 1 {
		v, err := strconv.ParseFloat(parts[0], 64)
		if err != nil {
			return 0
		}
		return int64(v)
	}

	value, err := strconv.ParseFloat(parts[0], 64)
	if err != nil {
		return 0
	}

	unit := strings.ToLower(parts[1])
	switch unit {
	case "bytes", "byte":
		return int64(value)
	case "KiB":
		return int64(value * 1024)
	case "MiB":
		return int64(value * 1024 * 1024)
	case "GiB":
		return int64(value * 1024 * 1024 * 1024)
	case "TiB":
		return int64(value * 1024 * 1024 * 1024 * 1024)
	case "PiB":
		return int64(value * 1024 * 1024 * 1024 * 1024 * 1024)
	default:
		return 0
	}
}
