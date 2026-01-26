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

package lrdb

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/google/uuid"
)

type PartitionInfo struct {
	PartitionName string
	Bounds        string
	Level         int
}

type OrgDateintInfo struct {
	OrganizationID uuid.UUID
	Dateint        int32
}

const partitionDiscoveryQuery = `
SELECT 
  c.relname AS partition_name, 
  pg_get_expr(c.relpartbound, c.oid, true) AS partition_bounds,
  pt.level
FROM pg_partition_tree($1::regclass) pt
JOIN pg_class c ON pt.relid = c.oid
WHERE pt.level > 0
ORDER BY pt.level, partition_name
`

// GetPartitions retrieves all partitions for a given table
func (q *Queries) GetPartitions(ctx context.Context, tableName string) ([]PartitionInfo, error) {
	rows, err := q.db.Query(ctx, partitionDiscoveryQuery, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []PartitionInfo
	for rows.Next() {
		var p PartitionInfo
		if err := rows.Scan(&p.PartitionName, &p.Bounds, &p.Level); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return partitions, nil
}

var (
	// Regex to extract UUID from partition bounds like "FOR VALUES IN ('uuid-string')"
	orgBoundsRegex = regexp.MustCompile(`FOR VALUES IN \('([^']+)'\)`)

	// Regex to extract dateint range from partition bounds like "FOR VALUES FROM (20250828) TO (20250829)"
	dateintBoundsRegex = regexp.MustCompile(`FOR VALUES FROM \((\d+)\) TO \(\d+\)`)
)

// ParseMetricPartitions extracts organization IDs and dateints from metric_seg partitions
func (q *Queries) ParseMetricPartitions(ctx context.Context) ([]OrgDateintInfo, error) {
	return q.parseSegmentPartitions(ctx, "metric_seg")
}

// ParseLogPartitions extracts organization IDs and dateints from log_seg partitions
func (q *Queries) ParseLogPartitions(ctx context.Context) ([]OrgDateintInfo, error) {
	return q.parseSegmentPartitions(ctx, "log_seg")
}

// ParseTracePartitions extracts organization IDs and dateints from trace_seg partitions
func (q *Queries) ParseTracePartitions(ctx context.Context) ([]OrgDateintInfo, error) {
	return q.parseSegmentPartitions(ctx, "trace_seg")
}

func (q *Queries) parseSegmentPartitions(ctx context.Context, tableName string) ([]OrgDateintInfo, error) {
	partitions, err := q.GetPartitions(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions for %s: %w", tableName, err)
	}

	var result []OrgDateintInfo
	orgUUIDs := make(map[string]uuid.UUID) // Cache for org partition name -> UUID mapping

	// Process partitions by level (level 1 = orgs, level 2 = dateints)
	for _, p := range partitions {
		switch p.Level {
		case 1:
			// Level 1: Organization partitions (contains UUID in bounds)
			if matches := orgBoundsRegex.FindStringSubmatch(p.Bounds); len(matches) > 1 {
				uuidStr := matches[1]
				if orgUUID, err := uuid.Parse(uuidStr); err == nil {
					orgUUIDs[p.PartitionName] = orgUUID
				}
			}
		case 2:
			// Level 2: Dateint partitions (contains dateint range in bounds)
			if matches := dateintBoundsRegex.FindStringSubmatch(p.Bounds); len(matches) > 1 {
				dateintStr := matches[1]
				if dateint, err := strconv.ParseInt(dateintStr, 10, 32); err == nil {
					// Extract parent org partition name from dateint partition name
					orgPartName := extractOrgPartitionName(p.PartitionName)
					if orgUUID, exists := orgUUIDs[orgPartName]; exists {
						result = append(result, OrgDateintInfo{
							OrganizationID: orgUUID,
							Dateint:        int32(dateint),
						})
					}
				}
			}
		}
	}

	return result, nil
}

// extractOrgPartitionName extracts the organization partition name from a dateint partition name
// e.g., "mseg_cn584at5wzobxlcnxk1wwn55b_20250828_1" -> "mseg_cn584at5wzobxlcnxk1wwn55b"
// e.g., "log_seg_cn584at5wzobxlcnxk1wwn55b_20250828_1" -> "log_seg_cn584at5wzobxlcnxk1wwn55b"
func extractOrgPartitionName(dateintPartName string) string {
	// Find the last occurrence of a dateint pattern (8 digits followed by underscore and number)
	// This handles cases where the org part itself might contain numbers
	dateintPattern := regexp.MustCompile(`_\d{8}_\d+$`)
	return dateintPattern.ReplaceAllString(dateintPartName, "")
}
