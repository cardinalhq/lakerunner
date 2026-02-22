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

package queryworker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cardinalhq/lakerunner/queryapi"
)

const DefaultMaxEstimatedGroupCardinality int64 = 100

var ErrCardinalityLimitExceeded = errors.New("cardinality limit exceeded")

type CardinalityLimitError struct {
	Estimated int64
	Limit     int64
	GroupBy   []string
}

func (e *CardinalityLimitError) Error() string {
	if len(e.GroupBy) == 0 {
		return fmt.Sprintf("estimated cardinality %d exceeds limit %d", e.Estimated, e.Limit)
	}
	return fmt.Sprintf(
		"estimated cardinality %d exceeds limit %d for group by [%s]",
		e.Estimated,
		e.Limit,
		strings.Join(e.GroupBy, ", "),
	)
}

func (e *CardinalityLimitError) Unwrap() error {
	return ErrCardinalityLimitExceeded
}

func shouldGuardGroupCardinality(req queryapi.PushDownRequest) bool {
	if req.BaseExpr == nil {
		return false
	}
	if req.BaseExpr.LogLeaf != nil {
		return false
	}
	if req.TagName != "" || req.TagNames || req.IsSummary {
		return false
	}
	return len(req.BaseExpr.GroupBy) > 0
}

func estimateGroupCardinalityForLocalPaths(
	ctx context.Context,
	w *CacheManager,
	req queryapi.PushDownRequest,
	localPaths []string,
) (int64, error) {
	if !shouldGuardGroupCardinality(req) || len(localPaths) == 0 {
		return 0, nil
	}

	src := buildReadParquetSource(localPaths)
	sql, ok := buildCardinalityEstimateSQL(req, src)
	if !ok {
		return 0, nil
	}

	conn, release, err := w.pool.GetConnection(ctx)
	if err != nil {
		return 0, fmt.Errorf("acquire connection for cardinality estimate: %w", err)
	}
	defer release()

	var estimated int64
	if err := conn.QueryRowContext(ctx, sql).Scan(&estimated); err != nil {
		return 0, fmt.Errorf("run cardinality estimate: %w", err)
	}
	if estimated < 0 {
		estimated = 0
	}
	return estimated, nil
}

func buildCardinalityEstimateSQL(req queryapi.PushDownRequest, src string) (string, bool) {
	if req.BaseExpr == nil || req.BaseExpr.LogLeaf != nil || len(req.BaseExpr.GroupBy) == 0 {
		return "", false
	}

	sql := req.BaseExpr.ToWorkerCardinalityEstimateSQL()
	if sql == "" {
		return "", false
	}

	sql = strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", req.StartTs))
	sql = strings.ReplaceAll(sql, "{end}", fmt.Sprintf("%d", req.EndTs))
	sql = strings.Replace(sql, "{table}", src, 1)
	return sql, true
}
