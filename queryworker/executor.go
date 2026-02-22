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
	"encoding/json"
	"fmt"
	"os"

	"github.com/cardinalhq/lakerunner/queryapi"
	"github.com/cardinalhq/lakerunner/queryworker/artifactspool"
	"github.com/cardinalhq/lakerunner/queryworker/workmanager"
)

// WorkExecutor implements workmanager.Executor by planning queries, writing
// Parquet artifacts to the spool, and returning metadata for the control stream.
type WorkExecutor struct {
	ws       *WorkerService
	spool    *artifactspool.Spool
	httpAddr string // advertised address for artifact fetch, e.g. "10.0.0.1:8081"
}

// NewWorkExecutor creates a WorkExecutor.
func NewWorkExecutor(ws *WorkerService, spool *artifactspool.Spool, httpAddr string) *WorkExecutor {
	return &WorkExecutor{
		ws:       ws,
		spool:    spool,
		httpAddr: httpAddr,
	}
}

// Execute implements workmanager.Executor.
func (e *WorkExecutor) Execute(ctx context.Context, workID string, spec []byte) (*workmanager.ArtifactResult, error) {
	var req queryapi.PushDownRequest
	if err := json.Unmarshal(spec, &req); err != nil {
		return nil, fmt.Errorf("unmarshal work spec: %w", err)
	}

	plan, err := e.ws.PlanQuery(req)
	if err != nil {
		return nil, fmt.Errorf("plan query: %w", err)
	}

	outputPath := e.spool.PathForWork(workID)

	fileResult, err := EvaluatePushDownToFile(ctx, plan.CacheManager, req, plan.SQL, outputPath, plan.TimestampCol)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}

	// EvaluatePushDownToFile returns early without writing a file when there
	// are no matching segments. Create an empty file so the spool can track it.
	if fileResult.RowCount == 0 {
		if _, statErr := os.Stat(outputPath); os.IsNotExist(statErr) {
			if err := os.WriteFile(outputPath, nil, 0o644); err != nil {
				return nil, fmt.Errorf("create empty artifact: %w", err)
			}
		}
	}

	checksum, size, err := e.spool.Register(workID, outputPath)
	if err != nil {
		return nil, fmt.Errorf("register artifact: %w", err)
	}

	return &workmanager.ArtifactResult{
		ArtifactURL:       artifactspool.ArtifactURL(e.httpAddr, workID),
		ArtifactSizeBytes: size,
		ArtifactChecksum:  checksum,
		RowCount:          fileResult.RowCount,
		MinTs:             fileResult.MinTs,
		MaxTs:             fileResult.MaxTs,
	}, nil
}
