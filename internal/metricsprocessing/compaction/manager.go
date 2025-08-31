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

package compaction

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type compactionStore interface {
	s3helper.ObjectCleanupStore
	ClaimMetricCompactionWork(ctx context.Context, params lrdb.ClaimMetricCompactionWorkParams) ([]lrdb.ClaimMetricCompactionWorkRow, error)
	DeleteMetricCompactionWork(ctx context.Context, params lrdb.DeleteMetricCompactionWorkParams) error
	ReleaseMetricCompactionWork(ctx context.Context, params lrdb.ReleaseMetricCompactionWorkParams) error
	TouchMetricCompactionWork(ctx context.Context, params lrdb.TouchMetricCompactionWorkParams) error
	CompactMetricSegs(ctx context.Context, args lrdb.ReplaceMetricSegsParams) error
	GetMetricSegsForCompactionWork(ctx context.Context, params lrdb.GetMetricSegsForCompactionWorkParams) ([]lrdb.MetricSeg, error)
	MarkMetricSegsCompactedByKeys(ctx context.Context, arg lrdb.MarkMetricSegsCompactedByKeysParams) error
}

type config struct {
	MaxAgeSeconds        int32
	BatchCount           int32
	DefaultTargetRecords int64
}

func GetConfigFromEnv() config {
	maxAge := int32(900)
	if env := os.Getenv("METRIC_COMPACTION_MAX_AGE_SECONDS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			maxAge = int32(val)
		}
	}

	batchCount := int32(20)
	if env := os.Getenv("METRIC_COMPACTION_BATCH_COUNT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			batchCount = int32(val)
		}
	}

	return config{
		MaxAgeSeconds:        maxAge,
		BatchCount:           batchCount,
		DefaultTargetRecords: 40_000,
	}
}

type Manager struct {
	db         compactionStore
	workerID   int64
	config     config
	ll         *slog.Logger
	sp         storageprofile.StorageProfileProvider
	awsmanager *awsclient.Manager
}

func NewManager(
	db compactionStore,
	workerID int64,
	config config,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
) *Manager {
	return &Manager{
		db:         db,
		workerID:   workerID,
		config:     config,
		ll:         slog.Default().With(slog.String("component", "metric-compaction-manager")),
		sp:         sp,
		awsmanager: awsmanager,
	}
}

func (m *Manager) ClaimWork(ctx context.Context) ([]lrdb.ClaimMetricCompactionWorkRow, error) {
	claimedRows, err := m.db.ClaimMetricCompactionWork(ctx, lrdb.ClaimMetricCompactionWorkParams{
		WorkerID:             m.workerID,
		NowTs:                nil, // Use database now()
		DefaultTargetRecords: m.config.DefaultTargetRecords,
		MaxAgeSeconds:        m.config.MaxAgeSeconds,
		BatchCount:           m.config.BatchCount,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim metric compaction work: %w", err)
	}

	if len(claimedRows) > 0 {
		m.ll.Info("Claimed metric compaction work batch",
			slog.Int("workItems", len(claimedRows)))
	}

	return claimedRows, nil
}

func (m *Manager) CompleteWork(ctx context.Context, rows []lrdb.ClaimMetricCompactionWorkRow) error {
	for _, row := range rows {
		if err := m.db.DeleteMetricCompactionWork(ctx, lrdb.DeleteMetricCompactionWorkParams{
			ID:        row.ID,
			ClaimedBy: m.workerID,
		}); err != nil {
			m.ll.Error("Failed to complete work item",
				slog.Int64("id", row.ID),
				slog.Any("error", err))
			return fmt.Errorf("failed to complete work item %d: %w", row.ID, err)
		}
	}
	return nil
}

func (m *Manager) FailWork(ctx context.Context, rows []lrdb.ClaimMetricCompactionWorkRow) error {
	// Use deadline context to ensure DB operations succeed even if original context is cancelled
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, row := range rows {
		if err := m.db.ReleaseMetricCompactionWork(releaseCtx, lrdb.ReleaseMetricCompactionWorkParams{
			ID:        row.ID,
			ClaimedBy: m.workerID,
		}); err != nil {
			m.ll.Error("Failed to fail work item",
				slog.Int64("id", row.ID),
				slog.Any("error", err))
			return fmt.Errorf("failed to fail work item %d: %w", row.ID, err)
		}
	}
	return nil
}

// Run starts the compaction loop using the manager's dependencies
func (m *Manager) Run(ctx context.Context) error {
	return runLoop(ctx, m, m.db, m.sp, m.awsmanager)
}
