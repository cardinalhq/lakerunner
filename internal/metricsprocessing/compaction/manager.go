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

	"github.com/jackc/pgx/v5"

	"github.com/cardinalhq/lakerunner/lrdb"
)

type CompactionStore interface {
	ClaimMetricCompactionWork(ctx context.Context, params lrdb.ClaimMetricCompactionWorkParams) ([]lrdb.ClaimMetricCompactionWorkRow, error)
	DeleteMetricCompactionWork(ctx context.Context, params lrdb.DeleteMetricCompactionWorkParams) error
	ReleaseMetricCompactionWork(ctx context.Context, params lrdb.ReleaseMetricCompactionWorkParams) error
	ReplaceCompactedMetricSegs(ctx context.Context, params lrdb.ReplaceCompactedMetricSegsParams) error
	GetMetricSegsForCompactionWork(ctx context.Context, params lrdb.GetMetricSegsForCompactionWorkParams) ([]lrdb.MetricSeg, error)
}

type Config struct {
	MaxAgeSeconds        int32
	BatchCount           int32
	DefaultTargetRecords int64
}

func GetConfigFromEnv() Config {
	maxAge := int32(900) // 15 minutes default
	if env := os.Getenv("METRIC_COMPACTION_MAX_AGE_SECONDS"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			maxAge = int32(val)
		}
	}

	batchCount := int32(100)
	if env := os.Getenv("METRIC_COMPACTION_BATCH_COUNT"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 {
			batchCount = int32(val)
		}
	}

	return Config{
		MaxAgeSeconds:        maxAge,
		BatchCount:           batchCount,
		DefaultTargetRecords: 40_000,
	}
}

type Manager struct {
	db       CompactionStore
	workerID int64
	config   Config
	ll       *slog.Logger
}

func NewManager(db CompactionStore, workerID int64, config Config) *Manager {
	return &Manager{
		db:       db,
		workerID: workerID,
		config:   config,
		ll:       slog.Default().With(slog.String("component", "metric-compaction-manager")),
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
	for _, row := range rows {
		if err := m.db.ReleaseMetricCompactionWork(ctx, lrdb.ReleaseMetricCompactionWorkParams{
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
