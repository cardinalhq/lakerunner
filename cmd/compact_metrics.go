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

package cmd

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metriccompaction"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.CleanTempDir()

			servicename := "lakerunner-compact-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "compact"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			metriccompaction.NewTIDMerger = func(tmpdir string, files []string, frequencyMs int32, rpfEstimate int64, startTs, endTs int64) (metriccompaction.TIDMerger, error) {
				merger, err := NewTIDMerger(tmpdir, files, frequencyMs, rpfEstimate, startTs, endTs)
				if err != nil {
					return nil, err
				}
				return &tidMergerAdapter{merger: merger}, nil
			}
			metriccompaction.GetRangeBoundsTimestamptz = RangeBounds[pgtype.Timestamptz]
			metriccompaction.GetRangeBoundsInt8 = RangeBounds[pgtype.Int8]
			metriccompaction.IsWantedFrequency = isWantedFrequency
			metriccompaction.AllRolledUp = allRolledUp

			loop, err := NewRunqueueLoopContext(doneCtx, "metrics", "compact", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, compactRollupItem)
		},
	}

	rootCmd.AddCommand(cmd)
}

type tidMergerAdapter struct {
	merger *TIDMerger
}

func (t *tidMergerAdapter) Merge() ([]metriccompaction.MergeResult, metriccompaction.MergeStats, error) {
	results, stats, err := t.merger.Merge()
	if err != nil {
		return nil, metriccompaction.MergeStats{DatapointsOutOfRange: stats.DatapointsOutOfRange}, err
	}

	converted := make([]metriccompaction.MergeResult, len(results))
	for i, result := range results {
		converted[i] = metriccompaction.MergeResult{
			FileName:    result.FileName,
			RecordCount: result.RecordCount,
			FileSize:    result.FileSize,
			TidCount:    int64(result.TidCount),
		}
	}

	return converted, metriccompaction.MergeStats{DatapointsOutOfRange: stats.DatapointsOutOfRange}, nil
}

func compactRollupItem(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
) (WorkResult, error) {
	result, err := metriccompaction.ProcessItem(ctx, ll, tmpdir, awsmanager, sp, mdb, inf, rpfEstimate)
	if err != nil {
		if result == metriccompaction.WorkResultTryAgainLater {
			return WorkResultTryAgainLater, err
		}
		return WorkResultTryAgainLater, err
	}
	return WorkResultSuccess, nil
}
