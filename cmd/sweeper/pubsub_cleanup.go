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

package sweeper

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/pubsub"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	pubsubHistoryCleanedCounter metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/cmd/sweeper")

	var err error
	pubsubHistoryCleanedCounter, err = meter.Int64Counter(
		"lakerunner.sweeper.pubsub_history_records_cleaned_total",
		metric.WithDescription("Total number of pubsub history records cleaned up"),
	)
	if err != nil {
		panic(err)
	}
}

// PubSubHistoryQuerier defines the interface needed for pubsub history cleanup
type PubSubHistoryQuerier interface {
	PubSubMessageHistoryCleanup(ctx context.Context, params lrdb.PubSubMessageHistoryCleanupParams) (pgconn.CommandTag, error)
}

func runPubSubHistoryCleanup(ctx context.Context, querier PubSubHistoryQuerier, cfg *config.Config) error {
	retention := pubsub.GetCleanupRetention(cfg)
	batchSize := int32(pubsub.GetCleanupBatchSize(cfg))

	if retention == 0 {
		slog.Debug("PubSub history cleanup disabled (retention = 0)")
		return nil
	}

	ageThreshold := time.Now().Add(-retention)

	slog.Info("Starting PubSub history cleanup", slog.Duration("retention", retention), slog.Int("batch_size", int(batchSize)), slog.Time("age_threshold", ageThreshold))

	totalCleaned := int64(0)

	for {
		select {
		case <-ctx.Done():
			slog.Info("PubSub history cleanup cancelled", slog.Int64("total_cleaned", totalCleaned))
			return ctx.Err()
		default:
		}

		cmdTag, err := querier.PubSubMessageHistoryCleanup(ctx, lrdb.PubSubMessageHistoryCleanupParams{
			AgeThreshold: ageThreshold,
			BatchSize:    batchSize,
		})

		if err != nil {
			slog.Error("Failed to cleanup PubSub history batch", slog.Any("error", err), slog.Int64("total_cleaned_so_far", totalCleaned))
			return err
		}

		batchCleaned := cmdTag.RowsAffected()
		totalCleaned += batchCleaned

		pubsubHistoryCleanedCounter.Add(ctx, batchCleaned)

		slog.Debug("Cleaned PubSub history batch", slog.Int64("batch_cleaned", batchCleaned), slog.Int64("total_cleaned", totalCleaned))

		if batchCleaned < int64(batchSize) {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if totalCleaned > 0 {
		slog.Info("PubSub history cleanup completed", slog.Int64("total_cleaned", totalCleaned), slog.Duration("retention", retention))
	} else {
		slog.Debug("PubSub history cleanup completed - no records to clean")
	}

	return nil
}
