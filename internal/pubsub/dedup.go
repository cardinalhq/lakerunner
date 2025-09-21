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

package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// DedupInserter defines the interface needed for deduplication
type DedupInserter interface {
	PubSubMessageHistoryInsert(ctx context.Context, params lrdb.PubSubMessageHistoryInsertParams) (pgconn.CommandTag, error)
}

// Deduplicator defines the interface for message deduplication
type Deduplicator interface {
	CheckAndRecord(ctx context.Context, bucket, objectID, source string) (bool, error)
}

// DatabaseDeduplicator handles message deduplication using database
type DatabaseDeduplicator struct {
	store DedupInserter
}

// NewDatabaseDeduplicator creates a new database deduplicator instance
func NewDatabaseDeduplicator(store DedupInserter) *DatabaseDeduplicator {
	return &DatabaseDeduplicator{
		store: store,
	}
}

// NewDeduplicator creates a new deduplicator instance (returns interface)
func NewDeduplicator(store DedupInserter) Deduplicator {
	return NewDatabaseDeduplicator(store)
}

// CheckAndRecord checks if a message is a duplicate and records it if not
// Returns true if the message should be processed (not a duplicate)
func (d *DatabaseDeduplicator) CheckAndRecord(ctx context.Context, bucket, objectID, source string) (bool, error) {
	// Try to insert into database - only succeeds if not a duplicate
	result, err := d.store.PubSubMessageHistoryInsert(ctx, lrdb.PubSubMessageHistoryInsertParams{
		Bucket:   bucket,
		ObjectID: objectID,
		Source:   source,
	})

	if err != nil {
		// Database error - fail closed (don't process message)
		slog.Error("Failed to check message deduplication",
			slog.Any("error", err),
			slog.String("bucket", bucket),
			slog.String("object_id", objectID))
		return false, fmt.Errorf("deduplication check failed: %w", err)
	}

	wasInserted := result.RowsAffected() > 0
	if wasInserted {
		slog.Debug("New message recorded in dedup table",
			slog.String("bucket", bucket),
			slog.String("object_id", objectID))
		return true, nil
	}

	slog.Info("Duplicate message detected, skipping",
		slog.String("bucket", bucket),
		slog.String("object_id", objectID),
		slog.String("source", source))
	recordDuplicate(ctx, bucket, source)
	return false, nil
}

// GetCleanupRetention returns the retention period for cleanup from config
func GetCleanupRetention(cfg *config.Config) time.Duration {
	return cfg.PubSub.Dedup.RetentionDuration
}

// GetCleanupBatchSize returns the batch size for cleanup operations from config
func GetCleanupBatchSize(cfg *config.Config) int {
	return cfg.PubSub.Dedup.CleanupBatchSize
}

// recordDuplicate increments the duplicate counter metric
func recordDuplicate(ctx context.Context, bucket, source string) {
	itemsDuplicated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("bucket", bucket),
		attribute.String("source", source),
	))
}
