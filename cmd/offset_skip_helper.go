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
	"log/slog"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// StartOffsetSkipChecker creates and starts an offset skip checker for a consumer.
// It returns a stop function that should be called when the consumer shuts down.
// The checker runs in a background goroutine and periodically checks for skip entries.
func StartOffsetSkipChecker(
	ctx context.Context,
	cfg *config.Config,
	store *lrdb.Store,
	consumerGroup, topic string,
) (stopFunc func(), err error) {
	adminClient, err := fly.NewAdminClient(&cfg.Kafka)
	if err != nil {
		slog.Warn("Failed to create admin client for offset skip checker, skipping",
			slog.String("consumerGroup", consumerGroup),
			slog.String("topic", topic),
			slog.Any("error", err))
		// Return a no-op stop function - the consumer can still run without skip checking
		return func() {}, nil
	}

	checker := fly.NewOffsetSkipChecker(store, adminClient, consumerGroup, topic, 30*time.Second)

	// Start the checker in a background goroutine
	go func() {
		slog.Info("Starting offset skip checker",
			slog.String("consumerGroup", consumerGroup),
			slog.String("topic", topic))
		checker.Start(ctx)
	}()

	return checker.Stop, nil
}
