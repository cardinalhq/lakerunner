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

package metricsprocessing

import (
	"context"
	"fmt"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// TraceCompactionConsumerV2 handles trace compaction using the common consumer framework
type TraceCompactionConsumerV2 struct {
	*CommonConsumer[*messages.TraceCompactionMessage, messages.TraceCompactionKey]
}

// NewTraceCompactionConsumerV2 creates a new trace compaction consumer using the common consumer framework
func NewTraceCompactionConsumerV2(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store TraceCompactionStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*TraceCompactionConsumerV2, error) {

	// Create processor
	processor := NewTraceCompactionProcessor(store, storageProvider, cmgr, cfg)

	// Configure the consumer
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  "lakerunner-trace-compaction-v2",
		Topic:         "lakerunner.segments.traces.compact",
		ConsumerGroup: "lakerunner.compact.traces",
		FlushInterval: 1 * time.Minute,
		StaleAge:      1 * time.Minute,
		MaxAge:        5 * time.Minute,
	}

	// Create common consumer
	commonConsumer, err := NewCommonConsumer[*messages.TraceCompactionMessage, messages.TraceCompactionKey](
		ctx,
		factory,
		cfg,
		consumerConfig,
		store,
		processor,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create common consumer: %w", err)
	}

	return &TraceCompactionConsumerV2{
		CommonConsumer: commonConsumer,
	}, nil
}
