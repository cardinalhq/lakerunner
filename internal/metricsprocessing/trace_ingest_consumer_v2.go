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

// TraceIngestConsumerV2 handles object store notification messages for trace ingestion using CommonConsumer
type TraceIngestConsumerV2 struct {
	*CommonConsumer[*messages.ObjStoreNotificationMessage, messages.IngestKey]
}

// NewTraceIngestConsumerV2 creates a new trace ingest consumer using the common consumer framework
func NewTraceIngestConsumerV2(
	ctx context.Context,
	factory *fly.Factory,
	cfg *config.Config,
	store TraceIngestStore,
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
) (*TraceIngestConsumerV2, error) {

	// Create Kafka producer for segment notifications
	kafkaProducer, err := factory.CreateProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	// Create TraceIngestProcessor (reusing existing)
	processor := newTraceIngestProcessor(store, storageProvider, cmgr, kafkaProducer)

	// Configure the consumer
	consumerConfig := CommonConsumerConfig{
		ConsumerName:  "lakerunner-trace-ingest-v2",
		Topic:         "lakerunner.objstore.ingest.traces",
		ConsumerGroup: "lakerunner.ingest.traces",
		FlushInterval: 10 * time.Second,
		StaleAge:      20 * time.Second,
		MaxAge:        20 * time.Second,
	}

	// Create common consumer
	commonConsumer, err := NewCommonConsumer[*messages.ObjStoreNotificationMessage, messages.IngestKey](
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

	return &TraceIngestConsumerV2{
		CommonConsumer: commonConsumer,
	}, nil
}