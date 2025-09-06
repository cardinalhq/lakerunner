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
	"os"

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/option"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type GCPPubSubService struct {
	tracer       trace.Tracer
	sp           storageprofile.StorageProfileProvider
	client       *pubsub.Client
	sub          *pubsub.Subscription
	kafkaHandler *KafkaHandler
}

// Ensure GCPPubSubService implements Backend interface
var _ Backend = (*GCPPubSubService)(nil)

func NewGCPPubSubService(kafkaFactory *fly.Factory) (*GCPPubSubService, error) {
	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		return nil, fmt.Errorf("GCP_PROJECT_ID environment variable is required")
	}

	subscriptionID := os.Getenv("GCP_SUBSCRIPTION_ID")
	if subscriptionID == "" {
		return nil, fmt.Errorf("GCP_SUBSCRIPTION_ID environment variable is required")
	}

	// Only set credentials if explicitly provided (ADC will handle GCE/Cloud Run)
	var opts []option.ClientOption
	if keyFile := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); keyFile != "" {
		opts = append(opts, option.WithCredentialsFile(keyFile))
	}
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create pubsub client: %w", err)
	}

	sub := client.Subscription(subscriptionID)

	cdb, err := dbopen.ConfigDBStore(context.Background())
	if err != nil {
		slog.Error("Failed to connect to configdb", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	// Kafka is required
	if !kafkaFactory.IsEnabled() {
		return nil, fmt.Errorf("Kafka is required for pubsub services but is not enabled")
	}

	kafkaHandler, err := NewKafkaHandler(kafkaFactory, "gcp", sp)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka handler: %w", err)
	}

	service := &GCPPubSubService{
		tracer:       otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub/gcp-pubsub"),
		sp:           sp,
		client:       client,
		sub:          sub,
		kafkaHandler: kafkaHandler,
	}

	slog.Info("GCP Pub/Sub service initialized with Kafka support")
	return service, nil
}

func (ps *GCPPubSubService) GetName() string {
	return "gcp"
}

func (ps *GCPPubSubService) Run(doneCtx context.Context) error {
	slog.Info("Starting GCP Pub/Sub service for Cloud Storage events")

	// Start the receive loop
	err := ps.sub.Receive(doneCtx, ps.messageHandler)

	// Ensure client and Kafka handler are closed on exit
	defer func() {
		if err := ps.client.Close(); err != nil {
			slog.Error("Failed to close GCP Pub/Sub client", slog.Any("error", err))
		}
		if ps.kafkaHandler != nil {
			if err := ps.kafkaHandler.Close(); err != nil {
				slog.Error("Failed to close Kafka handler", slog.Any("error", err))
			}
		}
	}()

	if err != nil && err != context.Canceled {
		return fmt.Errorf("GCP Pub/Sub receive error: %w", err)
	}

	return nil
}

// messageHandler processes individual messages with proper tracing and error handling
func (ps *GCPPubSubService) messageHandler(ctx context.Context, msg *pubsub.Message) {
	ctx, span := ps.tracer.Start(ctx, "gcp_pubsub.message_handler",
		trace.WithAttributes(
			attribute.String("message_id", msg.ID),
			attribute.String("publish_time", msg.PublishTime.String()),
		))
	defer span.End()

	// Process the message with Kafka
	err := ps.kafkaHandler.HandleMessage(ctx, msg.Data)

	if err != nil {
		// Record error in span
		span.RecordError(err)
		span.SetAttributes(attribute.String("error", err.Error()))

		// Log the error
		slog.Error("Failed to handle Cloud Storage event",
			slog.Any("error", err),
			slog.String("message_id", msg.ID))

		// Nack the message to trigger redelivery (or DLQ after max attempts)
		msg.Nack()
		return
	}

	msg.Ack()

	span.SetAttributes(attribute.String("status", "success"))
}
