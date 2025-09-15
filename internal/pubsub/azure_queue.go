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
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/cardinalhq/lakerunner/config"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/azureclient"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type AzureQueueService struct {
	tracer       trace.Tracer
	azureMgr     *azureclient.Manager
	sp           storageprofile.StorageProfileProvider
	kafkaHandler *KafkaHandler
}

var _ Backend = (*AzureQueueService)(nil)

func NewAzureQueueService(ctx context.Context, cfg *config.Config, kafkaFactory *fly.Factory) (*AzureQueueService, error) {
	azureMgr, err := azureclient.NewManager(context.Background(),
		azureclient.WithAssumeRoleSessionName("pubsub-azure"),
	)
	if err != nil {
		slog.Error("Failed to create Azure manager", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create Azure manager: %w", err)
	}

	cdb, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to configdb", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	kafkaHandler, err := NewKafkaHandler(ctx, cfg, kafkaFactory, "gcp", sp)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka handler: %w", err)
	}

	return &AzureQueueService{
		tracer:       otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub/azure"),
		azureMgr:     azureMgr,
		sp:           sp,
		kafkaHandler: kafkaHandler,
	}, nil
}

func (ps *AzureQueueService) Run(doneCtx context.Context) error {
	slog.Info("Starting Azure Queue Storage pubsub service")

	storageAccount := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if storageAccount == "" {
		return fmt.Errorf("AZURE_STORAGE_ACCOUNT environment variable is required")
	}

	queueName := os.Getenv("AZURE_QUEUE_NAME")
	if queueName == "" {
		return fmt.Errorf("AZURE_QUEUE_NAME environment variable is required")
	}

	queueClient, err := ps.azureMgr.GetQueue(context.Background(),
		azureclient.WithQueueStorageAccount(storageAccount),
		azureclient.WithQueueName(queueName),
	)
	if err != nil {
		slog.Error("Failed to create Azure Queue client", slog.Any("error", err))
		return fmt.Errorf("failed to create Azure Queue client: %w", err)
	}

	go ps.pollQueue(doneCtx, queueClient, queueName)

	<-doneCtx.Done()

	slog.Info("Shutting down Azure Queue Storage pubsub service")
	return nil
}

func (ps *AzureQueueService) pollQueue(doneCtx context.Context, queueClient *azureclient.QueueClient, queueName string) {
	slog.Info("Starting Azure Queue polling loop", slog.String("queue", queueName))

	for {
		select {
		case <-doneCtx.Done():
			slog.Info("Azure Queue polling loop stopped")
			return
		default:
		}

		ctx, cancel := context.WithTimeout(doneCtx, 30*time.Second)
		numberOfMessages := int32(32)
		visibilityTimeout := int32(30)
		result, err := queueClient.QueueClient.DequeueMessages(ctx, &azqueue.DequeueMessagesOptions{
			NumberOfMessages:  &numberOfMessages,
			VisibilityTimeout: &visibilityTimeout,
		})
		cancel()

		if err != nil {
			slog.Error("Failed to receive messages from Azure Queue", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}

		for _, message := range result.Messages {
			if message.MessageText != nil {
				msgBytes := decodeIfBase64(*message.MessageText)

				err := ps.kafkaHandler.HandleMessage(doneCtx, msgBytes)
				if err != nil {
					slog.Error("Failed to handle Azure Queue message",
						slog.Any("error", err),
						slog.String("message_content", *message.MessageText))
				}
			}

			// Delete the message after processing
			ctx, cancel := context.WithTimeout(doneCtx, 10*time.Second)
			_, err := queueClient.QueueClient.DeleteMessage(ctx, *message.MessageID, *message.PopReceipt, nil)
			cancel()
			if err != nil {
				slog.Error("Failed to delete Azure Queue message", slog.Any("error", err))
			}
		}

		if len(result.Messages) == 0 {
			time.Sleep(1 * time.Second)
		}
	}
}

func (ps *AzureQueueService) GetName() string {
	return "azure"
}

// Azure Events are base64 encoded from a few event sources
func decodeIfBase64(s string) []byte {
	// Quick reject: must be multiple of 4
	if len(s)%4 != 0 {
		return []byte(s)
	}

	// Quick reject: only valid base64 chars
	for _, c := range s {
		if !(('A' <= c && c <= 'Z') ||
			('a' <= c && c <= 'z') ||
			('0' <= c && c <= '9') ||
			c == '+' || c == '/' || c == '=') {
			return []byte(s)
		}
	}

	// Try decoding
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return []byte(s) // fallback to original
	}

	return decoded
}
