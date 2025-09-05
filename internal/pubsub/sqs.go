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
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type SQSService struct {
	tracer       trace.Tracer
	awsMgr       *awsclient.Manager
	sp           storageprofile.StorageProfileProvider
	kafkaHandler *KafkaHandler
}

// Ensure SQSService implements Backend interface
var _ Backend = (*SQSService)(nil)

func NewSQSService(kafkaFactory *fly.Factory) (*SQSService, error) {
	awsMgr, err := awsclient.NewManager(context.Background())
	if err != nil {
		slog.Error("Failed to create AWS manager", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

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

	kafkaHandler, err := NewKafkaHandler(kafkaFactory, "sqs", sp, slog.Default())
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka handler: %w", err)
	}

	service := &SQSService{
		tracer:       otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub/sqs"),
		awsMgr:       awsMgr,
		sp:           sp,
		kafkaHandler: kafkaHandler,
	}

	slog.Info("SQS pubsub service initialized with Kafka support")

	return service, nil
}

func (ps *SQSService) Run(doneCtx context.Context) error {
	slog.Info("Starting SQS pubsub service for S3 events")

	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		return fmt.Errorf("SQS_QUEUE_URL environment variable is required")
	}

	region := os.Getenv("SQS_REGION")
	if region == "" {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-west-2"
		}
	}

	// Get role ARN from environment (optional)
	roleARN := os.Getenv("SQS_ROLE_ARN")

	sqsClient, err := ps.awsMgr.GetSQS(context.Background(),
		awsclient.WithSQSRole(roleARN),
		awsclient.WithSQSRegion(region),
	)
	if err != nil {
		slog.Error("Failed to create SQS client", slog.Any("error", err))
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	go ps.pollSQS(doneCtx, sqsClient, queueURL)

	<-doneCtx.Done()

	slog.Info("Shutting down SQS pubsub service")

	// Close Kafka handler if it exists
	if ps.kafkaHandler != nil {
		if err := ps.kafkaHandler.Close(); err != nil {
			slog.Error("Failed to close Kafka handler", slog.Any("error", err))
		}
	}

	return nil
}

func (ps *SQSService) pollSQS(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string) {
	slog.Info("Starting SQS polling loop", slog.String("queueURL", queueURL))

	// Configure concurrency - can be made configurable via env var if needed
	const maxConcurrentMessages = 10

	for {
		select {
		case <-doneCtx.Done():
			slog.Info("SQS polling loop stopped")
			return
		default:
		}

		result, err := sqsClient.Client.ReceiveMessage(doneCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			slog.Error("Failed to receive messages from SQS", slog.Any("error", err))
			time.Sleep(5 * time.Second)
			continue
		}

		if len(result.Messages) == 0 {
			// No messages, continue to next poll
			continue
		}

		// Process messages concurrently
		ps.processMessagesConcurrently(doneCtx, sqsClient, queueURL, result.Messages, maxConcurrentMessages)
	}
}

// processMessagesConcurrently processes SQS messages in parallel with controlled concurrency
func (ps *SQSService) processMessagesConcurrently(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string, messages []types.Message, maxConcurrent int) {
	// Create semaphore for controlling concurrency
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	// Track successful messages for potential batch deletion in future
	var successfulMessages []types.Message
	var successMutex sync.Mutex

	for _, message := range messages {
		// Check if context is cancelled
		select {
		case <-doneCtx.Done():
			slog.Info("Context cancelled, stopping message processing")
			return
		default:
		}

		wg.Add(1)
		sem <- struct{}{} // Acquire semaphore

		go func(msg types.Message) {
			defer wg.Done()
			defer func() { <-sem }() // Release semaphore

			if msg.Body == nil {
				slog.Warn("Received SQS message with nil body")
				return
			}

			// Process message with timeout
			msgCtx, cancel := context.WithTimeout(doneCtx, 30*time.Second)
			defer cancel()

			err := ps.kafkaHandler.HandleMessage(msgCtx, []byte(*msg.Body))
			if err != nil {
				slog.Error("Failed to handle S3 event with Kafka, leaving message in SQS for retry",
					slog.Any("error", err),
					slog.String("messageId", *msg.MessageId))
				return // Don't delete message from SQS if Kafka handling failed
			}

			// Track successful message
			successMutex.Lock()
			successfulMessages = append(successfulMessages, msg)
			successMutex.Unlock()

			// Delete message from SQS after successful processing
			// Using a separate context for deletion to ensure it completes even if main context is cancelled
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer deleteCancel()

			_, err = sqsClient.Client.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			})
			if err != nil {
				slog.Error("Failed to delete SQS message after successful Kafka handling",
					slog.Any("error", err),
					slog.String("messageId", *msg.MessageId))
			} else {
				slog.Debug("Successfully processed and deleted SQS message",
					slog.String("messageId", *msg.MessageId))
			}
		}(message)
	}

	// Wait for all messages to be processed
	wg.Wait()

	if len(successfulMessages) > 0 {
		slog.Info("Batch processing completed",
			slog.Int("total_messages", len(messages)),
			slog.Int("successful_messages", len(successfulMessages)))
	}
}

func (ps *SQSService) GetName() string {
	return "sqs"
}
