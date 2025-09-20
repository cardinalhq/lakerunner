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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type SQSService struct {
	tracer       trace.Tracer
	awsMgr       *awsclient.Manager
	sp           storageprofile.StorageProfileProvider
	kafkaHandler *KafkaHandler
	deduplicator *Deduplicator

	// Async Kafka handling
	maxOutstanding int
	outstanding    chan struct{} // Semaphore for max outstanding messages
}

// Ensure SQSService implements Backend interface
var _ Backend = (*SQSService)(nil)

func NewSQSService(ctx context.Context, cfg *config.Config, kafkaFactory *fly.Factory) (*SQSService, error) {
	awsMgr, err := awsclient.NewManager(ctx)
	if err != nil {
		slog.Error("Failed to create AWS manager", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	cdb, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to configdb", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	lrdbStore, err := lrdb.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lrdb for deduplication: %w", err)
	}
	deduplicator := NewDeduplicator(lrdbStore)

	kafkaHandler, err := NewKafkaHandler(ctx, cfg, kafkaFactory, "sqs", sp, deduplicator)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka handler: %w", err)
	}

	// Configure max outstanding messages (can be made configurable via env var)
	maxOutstanding := 1000

	service := &SQSService{
		tracer:         otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub/sqs"),
		awsMgr:         awsMgr,
		sp:             sp,
		kafkaHandler:   kafkaHandler,
		deduplicator:   deduplicator,
		maxOutstanding: maxOutstanding,
		outstanding:    make(chan struct{}, maxOutstanding),
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

// processMessagesConcurrently processes SQS messages with async Kafka writes and completion-based deletion
func (ps *SQSService) processMessagesConcurrently(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string, messages []types.Message, _ int) {
	// Track completion of Kafka writes for SQS deletion
	type pendingMessage struct {
		sqsMessage    types.Message
		kafkaComplete chan error
	}

	// Channel for completion notifications
	completions := make(chan *pendingMessage, len(messages))
	var wg sync.WaitGroup

	// Start goroutine to handle SQS deletions based on Kafka completions
	wg.Add(1)
	go func() {
		defer wg.Done()
		successCount := 0
		failureCount := 0

		for pending := range completions {
			// Wait for Kafka completion
			err := <-pending.kafkaComplete

			// Release outstanding slot
			<-ps.outstanding

			if err != nil {
				failureCount++
				slog.Error("Kafka write failed, leaving message in SQS for retry",
					slog.Any("error", err),
					slog.String("messageId", *pending.sqsMessage.MessageId))
				continue
			}

			successCount++

			// Delete from SQS after successful Kafka write
			deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, deleteErr := sqsClient.Client.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: pending.sqsMessage.ReceiptHandle,
			})
			deleteCancel()

			if deleteErr != nil {
				slog.Error("Failed to delete SQS message after successful Kafka handling",
					slog.Any("error", deleteErr),
					slog.String("messageId", *pending.sqsMessage.MessageId))
			} else {
				slog.Debug("Successfully processed and deleted SQS message",
					slog.String("messageId", *pending.sqsMessage.MessageId))
			}
		}

		if successCount > 0 || failureCount > 0 {
			slog.Info("Batch processing completed",
				slog.Int("successful", successCount),
				slog.Int("failed", failureCount))
		}
	}()

	// Queue all messages for Kafka processing immediately
	for _, message := range messages {
		// Check if context is cancelled
		select {
		case <-doneCtx.Done():
			slog.Info("Context cancelled, stopping message processing")
			close(completions)
			wg.Wait()
			return
		default:
		}

		if message.Body == nil {
			slog.Warn("Received SQS message with nil body, skipping")
			continue
		}

		// Acquire outstanding slot (blocks if at max)
		select {
		case ps.outstanding <- struct{}{}:
			// Got slot, proceed
		case <-doneCtx.Done():
			slog.Info("Context cancelled while waiting for outstanding slot")
			close(completions)
			wg.Wait()
			return
		}

		pending := &pendingMessage{
			sqsMessage:    message,
			kafkaComplete: make(chan error, 1),
		}

		// Send to completions channel for tracking
		completions <- pending

		// Start async Kafka write immediately (non-blocking)
		go func(msg types.Message, complete chan error) {
			// Process message with timeout
			msgCtx, cancel := context.WithTimeout(doneCtx, 30*time.Second)
			defer cancel()

			// Send to Kafka (this does the batching internally)
			err := ps.kafkaHandler.HandleMessage(msgCtx, []byte(*msg.Body))

			// Signal completion
			complete <- err
			close(complete)
		}(message, pending.kafkaComplete)
	}

	// Close completions channel to signal deletion goroutine to finish
	close(completions)

	// Wait for all deletions to complete
	wg.Wait()
}

func (ps *SQSService) GetName() string {
	return "sqs"
}
