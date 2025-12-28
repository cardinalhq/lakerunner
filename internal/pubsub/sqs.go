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
	deduplicator Deduplicator
	sqsConfig    config.PubSubSQSConfig

	// Async Kafka handling
	outstanding chan struct{} // Semaphore for max outstanding messages
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

	sqsConfig := cfg.PubSub.SQS

	service := &SQSService{
		tracer:       otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub/sqs"),
		awsMgr:       awsMgr,
		sp:           sp,
		kafkaHandler: kafkaHandler,
		deduplicator: deduplicator,
		sqsConfig:    sqsConfig,
		outstanding:  make(chan struct{}, sqsConfig.MaxOutstanding),
	}

	slog.Info("SQS pubsub service initialized",
		slog.Int("numPollers", sqsConfig.NumPollers),
		slog.Int("maxOutstanding", sqsConfig.MaxOutstanding))

	return service, nil
}

func (ps *SQSService) Run(doneCtx context.Context) error {
	slog.Info("Starting SQS pubsub service for S3 events")

	queueURL := ps.sqsConfig.QueueURL
	if queueURL == "" {
		queueURL = os.Getenv("SQS_QUEUE_URL")
		if queueURL == "" {
			return fmt.Errorf("LAKERUNNER_PUBSUB_SQS_QUEUE_URL is required")
		}
	}

	region := ps.sqsConfig.Region
	if region == "" {
		region = os.Getenv("SQS_REGION")
		if region == "" {
			region = os.Getenv("AWS_REGION")
			if region == "" {
				region = "us-west-2"
			}
		}
	}

	roleARN := ps.sqsConfig.RoleARN
	if roleARN == "" {
		roleARN = os.Getenv("SQS_ROLE_ARN")
	}

	sqsClient, err := ps.awsMgr.GetSQS(context.Background(),
		awsclient.WithSQSRole(roleARN),
		awsclient.WithSQSRegion(region),
	)
	if err != nil {
		slog.Error("Failed to create SQS client", slog.Any("error", err))
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	// Start multiple parallel polling goroutines
	var wg sync.WaitGroup
	for i := range ps.sqsConfig.NumPollers {
		wg.Add(1)
		go func(pollerID int) {
			defer wg.Done()
			ps.pollSQS(doneCtx, sqsClient, queueURL, pollerID)
		}(i)
	}

	<-doneCtx.Done()

	slog.Info("Shutting down SQS pubsub service, waiting for pollers...")
	wg.Wait()

	// Close Kafka handler if it exists
	if ps.kafkaHandler != nil {
		if err := ps.kafkaHandler.Close(); err != nil {
			slog.Error("Failed to close Kafka handler", slog.Any("error", err))
		}
	}

	return nil
}

func (ps *SQSService) pollSQS(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string, pollerID int) {
	slog.Info("Starting SQS polling loop",
		slog.String("queueURL", queueURL),
		slog.Int("pollerID", pollerID))

	for {
		select {
		case <-doneCtx.Done():
			slog.Info("SQS polling loop stopped", slog.Int("pollerID", pollerID))
			return
		default:
		}

		result, err := sqsClient.Client.ReceiveMessage(doneCtx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if doneCtx.Err() != nil {
				return // Context cancelled, exit gracefully
			}
			slog.Error("Failed to receive messages from SQS",
				slog.Any("error", err),
				slog.Int("pollerID", pollerID))
			time.Sleep(5 * time.Second)
			continue
		}

		if len(result.Messages) == 0 {
			continue
		}

		// Process messages (non-blocking - fires off goroutines and returns immediately)
		ps.processMessages(doneCtx, sqsClient, queueURL, result.Messages)
	}
}

// processMessages processes SQS messages with async Kafka writes (non-blocking)
func (ps *SQSService) processMessages(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string, messages []types.Message) {
	for _, message := range messages {
		// Check if context is cancelled
		select {
		case <-doneCtx.Done():
			return
		default:
		}

		if message.Body == nil {
			slog.Warn("Received SQS message with nil body, skipping")
			continue
		}

		// Acquire outstanding slot (blocks if at max - provides backpressure)
		select {
		case ps.outstanding <- struct{}{}:
			// Got slot, proceed
		case <-doneCtx.Done():
			return
		}

		// Start async processing (non-blocking)
		go ps.handleMessage(doneCtx, sqsClient, queueURL, message)
	}
}

// handleMessage processes a single message and deletes on success
func (ps *SQSService) handleMessage(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string, msg types.Message) {
	defer func() { <-ps.outstanding }() // Release slot when done

	// Process message with timeout
	msgCtx, cancel := context.WithTimeout(doneCtx, 30*time.Second)
	defer cancel()

	// Send to Kafka
	err := ps.kafkaHandler.HandleMessage(msgCtx, []byte(*msg.Body))
	if err != nil {
		slog.Error("Kafka write failed, leaving message in SQS for retry",
			slog.Any("error", err),
			slog.String("messageId", *msg.MessageId))
		return
	}

	// Delete from SQS
	deleteCtx, deleteCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, deleteErr := sqsClient.Client.DeleteMessage(deleteCtx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueURL),
		ReceiptHandle: msg.ReceiptHandle,
	})
	deleteCancel()

	if deleteErr != nil {
		slog.Error("Failed to delete SQS message",
			slog.Any("error", deleteErr),
			slog.String("messageId", *msg.MessageId))
	}
}

func (ps *SQSService) GetName() string {
	return "sqs"
}
