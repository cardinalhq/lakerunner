// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
)

type sqsPubsubCmd struct {
	tracer trace.Tracer
	awsMgr *awsclient.Manager
}

func NewSQS() (*sqsPubsubCmd, error) {
	awsMgr, err := awsclient.NewManager(context.Background(),
		awsclient.WithAssumeRoleSessionName("pubsub-sqs"),
	)
	if err != nil {
		slog.Error("Failed to create AWS manager", slog.Any("error", err))
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	return &sqsPubsubCmd{
		tracer: otel.Tracer("github.com/cardinalhq/lakerunner/cmd/pubsub/sqs"),
		awsMgr: awsMgr,
	}, nil
}

func (ps *sqsPubsubCmd) Run(doneCtx context.Context) error {
	slog.Info("Starting SQS pubsub service for S3 events")

	// Get SQS queue URL from environment
	queueURL := os.Getenv("SQS_QUEUE_URL")
	if queueURL == "" {
		return fmt.Errorf("SQS_QUEUE_URL environment variable is required")
	}

	// Get region from environment or use default
	region := os.Getenv("SQS_REGION")
	if region == "" {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-west-2" // Default fallback
		}
	}

	// Get role ARN from environment (optional)
	roleARN := os.Getenv("SQS_ROLE_ARN")

	// Create SQS client
	var sqsClient *awsclient.SQSClient
	var err error

	if roleARN != "" {
		// Use role assumption
		sqsClient, err = ps.awsMgr.GetSQS(context.Background(),
			awsclient.WithSQSRole(roleARN),
			awsclient.WithSQSRegion(region),
		)
	} else {
		// Use default credentials
		sqsClient, err = ps.awsMgr.GetSQS(context.Background(),
			awsclient.WithSQSRegion(region),
		)
	}

	if err != nil {
		slog.Error("Failed to create SQS client", slog.Any("error", err))
		return fmt.Errorf("failed to create SQS client: %w", err)
	}

	// Start SQS polling loop
	go ps.pollSQS(doneCtx, sqsClient, queueURL)

	<-doneCtx.Done()

	slog.Info("Shutting down SQS pubsub service")
	return nil
}

func (ps *sqsPubsubCmd) pollSQS(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string) {
	slog.Info("Starting SQS polling loop", slog.String("queueURL", queueURL))

	for {
		select {
		case <-doneCtx.Done():
			slog.Info("SQS polling loop stopped")
			return
		default:
		}

		// Receive messages from SQS
		result, err := sqsClient.Client.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20, // Long polling
		})

		if err != nil {
			slog.Error("Failed to receive messages from SQS", slog.Any("error", err))
			time.Sleep(5 * time.Second) // Wait before retrying
			continue
		}

		// Process received messages
		for _, message := range result.Messages {
			if message.Body != nil {
				err := ps.handleS3Event(*message.Body)
				if err != nil {
					slog.Error("Failed to handle S3 event", slog.Any("error", err))
					continue // Do not delete the message
				}
			}
			// Delete message only if processing succeeded
			_, err := sqsClient.Client.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: message.ReceiptHandle,
			})

			if err != nil {
				slog.Error("Failed to delete SQS message", slog.Any("error", err))
			}
		}
	}
}

func (ps *sqsPubsubCmd) handleS3Event(messageBody string) error {
	// Parse S3 event using existing function
	items, err := parseS3LikeEvents([]byte(messageBody))
	if err != nil {
		slog.Error("Failed to parse S3 event", slog.Any("error", err))
		return err
	}

	// Process each S3 event record
	for _, item := range items {
		// URL decode the object key (like the Scala code does)
		decodedObjectID := strings.ReplaceAll(item.ObjectID, "%3D", "=")

		slog.Info("Processed S3 event",
			slog.String("bucket", item.Bucket),
			slog.String("object_id", decodedObjectID),
			slog.String("telemetry_type", item.TelemetryType),
			slog.String("collector_name", item.CollectorName),
			slog.String("organization_id", item.OrganizationID.String()),
		)

		fmt.Printf("S3 Event: Bucket=%s, Object=%s, Type=%s, Collector=%s\n",
			item.Bucket, decodedObjectID, item.TelemetryType, item.CollectorName)
	}

	return nil
}
