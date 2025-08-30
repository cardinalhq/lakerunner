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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type SQSService struct {
	tracer trace.Tracer
	awsMgr *awsclient.Manager
	sp     storageprofile.StorageProfileProvider
	mdb    InqueueInserter
}

// Ensure SQSService implements Backend interface
var _ Backend = (*SQSService)(nil)

func NewSQSService() (*SQSService, error) {
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

	mdb, err := dbopen.LRDBStore(context.Background())
	if err != nil {
		slog.Error("Failed to connect to lr database", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to lr database: %w", err)
	}

	return &SQSService{
		tracer: otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub/sqs"),
		awsMgr: awsMgr,
		sp:     sp,
		mdb:    mdb,
	}, nil
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
	return nil
}

func (ps *SQSService) pollSQS(doneCtx context.Context, sqsClient *awsclient.SQSClient, queueURL string) {
	slog.Info("Starting SQS polling loop", slog.String("queueURL", queueURL))

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

		for _, message := range result.Messages {
			if message.Body != nil {
				err := handleMessage(context.Background(), []byte(*message.Body), ps.sp, ps.mdb)
				if err != nil {
					slog.Error("Failed to handle S3 event", slog.Any("error", err))
				}
			}
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

func (ps *SQSService) GetName() string {
	return "sqs"
}
