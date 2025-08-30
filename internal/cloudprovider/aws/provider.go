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

package aws

import (
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider"
	"github.com/cardinalhq/lakerunner/internal/pubsub"
)

// AWSProvider implements the CloudProvider interface for AWS
type AWSProvider struct {
	awsManager *awsclient.Manager
}

// NewAWSProvider creates a new AWS provider
func NewAWSProvider() (cloudprovider.CloudProvider, error) {
	// Create AWS manager - this will be initialized when needed
	return &AWSProvider{}, nil
}

func (p *AWSProvider) Name() string {
	return "Amazon Web Services"
}

func (p *AWSProvider) Type() cloudprovider.ProviderType {
	return cloudprovider.ProviderAWS
}

func (p *AWSProvider) CreateObjectStoreClient(ctx context.Context, config cloudprovider.ObjectStoreConfig) (cloudprovider.ObjectStoreClient, error) {
	// Initialize AWS manager if not already done
	if p.awsManager == nil {
		var err error
		p.awsManager, err = awsclient.NewManager(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create AWS manager: %w", err)
		}
	}

	// Build S3 options from config
	var opts []awsclient.S3Option

	if config.Region != "" {
		opts = append(opts, awsclient.WithRegion(config.Region))
	}

	if config.Role != "" {
		opts = append(opts, awsclient.WithRole(config.Role))
	}

	if config.Endpoint != "" {
		opts = append(opts, awsclient.WithEndpoint(config.Endpoint))
	}

	if config.UsePathStyle {
		opts = append(opts, awsclient.WithPathStyle())
	}

	if config.InsecureTLS {
		opts = append(opts, awsclient.WithInsecureTLS())
	}

	s3Client, err := p.awsManager.GetS3(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	return &AWSObjectStoreClient{s3Client: s3Client}, nil
}

func (p *AWSProvider) CreatePubSubBackend(ctx context.Context, config cloudprovider.PubSubConfig) (pubsub.Backend, error) {
	// If no specific type requested, use AWS SQS as default (unless using S3-compatible endpoint)
	if config.Type == "" {
		return pubsub.NewBackend(ctx, pubsub.BackendTypeSQS)
	}

	// Handle specific pubsub types
	switch config.Type {
	case "sqs":
		return pubsub.NewBackend(ctx, pubsub.BackendTypeSQS)
	case "http":
		return pubsub.NewBackend(ctx, pubsub.BackendTypeHTTP)
	default:
		return nil, fmt.Errorf("AWS provider does not support pubsub type: %s", config.Type)
	}
}

func (p *AWSProvider) SupportsFeature(feature string) bool {
	switch feature {
	case cloudprovider.FeatureMultiRegion:
		return true
	case cloudprovider.FeatureCrossAccount:
		return true
	case cloudprovider.FeatureKubernetesAuth:
		return true // IRSA support
	case cloudprovider.FeatureBucketNotifications:
		return true // SQS notifications
	default:
		return false
	}
}

func (p *AWSProvider) Validate(config cloudprovider.ProviderConfig) error {
	if config.ObjectStore.Bucket == "" {
		return fmt.Errorf("bucket is required for AWS provider")
	}

	// Region defaults to us-east-1 if not specified
	if config.ObjectStore.Region == "" {
		config.ObjectStore.Region = "us-east-1"
	}

	// For S3-compatible endpoints, credentials are usually required but
	// we let the AWS SDK handle credential resolution (env vars, IAM, etc.)

	return nil
}

// AWSObjectStoreClient wraps the existing S3Client
type AWSObjectStoreClient struct {
	s3Client *awsclient.S3Client
}

func (c *AWSObjectStoreClient) GetS3Client() *awsclient.S3Client {
	return c.s3Client
}
