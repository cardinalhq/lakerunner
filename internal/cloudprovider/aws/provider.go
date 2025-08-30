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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/credential"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/objstore"
	"github.com/cardinalhq/lakerunner/internal/cloudprovider/pubsub"
)

// AWSProvider implements the CloudProvider interface for AWS
type AWSProvider struct {
	sessionName        string
	credentialProvider credential.Provider
}

var _ cloudprovider.ObjectStoreProvider = (*AWSProvider)(nil)
var _ cloudprovider.PubSubProvider = (*AWSProvider)(nil)
var _ cloudprovider.CredentialProvider = (*AWSProvider)(nil)

// NewAWSProvider creates a new AWS provider
func NewAWSProvider() (cloudprovider.CloudProvider, error) {
	credProvider := credential.NewAWSProvider(nil)
	return &AWSProvider{
		credentialProvider: credProvider,
	}, nil
}

func (p *AWSProvider) Name() string {
	return "Amazon Web Services"
}

func (p *AWSProvider) Type() cloudprovider.ProviderType {
	return cloudprovider.ProviderAWS
}

func (p *AWSProvider) GetCredentialProvider() credential.Provider {
	return p.credentialProvider
}

func (p *AWSProvider) CreateObjectStoreClient(ctx context.Context, config cloudprovider.ObjectStoreConfig) (cloudprovider.ObjectStoreClient, error) {
	// Convert config to credential config and get credentials
	credConfig := credential.CredentialConfig{
		Provider: "aws",
		Region:   config.Region,
		Role:     config.Role,
		Settings: make(map[string]any),
	}

	// Add provider-specific settings
	if config.Endpoint != "" {
		credConfig.Settings["endpoint"] = config.Endpoint
	}

	// Get credentials
	creds, err := p.credentialProvider.GetCredentials(ctx, credConfig)
	if err != nil {
		return nil, fmt.Errorf("getting AWS credentials: %w", err)
	}

	// Extract AWS config
	awsCreds, ok := creds.(*credential.AWSCredentials)
	if !ok {
		return nil, fmt.Errorf("expected AWS credentials, got %T", creds)
	}

	// Create S3 client and manager
	s3Client := s3.NewFromConfig(awsCreds.GetAWSConfig())
	return objstore.NewAWSS3Manager(s3Client), nil
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
