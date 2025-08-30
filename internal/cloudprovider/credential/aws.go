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

package credential

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// AWSProvider provides AWS credentials
type AWSProvider struct {
	logger *slog.Logger
}

// NewAWSProvider creates a new AWS credential provider
func NewAWSProvider(logger *slog.Logger) Provider {
	return &AWSProvider{logger: logger}
}

// AWSCredentials implements Credentials for AWS
type AWSCredentials struct {
	cfg aws.Config
}

// AccessKey returns the AWS access key
func (c *AWSCredentials) AccessKey() string {
	creds, err := c.cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return ""
	}
	return creds.AccessKeyID
}

// SecretKey returns the AWS secret key
func (c *AWSCredentials) SecretKey() string {
	creds, err := c.cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return ""
	}
	return creds.SecretAccessKey
}

// Token returns the AWS session token
func (c *AWSCredentials) Token() string {
	creds, err := c.cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return ""
	}
	return creds.SessionToken
}

// IsExpired checks if the AWS credentials have expired
func (c *AWSCredentials) IsExpired() bool {
	creds, err := c.cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		return true
	}
	return creds.Expired()
}

// GetAWSConfig returns the underlying AWS config
func (c *AWSCredentials) GetAWSConfig() aws.Config {
	return c.cfg
}

// GetCredentials returns AWS credentials for the specified configuration
func (p *AWSProvider) GetCredentials(ctx context.Context, credConfig CredentialConfig) (Credentials, error) {
	if credConfig.Provider != "aws" {
		return nil, fmt.Errorf("provider %s not supported by AWSProvider", credConfig.Provider)
	}

	var cfg aws.Config
	var err error

	// Load base configuration
	if credConfig.Profile != "" {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithSharedConfigProfile(credConfig.Profile),
			config.WithRegion(credConfig.Region),
		)
	} else {
		cfg, err = config.LoadDefaultConfig(ctx,
			config.WithRegion(credConfig.Region),
		)
	}
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	// Handle role assumption if specified
	if credConfig.Role != "" {
		stsClient := sts.NewFromConfig(cfg)

		// Handle custom endpoint if specified in settings
		// TODO: Implement custom endpoint resolution if needed

		// Assume the role with session name "lakerunner"
		cfg.Credentials = stscreds.NewAssumeRoleProvider(stsClient, credConfig.Role, func(aro *stscreds.AssumeRoleOptions) {
			aro.RoleSessionName = "lakerunner"
			aro.Duration = 60 * time.Minute
		})
	}

	return &AWSCredentials{cfg: cfg}, nil
}
