// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package awsclient

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type SQSClient struct {
	Client *sqs.Client
	Tracer trace.Tracer
}

// ----------------------------------------------------------------
// internal config struct for GetSQS
// ----------------------------------------------------------------
type sqsConfig struct {
	RoleARN      string
	Region       string
	applyConfigs []func(*aws.Config)
	applySQSs    []func(*sqs.Options)
}

// SQSOption is a functional option for GetSQS.
type SQSOption func(*sqsConfig)

// WithSQSRole sets the IAM Role ARN to assume (empty = no assume).
func WithSQSRole(roleARN string) SQSOption {
	return func(c *sqsConfig) {
		c.RoleARN = roleARN
	}
}

// WithSQSRegion overrides the AWS region for this call.
func WithSQSRegion(region string) SQSOption {
	return func(c *sqsConfig) {
		c.Region = region
	}
}

func (m *Manager) GetSQS(ctx context.Context, opts ...SQSOption) (*SQSClient, error) {
	sc := sqsConfig{
		Region: m.baseCfg.Region,
	}
	for _, o := range opts {
		o(&sc)
	}

	key := roleKey{Region: sc.Region, RoleARN: sc.RoleARN}
	m.RLock()
	provider, ok := m.providers[key]
	m.RUnlock()
	if !ok {
		m.Lock()
		if provider, ok = m.providers[key]; !ok {
			if sc.RoleARN == "" {
				provider = m.baseCfg.Credentials
			} else {
				p := stscreds.NewAssumeRoleProvider(m.stsClient, sc.RoleARN, func(o *stscreds.AssumeRoleOptions) {
					o.RoleSessionName = m.sessionName
				})
				provider = aws.NewCredentialsCache(p)
			}
			m.providers[key] = provider
		}
		m.Unlock()
	}

	cfg := m.baseCfg.Copy()
	cfg.Region = sc.Region
	cfg.Credentials = provider
	for _, fn := range sc.applyConfigs {
		fn(&cfg)
	}

	client := sqs.NewFromConfig(cfg, sc.applySQSs...)

	return &SQSClient{Client: client, Tracer: m.tracer}, nil
}

// helper to bind your StorageProfile
func (m *Manager) GetSQSForProfile(ctx context.Context, p storageprofile.StorageProfile) (*SQSClient, error) {
	var opts []SQSOption
	if p.Role != "" {
		opts = append(opts, WithSQSRole(p.Role))
	}
	if p.Region != "" {
		opts = append(opts, WithSQSRegion(p.Region))
	}
	return m.GetSQS(ctx, opts...)
}
