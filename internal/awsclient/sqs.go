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

package awsclient

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
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
