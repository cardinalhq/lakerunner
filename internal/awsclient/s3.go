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
	"crypto/tls"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
)

type S3Client struct {
	Client *s3.Client
	Tracer trace.Tracer
}

// ----------------------------------------------------------------
// internal config struct for GetS3
// ----------------------------------------------------------------
type s3Config struct {
	RoleARN      string
	Region       string
	applyConfigs []func(*aws.Config)
	applyS3s     []func(*s3.Options)
}

// S3Option is a functional option for GetS3.
type S3Option func(*s3Config)

// WithRole sets the IAM Role ARN to assume (empty = no assume).
func WithRole(roleARN string) S3Option {
	return func(c *s3Config) {
		c.RoleARN = roleARN
	}
}

// WithRegion overrides the AWS region for this call.
func WithRegion(region string) S3Option {
	return func(c *s3Config) {
		c.Region = region
	}
}

// WithEndpoint forces a custom S3 endpoint (eg MinIO, Ceph).
func WithEndpoint(url string) S3Option {
	return func(c *s3Config) {
		c.applyS3s = append(c.applyS3s, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(url)
		})
	}
}

// WithPathStyle uses path-style addressing instead of virtual-host.
func WithPathStyle() S3Option {
	return func(c *s3Config) {
		c.applyS3s = append(c.applyS3s, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}
}

// WithInsecureTLS turns off cert verification (for self-signed or insecure).
func WithInsecureTLS() S3Option {
	return func(c *s3Config) {
		c.applyConfigs = append(c.applyConfigs, func(cfg *aws.Config) {
			tr := http.DefaultTransport.(*http.Transport).Clone()
			tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
			cfg.HTTPClient = &http.Client{Transport: tr}
		})
	}
}

type roleKey struct {
	Region  string
	RoleARN string
}

func (m *Manager) GetS3(ctx context.Context, opts ...S3Option) (*S3Client, error) {
	sc := s3Config{
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

	client := s3.NewFromConfig(cfg, sc.applyS3s...)

	return &S3Client{Client: client, Tracer: m.tracer}, nil
}

// helper to bind your StorageProfile
func (m *Manager) GetS3ForProfile(ctx context.Context, p storageprofile.StorageProfile) (*S3Client, error) {
	var opts []S3Option
	if p.Role != "" {
		opts = append(opts, WithRole(p.Role))
	}
	if p.Region != "" {
		opts = append(opts, WithRegion(p.Region))
	}
	if p.Endpoint != "" {
		opts = append(opts, WithEndpoint(p.Endpoint))
	}
	if p.UsePathStyle {
		opts = append(opts, WithPathStyle())
	}
	if p.InsecureTLS {
		opts = append(opts, WithInsecureTLS())
	}
	return m.GetS3(ctx, opts...)
}

// type customResolverV2 struct{ url string }

// func (r customResolverV2) ResolveEndpoint(
// 	ctx context.Context,
// 	params s3.EndpointParameters,
// ) (smithyendpoint.Endpoint, error) {
// 	return smithyendpoint.Endpoint{
// 		URL:               r.url,
// 		SigningRegion:     params.Region,
// 		HostnameImmutable: true,
// 	}, nil
// }
