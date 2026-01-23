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

package awsclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
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

// WithGCPProvider enables GCP-specific S3 options for GCP Cloud Storage compatibility
func WithGCPProvider() S3Option {
	return func(c *s3Config) {
		c.applyConfigs = append(c.applyConfigs, func(cfg *aws.Config) {
			cfg.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
			// GCP may transparently decompress .gz files during download, which causes
			// the downloaded bytes to not match the stored checksum (computed on compressed data).
			cfg.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		})
		c.applyS3s = append(c.applyS3s, func(o *s3.Options) {
			SignForGCP(o)
		})
	}
}

const acceptEncodingHeader = "Accept-Encoding"

type acceptEncodingKey struct{}

func GetAcceptEncodingKey(ctx context.Context) (v string) {
	v, _ = middleware.GetStackValue(ctx, acceptEncodingKey{}).(string)
	return v
}

func SetAcceptEncodingKey(ctx context.Context, value string) context.Context {
	return middleware.WithStackValue(ctx, acceptEncodingKey{}, value)
}

var dropAcceptEncodingHeader = middleware.FinalizeMiddlewareFunc("DropAcceptEncodingHeader",
	func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return out, metadata, &v4.SigningError{Err: fmt.Errorf("unexpected request middleware type %T", in.Request)}
		}

		ae := req.Header.Get(acceptEncodingHeader)
		ctx = SetAcceptEncodingKey(ctx, ae)
		req.Header.Del(acceptEncodingHeader)
		in.Request = req

		return next.HandleFinalize(ctx, in)
	},
)

var replaceAcceptEncodingHeader = middleware.FinalizeMiddlewareFunc("ReplaceAcceptEncodingHeader",
	func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (out middleware.FinalizeOutput, metadata middleware.Metadata, err error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return out, metadata, &v4.SigningError{Err: fmt.Errorf("unexpected request middleware type %T", in.Request)}
		}

		ae := GetAcceptEncodingKey(ctx)
		req.Header.Set(acceptEncodingHeader, ae)
		in.Request = req

		return next.HandleFinalize(ctx, in)
	},
)

func SignForGCP(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		if err := stack.Finalize.Insert(dropAcceptEncodingHeader, "Signing", middleware.Before); err != nil {
			return err
		}

		if err := stack.Finalize.Insert(replaceAcceptEncodingHeader, "Signing", middleware.After); err != nil {
			return err
		}

		return nil
	})
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
	// Automatically enable GCP signing if the cloud provider is GCP
	if p.CloudProvider == "gcp" {
		opts = append(opts, WithGCPProvider())
	}
	return m.GetS3(ctx, opts...)
}
