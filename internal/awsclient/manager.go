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
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Manager struct {
	baseCfg     aws.Config
	stsClient   *sts.Client
	sessionName string

	sync.RWMutex
	providers map[roleKey]aws.CredentialsProvider
	tracer    trace.Tracer
}

// ManagerOption is a functional option for configuring the Manager.
type ManagerOption func(*Manager)

func WithAssumeRoleSessionName(name string) ManagerOption {
	return func(mgr *Manager) {
		mgr.sessionName = name
	}
}

// NewManager initializes AWS config + a single STS client.
func NewManager(ctx context.Context, opts ...ManagerOption) (*Manager, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	otelaws.AppendMiddlewares(&cfg.APIOptions)

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/internal/awsclient")
	mgr := &Manager{
		baseCfg:     cfg,
		stsClient:   sts.NewFromConfig(cfg),
		sessionName: "default-session-name",
		providers:   make(map[roleKey]aws.CredentialsProvider),
		tracer:      tracer,
	}
	for _, opt := range opts {
		opt(mgr)
	}

	return mgr, nil
}
