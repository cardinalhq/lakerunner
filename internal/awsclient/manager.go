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

// NewManager initializes AWS config + a single STS client.
func NewManager(ctx context.Context) (*Manager, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	otelaws.AppendMiddlewares(&cfg.APIOptions)

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/internal/awsclient")
	mgr := &Manager{
		baseCfg:     cfg,
		stsClient:   sts.NewFromConfig(cfg),
		sessionName: "lakerunner",
		providers:   make(map[roleKey]aws.CredentialsProvider),
		tracer:      tracer,
	}

	return mgr, nil
}
