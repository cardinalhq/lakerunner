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

package gcpclient

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// StorageClient wraps a GCP storage client with OpenTelemetry tracing.
type StorageClient struct {
	Client *storage.Client
	Tracer trace.Tracer
}

// storageClientKey is used for caching storage clients.
type storageClientKey struct {
	ServiceAccountEmail string
}

// storageConfig holds configuration for creating a storage client.
type storageConfig struct {
	ServiceAccountEmail string
}

// StorageOption is a functional option for GetStorage.
type StorageOption func(*storageConfig)

// WithImpersonateServiceAccount sets the service account email to impersonate.
func WithImpersonateServiceAccount(email string) StorageOption {
	return func(c *storageConfig) {
		c.ServiceAccountEmail = email
	}
}

// GetStorage creates a GCP storage client with the given options.
func (m *Manager) GetStorage(ctx context.Context, opts ...StorageOption) (*StorageClient, error) {
	cfg := storageConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	key := storageClientKey(cfg)
	m.RLock()
	client, ok := m.storageClients[key]
	m.RUnlock()
	if ok {
		return client, nil
	}

	m.Lock()
	defer m.Unlock()

	// Double-check after acquiring write lock
	if client, ok = m.storageClients[key]; ok {
		return client, nil
	}

	var clientOpts []option.ClientOption

	if cfg.ServiceAccountEmail != "" {
		ts, err := impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
			TargetPrincipal: cfg.ServiceAccountEmail,
			Scopes:          []string{storage.ScopeFullControl},
		})
		if err != nil {
			return nil, fmt.Errorf("creating impersonated token source: %w", err)
		}
		clientOpts = append(clientOpts, option.WithTokenSource(ts))
	}

	storageClient, err := storage.NewClient(ctx, clientOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating GCP storage client: %w", err)
	}

	client = &StorageClient{
		Client: storageClient,
		Tracer: m.tracer,
	}
	m.storageClients[key] = client

	return client, nil
}

// GetStorageForProfile creates a GCP storage client for a storage profile.
func (m *Manager) GetStorageForProfile(ctx context.Context, p storageprofile.StorageProfile) (*StorageClient, error) {
	var opts []StorageOption

	// Use Role field for service account impersonation (similar to AWS AssumeRole)
	if p.Role != "" {
		opts = append(opts, WithImpersonateServiceAccount(p.Role))
	}

	return m.GetStorage(ctx, opts...)
}
