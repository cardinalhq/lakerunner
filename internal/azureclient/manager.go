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

package azureclient

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type Manager struct {
	baseCred    *azidentity.DefaultAzureCredential
	sessionName string

	sync.RWMutex
	queueClients map[clientKey]*QueueClient
	blobClients  map[blobClientKey]*BlobClient
	tracer       trace.Tracer
}

// ManagerOption is a functional option for configuring the Manager.
type ManagerOption func(*Manager)

func WithAssumeRoleSessionName(name string) ManagerOption {
	return func(mgr *Manager) {
		mgr.sessionName = name
	}
}

// NewManager initializes Azure config + credential management.
func NewManager(ctx context.Context, opts ...ManagerOption) (*Manager, error) {
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("loading Azure credentials: %w", err)
	}

	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/internal/azureclient")
	mgr := &Manager{
		baseCred:     cred,
		sessionName:  "default-session-name",
		queueClients: make(map[clientKey]*QueueClient),
		blobClients:  make(map[blobClientKey]*BlobClient),
		tracer:       tracer,
	}
	for _, opt := range opts {
		opt(mgr)
	}

	return mgr, nil
}

// GetBlobForProfile helper to bind your StorageProfile
func (m *Manager) GetBlobForProfile(ctx context.Context, p storageprofile.StorageProfile) (*BlobClient, error) {
	if p.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required for Azure blob storage")
	}

	// Extract storage account from endpoint
	storageAccount, err := extractStorageAccountFromEndpoint(p.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to extract storage account from endpoint %s: %w", p.Endpoint, err)
	}

	var opts []BlobOption
	opts = append(opts, WithBlobEndpoint(p.Endpoint))
	opts = append(opts, WithBlobStorageAccount(storageAccount))

	return m.GetBlob(ctx, opts...)
}

func extractStorageAccountFromEndpoint(endpoint string) (string, error) {
	if endpoint == "" {
		return "", fmt.Errorf("empty endpoint")
	}

	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("invalid endpoint: %w", err)
	}

	host := u.Hostname()
	parts := strings.Split(host, ".")
	if len(parts) < 1 {
		return "", fmt.Errorf("could not parse account name from host: %s", host)
	}

	return parts[0], nil
}
