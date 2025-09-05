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

package azureclient

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"go.opentelemetry.io/otel/trace"
)

type BlobClient struct {
	ServiceClient *service.Client
	Client        *azblob.Client
	Tracer        trace.Tracer
}

type blobConfig struct {
	StorageAccount string
	Endpoint       string
}

type BlobOption func(*blobConfig)

func WithBlobStorageAccount(storageAccount string) BlobOption {
	return func(c *blobConfig) {
		c.StorageAccount = storageAccount
	}
}

func WithBlobEndpoint(endpoint string) BlobOption {
	return func(c *blobConfig) {
		c.Endpoint = endpoint
	}
}

type blobClientKey struct {
	StorageAccount string
}

func (m *Manager) GetBlob(ctx context.Context, opts ...BlobOption) (*BlobClient, error) {
	bc := blobConfig{}
	for _, o := range opts {
		o(&bc)
	}

	if bc.StorageAccount == "" {
		return nil, fmt.Errorf("storage account is required")
	}

	if bc.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}

	key := blobClientKey{StorageAccount: bc.StorageAccount}
	m.RLock()
	client, ok := m.blobClients[key]
	m.RUnlock()
	if !ok {
		m.Lock()
		if client, ok = m.blobClients[key]; !ok {
			serviceClient, err := service.NewClient(bc.Endpoint, m.baseCred, nil)
			if err != nil {
				m.Unlock()
				return nil, fmt.Errorf("failed to create blob service client: %w", err)
			}

			blobClient, err := azblob.NewClient(bc.Endpoint, m.baseCred, nil)
			if err != nil {
				m.Unlock()
				return nil, fmt.Errorf("failed to create blob client: %w", err)
			}

			client = &BlobClient{
				ServiceClient: serviceClient,
				Client:        blobClient,
				Tracer:        m.tracer,
			}
			m.blobClients[key] = client
		}
		m.Unlock()
	}

	return client, nil
}
