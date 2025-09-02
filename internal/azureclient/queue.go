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

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"go.opentelemetry.io/otel/trace"
)

type QueueClient struct {
	ServiceClient *azqueue.ServiceClient
	QueueClient   *azqueue.QueueClient
	Tracer        trace.Tracer
}

type queueConfig struct {
	StorageAccount string
	QueueName      string
	applyOptions   []func(*azqueue.ClientOptions)
}

type QueueOption func(*queueConfig)

func WithQueueStorageAccount(storageAccount string) QueueOption {
	return func(c *queueConfig) {
		c.StorageAccount = storageAccount
	}
}

func WithQueueName(queueName string) QueueOption {
	return func(c *queueConfig) {
		c.QueueName = queueName
	}
}

type clientKey struct {
	StorageAccount string
	QueueName      string
}

func (m *Manager) GetQueue(ctx context.Context, opts ...QueueOption) (*QueueClient, error) {
	qc := queueConfig{}
	for _, o := range opts {
		o(&qc)
	}

	if qc.StorageAccount == "" {
		return nil, fmt.Errorf("storage account is required")
	}

	if qc.QueueName == "" {
		return nil, fmt.Errorf("queue name is required")
	}

	key := clientKey{StorageAccount: qc.StorageAccount, QueueName: qc.QueueName}
	m.RLock()
	client, ok := m.clients[key]
	m.RUnlock()
	if !ok {
		m.Lock()
		if client, ok = m.clients[key]; !ok {
			serviceURL := fmt.Sprintf("https://%s.queue.core.windows.net", qc.StorageAccount)
			serviceClient, err := azqueue.NewServiceClient(serviceURL, m.baseCred, nil)
			if err != nil {
				m.Unlock()
				return nil, fmt.Errorf("failed to create service client: %w", err)
			}

			queueClient := serviceClient.NewQueueClient(qc.QueueName)
			client = &QueueClient{
				ServiceClient: serviceClient,
				QueueClient:   queueClient,
				Tracer:        m.tracer,
			}
			m.clients[key] = client
		}
		m.Unlock()
	}

	return client, nil
}