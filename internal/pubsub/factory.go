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

package pubsub

import (
	"context"
	"fmt"
)

// NewBackend creates a new Backend implementation based on the specified type
func NewBackend(ctx context.Context, backendType BackendType) (Backend, error) {
	switch backendType {
	case BackendTypeSQS:
		return NewSQSService()
	case BackendTypeGCPPubSub:
		return NewGCPPubSubService()
	case BackendTypeAzure:
		return NewAzureQueueService()
	default:
		return nil, fmt.Errorf("unsupported backend type: %s", backendType)
	}
}
