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
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Manager struct {
	baseCred    *azidentity.DefaultAzureCredential
	sessionName string

	sync.RWMutex
	clients map[clientKey]*QueueClient
	tracer  trace.Tracer
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
		baseCred:    cred,
		sessionName: "default-session-name",
		clients:     make(map[clientKey]*QueueClient),
		tracer:      tracer,
	}
	for _, opt := range opts {
		opt(mgr)
	}

	return mgr, nil
}