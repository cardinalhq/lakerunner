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

package gcpclient

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Manager handles GCP client creation and caching using Application Default Credentials.
type Manager struct {
	sync.RWMutex
	storageClients map[storageClientKey]*StorageClient
	tracer         trace.Tracer
}

// NewManager creates a new GCP client manager.
func NewManager(ctx context.Context) (*Manager, error) {
	tracer := otel.Tracer("github.com/cardinalhq/lakerunner/internal/gcpclient")
	return &Manager{
		storageClients: make(map[storageClientKey]*StorageClient),
		tracer:         tracer,
	}, nil
}
