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

package fingerprint

import (
	"sync"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
)

// Tenant holds the fingerprinting state for a specific organization
type Tenant struct {
	trieClusterManager *fingerprinter.TrieClusterManager
}

// GetTrieClusterManager returns the TrieClusterManager for this tenant
func (t *Tenant) GetTrieClusterManager() *fingerprinter.TrieClusterManager {
	return t.trieClusterManager
}

// TenantManager manages fingerprinting tenants per organization
type TenantManager struct {
	tenants   sync.Map // organizationID (string) -> *Tenant
	threshold float64
}

// NewTenantManager creates a new fingerprinting tenant manager
func NewTenantManager(threshold float64) *TenantManager {
	return &TenantManager{
		tenants:   sync.Map{},
		threshold: threshold,
	}
}

// GetTenant retrieves or creates a tenant for the given organization ID
func (tm *TenantManager) GetTenant(organizationID string) *Tenant {
	if existing, ok := tm.tenants.Load(organizationID); ok {
		return existing.(*Tenant)
	}

	tenant := &Tenant{
		trieClusterManager: fingerprinter.NewTrieClusterManager(tm.threshold),
	}

	tm.tenants.Store(organizationID, tenant)
	return tenant
}
