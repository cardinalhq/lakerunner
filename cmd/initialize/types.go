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

package initialize

import (
	"github.com/google/uuid"
)

// StorageProfile represents the YAML structure for storage profile initialization
type StorageProfile struct {
	OrganizationID uuid.UUID `yaml:"organization_id"`
	InstanceNum    int       `yaml:"instance_num,omitempty"`   // Defaults to 1 if not specified
	CollectorName  string    `yaml:"collector_name,omitempty"` // Defaults to "default" if not specified
	CloudProvider  string    `yaml:"cloud_provider"`
	Region         string    `yaml:"region"`
	Bucket         string    `yaml:"bucket"`
	Role           string    `yaml:"role,omitempty"`
	Endpoint       string    `yaml:"endpoint,omitempty"`
	UsePathStyle   bool      `yaml:"use_path_style,omitempty"`
	InsecureTLS    bool      `yaml:"insecure_tls,omitempty"`
}

// API Keys configuration
type APIKeysConfig []APIKeyOrg

type APIKeyOrg struct {
	OrganizationID uuid.UUID `yaml:"organization_id"`
	Keys           []string  `yaml:"keys"`
}
