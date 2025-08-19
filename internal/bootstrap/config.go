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

package bootstrap

import (
	"github.com/google/uuid"
)

// BootstrapConfig represents the YAML structure for one-time import
type BootstrapConfig struct {
	Version                   int                          `yaml:"version" json:"version"`
	BucketConfigurations      []BucketConfiguration        `yaml:"bucket_configurations,omitempty" json:"bucket_configurations,omitempty"`
	OrganizationBuckets       []OrganizationBucket         `yaml:"organization_buckets,omitempty" json:"organization_buckets,omitempty"`
	BucketPrefixMappings      []BucketPrefixMapping        `yaml:"bucket_prefix_mappings,omitempty" json:"bucket_prefix_mappings,omitempty"`
	AdminAPIKeys              []AdminAPIKey                `yaml:"admin_api_keys,omitempty" json:"admin_api_keys,omitempty"`
	OrganizationAPIKeys       []OrganizationAPIKey         `yaml:"organization_api_keys,omitempty" json:"organization_api_keys,omitempty"`
	OrganizationAPIKeyMapping []OrganizationAPIKeyMapping  `yaml:"organization_api_key_mappings,omitempty" json:"organization_api_key_mappings,omitempty"`
}

// BucketConfiguration matches lrconfig_bucket_configurations table
type BucketConfiguration struct {
	ID            *uuid.UUID `yaml:"id,omitempty" json:"id,omitempty"`
	BucketName    string     `yaml:"bucket_name" json:"bucket_name"`
	CloudProvider string     `yaml:"cloud_provider" json:"cloud_provider"`
	Region        string     `yaml:"region" json:"region"`
	Endpoint      *string    `yaml:"endpoint,omitempty" json:"endpoint,omitempty"`
	Role          *string    `yaml:"role,omitempty" json:"role,omitempty"`
}

// OrganizationBucket matches lrconfig_organization_buckets table
type OrganizationBucket struct {
	ID             *uuid.UUID `yaml:"id,omitempty" json:"id,omitempty"`
	OrganizationID uuid.UUID  `yaml:"organization_id" json:"organization_id"`
	BucketID       uuid.UUID  `yaml:"bucket_id" json:"bucket_id"`
}

// BucketPrefixMapping matches lrconfig_bucket_prefix_mappings table
type BucketPrefixMapping struct {
	ID             *uuid.UUID `yaml:"id,omitempty" json:"id,omitempty"`
	BucketID       uuid.UUID  `yaml:"bucket_id" json:"bucket_id"`
	OrganizationID uuid.UUID  `yaml:"organization_id" json:"organization_id"`
	PathPrefix     string     `yaml:"path_prefix" json:"path_prefix"`
	Signal         string     `yaml:"signal" json:"signal"` // logs, metrics, traces
}

// AdminAPIKey matches lrconfig_admin_api_keys table
type AdminAPIKey struct {
	ID          *uuid.UUID `yaml:"id,omitempty" json:"id,omitempty"`
	KeyHash     *string    `yaml:"key_hash,omitempty" json:"key_hash,omitempty"` // Will be computed from Key
	Key         string     `yaml:"key" json:"key"`                               // Plaintext for import only
	Name        string     `yaml:"name" json:"name"`
	Description *string    `yaml:"description,omitempty" json:"description,omitempty"`
}

// OrganizationAPIKey matches lrconfig_organization_api_keys table
type OrganizationAPIKey struct {
	ID          *uuid.UUID `yaml:"id,omitempty" json:"id,omitempty"`
	KeyHash     *string    `yaml:"key_hash,omitempty" json:"key_hash,omitempty"` // Will be computed from Key
	Key         string     `yaml:"key" json:"key"`                               // Plaintext for import only
	Name        string     `yaml:"name" json:"name"`
	Description *string    `yaml:"description,omitempty" json:"description,omitempty"`
}

// OrganizationAPIKeyMapping matches lrconfig_organization_api_key_mappings table
type OrganizationAPIKeyMapping struct {
	ID             *uuid.UUID `yaml:"id,omitempty" json:"id,omitempty"`
	APIKeyID       uuid.UUID  `yaml:"api_key_id" json:"api_key_id"`
	OrganizationID uuid.UUID  `yaml:"organization_id" json:"organization_id"`
}