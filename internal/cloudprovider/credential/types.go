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

package credential

import (
	"context"
)

// Provider manages credentials for cloud services
type Provider interface {
	// GetCredentials returns credentials for the specified profile/configuration
	GetCredentials(ctx context.Context, config CredentialConfig) (Credentials, error)
}

// Credentials represents authentication credentials for a cloud service
type Credentials interface {
	// AccessKey returns the access key (if applicable)
	AccessKey() string

	// SecretKey returns the secret key (if applicable)
	SecretKey() string

	// Token returns a temporary session token (if applicable)
	Token() string

	// IsExpired checks if the credentials have expired
	IsExpired() bool
}

// CredentialConfig contains configuration for credential retrieval
type CredentialConfig struct {
	// Provider type (aws, gcp, azure, etc.)
	Provider string `json:"provider"`

	// Region for credential scope
	Region string `json:"region,omitempty"`

	// Role to assume (for role-based auth)
	Role string `json:"role,omitempty"`

	// Profile name for credential lookup
	Profile string `json:"profile,omitempty"`

	// Additional provider-specific settings
	Settings map[string]any `json:"settings,omitempty"`
}
