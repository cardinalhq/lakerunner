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

package storageprofile

import (
	"context"

	"github.com/google/uuid"
)

type StorageProfile struct {
	OrganizationID uuid.UUID `json:"organization_id"`
	CloudProvider  string    `json:"cloud_provider"`
	Region         string    `json:"region"`
	Role           string    `json:"role,omitempty"`
	Bucket         string    `json:"bucket"`
	Endpoint       string    `json:"endpoint,omitempty"`
	InsecureTLS    bool      `json:"insecure_tls,omitempty"`
	UsePathStyle   bool      `json:"use_path_style,omitempty"`
	UseSSL         bool      `json:"use_ssl,omitempty"`
}

type StorageProfileProvider interface {
	GetStorageProfileForBucket(ctx context.Context, organizationID uuid.UUID, bucketName string) (StorageProfile, error)
	GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error)
	GetStorageProfileForOrganization(ctx context.Context, organizationID uuid.UUID) (StorageProfile, error)
	ResolveOrganization(ctx context.Context, bucketName, objectPath string) (uuid.UUID, error)
}

func NewStorageProfileProvider(cdb ConfigDBStoreageProfileFetcher) StorageProfileProvider {
	return NewDatabaseProvider(cdb)
}
