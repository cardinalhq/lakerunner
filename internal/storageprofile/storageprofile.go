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
	"errors"
	"log/slog"
	"os"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
)

type StorageProfile struct {
	OrganizationID uuid.UUID `json:"organization_id" yaml:"organization_id"`
	InstanceNum    int16     `json:"instance_num" yaml:"instance_num"`
	CollectorName  string    `json:"collector_name" yaml:"collector_name"`
	CloudProvider  string    `json:"cloud_provider" yaml:"cloud_provider"`
	Region         string    `json:"region" yaml:"region"`
	Role           string    `json:"role,omitempty" yaml:"role,omitempty"`
	Bucket         string    `json:"bucket" yaml:"bucket"`
	Hosted         bool      `json:"hosted,omitempty" yaml:"hosted,omitempty"`
	Endpoint       string    `json:"endpoint,omitempty" yaml:"endpoint,omitempty"`
	InsecureTLS    bool      `json:"insecure_tls,omitempty" yaml:"insecure_tls,omitempty"`
	UsePathStyle   bool      `json:"use_path_style,omitempty" yaml:"use_path_style,omitempty"`
	UseSSL         bool      `json:"use_ssl,omitempty" yaml:"use_ssl,omitempty"`
}

type StorageProfileProvider interface {
	Get(ctx context.Context, organizationID uuid.UUID, instanceNum int16) (StorageProfile, error)
	GetStorageProfilesByBucketName(ctx context.Context, bucketName string) ([]StorageProfile, error)
	GetByCollectorName(ctx context.Context, organizationID uuid.UUID, collectorName string) (StorageProfile, error)
}

func SetupStorageProfiles() (StorageProfileProvider, error) {
	cdb, err := dbopen.ConfigDBStore(context.Background())
	if err == nil {
		sp := NewDatabaseProvider(cdb)
		slog.Info("Using database storage profile provider")
		return sp, nil
	}
	if !errors.Is(err, dbopen.ErrDatabaseNotConfigured) {
		return nil, err
	}

	slog.Info("Database storage profile provider not configured, falling back to file provider", "error", err)

	storagePath := os.Getenv("STORAGE_PROFILE_FILE")
	if storagePath == "" {
		storagePath = "/app/config/storage_profiles.yaml"
	}
	slog.Info("Using file storage profile provider", "path", storagePath)
	return NewFileProvider(storagePath)
}
