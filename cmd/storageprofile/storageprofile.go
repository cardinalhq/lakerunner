// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageprofile

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/google/uuid"
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
