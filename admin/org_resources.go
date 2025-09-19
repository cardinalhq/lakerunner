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

package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/configdb"
)

// generateAPIKey generates a new random API key
func generateAPIKey() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("failed to generate random key: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// hashAPIKey creates a SHA256 hash of the API key
func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}

// ListOrganizationAPIKeys lists all API keys for an organization
func (s *Service) ListOrganizationAPIKeys(ctx context.Context, req *adminproto.ListOrganizationAPIKeysRequest) (*adminproto.ListOrganizationAPIKeysResponse, error) {
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}

	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid organization_id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	// Use the generated query
	apiKeys, err := store.ListOrganizationAPIKeysByOrg(ctx, orgID)
	if err != nil {
		slog.Error("Failed to list API keys", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to list API keys")
	}

	var protoAPIKeys []*adminproto.OrganizationAPIKey
	for _, apiKey := range apiKeys {
		var desc string
		if apiKey.Description != nil {
			desc = *apiKey.Description
		}
		protoAPIKeys = append(protoAPIKeys, &adminproto.OrganizationAPIKey{
			Id:             apiKey.ID.String(),
			OrganizationId: req.OrganizationId,
			Name:           apiKey.Name,
			Description:    desc,
			KeyPreview:     apiKey.KeyHash[:8], // Show first 8 chars of hash as preview
		})
	}

	return &adminproto.ListOrganizationAPIKeysResponse{ApiKeys: protoAPIKeys}, nil
}

// CreateOrganizationAPIKey creates a new API key for an organization
func (s *Service) CreateOrganizationAPIKey(ctx context.Context, req *adminproto.CreateOrganizationAPIKeyRequest) (*adminproto.CreateOrganizationAPIKeyResponse, error) {
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name is required")
	}

	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid organization_id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	// Generate new API key
	apiKey, err := generateAPIKey()
	if err != nil {
		slog.Error("Failed to generate API key", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to generate API key")
	}

	keyHash := hashAPIKey(apiKey)

	var desc *string
	if req.Description != "" {
		desc = &req.Description
	}

	// Use the transaction method from the Store
	apiKeyRow, err := store.CreateOrganizationAPIKeyWithMapping(ctx, configdb.CreateOrganizationAPIKeyParams{
		KeyHash:     keyHash,
		Name:        req.Name,
		Description: desc,
	}, orgID)
	if err != nil {
		slog.Error("Failed to create API key", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to create API key")
	}

	return &adminproto.CreateOrganizationAPIKeyResponse{
		ApiKey: &adminproto.OrganizationAPIKey{
			Id:             apiKeyRow.ID.String(),
			OrganizationId: req.OrganizationId,
			Name:           req.Name,
			Description:    req.Description,
			KeyPreview:     keyHash[:8],
		},
		FullKey: apiKey,
	}, nil
}

// DeleteOrganizationAPIKey deletes an API key
func (s *Service) DeleteOrganizationAPIKey(ctx context.Context, req *adminproto.DeleteOrganizationAPIKeyRequest) (*adminproto.DeleteOrganizationAPIKeyResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	keyID, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	// Use the transaction method from the Store
	err = store.DeleteOrganizationAPIKeyWithMappings(ctx, keyID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "API key not found")
		}
		slog.Error("Failed to delete API key", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to delete API key")
	}

	return &adminproto.DeleteOrganizationAPIKeyResponse{}, nil
}

// ListOrganizationBuckets lists all buckets for an organization
func (s *Service) ListOrganizationBuckets(ctx context.Context, req *adminproto.ListOrganizationBucketsRequest) (*adminproto.ListOrganizationBucketsResponse, error) {
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}

	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid organization_id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	buckets, err := store.ListOrganizationBucketsByOrg(ctx, orgID)
	if err != nil {
		slog.Error("Failed to list organization buckets", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to list organization buckets")
	}

	var protoBuckets []*adminproto.OrganizationBucket
	for _, bucket := range buckets {
		protoBuckets = append(protoBuckets, &adminproto.OrganizationBucket{
			OrganizationId: bucket.OrganizationID.String(),
			BucketName:     bucket.BucketName,
			InstanceNum:    int32(bucket.InstanceNum),
			CollectorName:  bucket.CollectorName,
		})
	}

	return &adminproto.ListOrganizationBucketsResponse{Buckets: protoBuckets}, nil
}

// AddOrganizationBucket adds a bucket to an organization
func (s *Service) AddOrganizationBucket(ctx context.Context, req *adminproto.AddOrganizationBucketRequest) (*adminproto.AddOrganizationBucketResponse, error) {
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	if req.BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket_name is required")
	}

	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid organization_id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	// Use the transaction method from the Store
	err = store.AddOrganizationBucket(ctx, orgID, req.BucketName, int16(req.InstanceNum), req.CollectorName)
	if err != nil {
		slog.Error("Failed to add organization bucket", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to add organization bucket")
	}

	return &adminproto.AddOrganizationBucketResponse{
		Bucket: &adminproto.OrganizationBucket{
			OrganizationId: req.OrganizationId,
			BucketName:     req.BucketName,
			InstanceNum:    req.InstanceNum,
			CollectorName:  req.CollectorName,
		},
	}, nil
}

// DeleteOrganizationBucket removes a bucket from an organization
func (s *Service) DeleteOrganizationBucket(ctx context.Context, req *adminproto.DeleteOrganizationBucketRequest) (*adminproto.DeleteOrganizationBucketResponse, error) {
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	if req.BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket_name is required")
	}

	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid organization_id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	err = store.DeleteOrganizationBucket(ctx, configdb.DeleteOrganizationBucketParams{
		OrganizationID: orgID,
		BucketName:     req.BucketName,
		InstanceNum:    int16(req.InstanceNum),
		CollectorName:  req.CollectorName,
	})
	if err != nil {
		slog.Error("Failed to remove organization bucket", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to remove organization bucket")
	}

	return &adminproto.DeleteOrganizationBucketResponse{}, nil
}

// ListBucketConfigurations lists all bucket configurations
func (s *Service) ListBucketConfigurations(ctx context.Context, req *adminproto.ListBucketConfigurationsRequest) (*adminproto.ListBucketConfigurationsResponse, error) {
	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	configs, err := store.ListBucketConfigurations(ctx)
	if err != nil {
		slog.Error("Failed to list bucket configurations", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to list bucket configurations")
	}

	var protoConfigs []*adminproto.BucketConfiguration
	for _, cfg := range configs {
		pc := &adminproto.BucketConfiguration{
			BucketName:   cfg.BucketName,
			UsePathStyle: cfg.UsePathStyle,
			InsecureTls:  cfg.InsecureTls,
		}
		if cfg.CloudProvider != "" {
			pc.CloudProvider = cfg.CloudProvider
		}
		if cfg.Region != "" {
			pc.Region = cfg.Region
		}
		if cfg.Endpoint != nil && *cfg.Endpoint != "" {
			pc.Endpoint = *cfg.Endpoint
		}
		if cfg.Role != nil && *cfg.Role != "" {
			pc.Role = *cfg.Role
		}
		protoConfigs = append(protoConfigs, pc)
	}

	return &adminproto.ListBucketConfigurationsResponse{Configurations: protoConfigs}, nil
}

// CreateBucketConfiguration creates a new bucket configuration
func (s *Service) CreateBucketConfiguration(ctx context.Context, req *adminproto.CreateBucketConfigurationRequest) (*adminproto.CreateBucketConfigurationResponse, error) {
	if req.BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket_name is required")
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	var endpoint, role *string
	if req.Endpoint != "" {
		endpoint = &req.Endpoint
	}
	if req.Role != "" {
		role = &req.Role
	}

	cfg, err := store.CreateBucketConfiguration(ctx, configdb.CreateBucketConfigurationParams{
		BucketName:    req.BucketName,
		CloudProvider: req.CloudProvider,
		Region:        req.Region,
		Endpoint:      endpoint,
		Role:          role,
		UsePathStyle:  req.UsePathStyle,
		InsecureTls:   req.InsecureTls,
	})
	if err != nil {
		slog.Error("Failed to create bucket configuration", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to create bucket configuration")
	}

	pc := &adminproto.BucketConfiguration{
		BucketName:    cfg.BucketName,
		CloudProvider: cfg.CloudProvider,
		Region:        cfg.Region,
		UsePathStyle:  cfg.UsePathStyle,
		InsecureTls:   cfg.InsecureTls,
	}
	if cfg.Endpoint != nil && *cfg.Endpoint != "" {
		pc.Endpoint = *cfg.Endpoint
	}
	if cfg.Role != nil && *cfg.Role != "" {
		pc.Role = *cfg.Role
	}

	return &adminproto.CreateBucketConfigurationResponse{Configuration: pc}, nil
}

// DeleteBucketConfiguration deletes a bucket configuration
func (s *Service) DeleteBucketConfiguration(ctx context.Context, req *adminproto.DeleteBucketConfigurationRequest) (*adminproto.DeleteBucketConfigurationResponse, error) {
	if req.BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket_name is required")
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	err = store.DeleteBucketConfiguration(ctx, req.BucketName)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "bucket configuration not found")
		}
		slog.Error("Failed to delete bucket configuration", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to delete bucket configuration")
	}

	return &adminproto.DeleteBucketConfigurationResponse{}, nil
}

// ListBucketPrefixMappings lists bucket prefix mappings
func (s *Service) ListBucketPrefixMappings(ctx context.Context, req *adminproto.ListBucketPrefixMappingsRequest) (*adminproto.ListBucketPrefixMappingsResponse, error) {
	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	mappings, err := store.ListBucketPrefixMappings(ctx)
	if err != nil {
		slog.Error("Failed to list bucket prefix mappings", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to list bucket prefix mappings")
	}

	var protoMappings []*adminproto.BucketPrefixMapping
	for _, m := range mappings {
		// Filter by bucket name if specified
		if req.BucketName != "" && m.BucketName != req.BucketName {
			continue
		}
		// Filter by organization ID if specified
		if req.OrganizationId != "" && m.OrganizationID.String() != req.OrganizationId {
			continue
		}

		protoMappings = append(protoMappings, &adminproto.BucketPrefixMapping{
			Id:             m.ID.String(),
			BucketName:     m.BucketName,
			OrganizationId: m.OrganizationID.String(),
			PathPrefix:     m.PathPrefix,
			Signal:         m.Signal,
		})
	}

	return &adminproto.ListBucketPrefixMappingsResponse{Mappings: protoMappings}, nil
}

// CreateBucketPrefixMapping creates a new bucket prefix mapping
func (s *Service) CreateBucketPrefixMapping(ctx context.Context, req *adminproto.CreateBucketPrefixMappingRequest) (*adminproto.CreateBucketPrefixMappingResponse, error) {
	if req.BucketName == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket_name is required")
	}
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	if req.PathPrefix == "" {
		return nil, status.Error(codes.InvalidArgument, "path_prefix is required")
	}
	if req.Signal == "" {
		return nil, status.Error(codes.InvalidArgument, "signal is required")
	}

	orgID, err := uuid.Parse(req.OrganizationId)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid organization_id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	// Use the transaction method from the Store
	mapping, err := store.CreateBucketPrefixMappingForOrg(ctx, req.BucketName, orgID, req.PathPrefix, req.Signal)
	if err != nil {
		slog.Error("Failed to create bucket prefix mapping", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to create bucket prefix mapping")
	}

	return &adminproto.CreateBucketPrefixMappingResponse{
		Mapping: &adminproto.BucketPrefixMapping{
			Id:             mapping.ID.String(),
			BucketName:     req.BucketName,
			OrganizationId: req.OrganizationId,
			PathPrefix:     req.PathPrefix,
			Signal:         req.Signal,
		},
	}, nil
}

// DeleteBucketPrefixMapping deletes a bucket prefix mapping
func (s *Service) DeleteBucketPrefixMapping(ctx context.Context, req *adminproto.DeleteBucketPrefixMappingRequest) (*adminproto.DeleteBucketPrefixMappingResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	mappingID, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid id: %v", err)
	}

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to config database", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to connect to database")
	}

	err = store.DeleteBucketPrefixMapping(ctx, mappingID)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "bucket prefix mapping not found")
		}
		slog.Error("Failed to delete bucket prefix mapping", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to delete bucket prefix mapping")
	}

	return &adminproto.DeleteBucketPrefixMappingResponse{}, nil
}
