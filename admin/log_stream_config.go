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
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/configdb"
)

const (
	// configKeyLogStream matches the key used in configservice
	configKeyLogStream = "log.stream"

	// defaultLogStreamField is the hardcoded fallback
	defaultLogStreamField = "resource_service_name"
)

// logStreamConfigJSON is the JSON structure stored in the database
type logStreamConfigJSON struct {
	FieldName string `json:"field_name"`
}

// GetLogStreamConfig gets the log stream configuration for an organization.
// If no org-specific config exists, returns the system default or hardcoded fallback.
func (s *Service) GetLogStreamConfig(ctx context.Context, req *adminproto.GetLogStreamConfigRequest) (*adminproto.GetLogStreamConfigResponse, error) {
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

	// Try org-specific config first
	val, err := store.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyLogStream,
	})
	if err == nil && len(val) > 0 {
		var config logStreamConfigJSON
		if json.Unmarshal(val, &config) == nil && config.FieldName != "" {
			return &adminproto.GetLogStreamConfigResponse{
				Config: &adminproto.LogStreamConfig{
					OrganizationId: req.OrganizationId,
					FieldName:      config.FieldName,
				},
				IsDefault: false,
			}, nil
		}
	} else if err != nil && err != pgx.ErrNoRows {
		slog.Error("Failed to get org config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to get config")
	}

	// Try system default (nil UUID)
	nilUUID := uuid.UUID{}
	val, err = store.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
		OrganizationID: nilUUID,
		Key:            configKeyLogStream,
	})
	if err == nil && len(val) > 0 {
		var config logStreamConfigJSON
		if json.Unmarshal(val, &config) == nil && config.FieldName != "" {
			return &adminproto.GetLogStreamConfigResponse{
				Config: &adminproto.LogStreamConfig{
					OrganizationId: req.OrganizationId,
					FieldName:      config.FieldName,
				},
				IsDefault: true,
			}, nil
		}
	} else if err != nil && err != pgx.ErrNoRows {
		slog.Error("Failed to get default config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to get default config")
	}

	// Return hardcoded fallback
	return &adminproto.GetLogStreamConfigResponse{
		Config: &adminproto.LogStreamConfig{
			OrganizationId: req.OrganizationId,
			FieldName:      defaultLogStreamField,
		},
		IsDefault: true,
	}, nil
}

// SetLogStreamConfig sets or updates the log stream configuration for an organization.
func (s *Service) SetLogStreamConfig(ctx context.Context, req *adminproto.SetLogStreamConfigRequest) (*adminproto.SetLogStreamConfigResponse, error) {
	if req.OrganizationId == "" {
		return nil, status.Error(codes.InvalidArgument, "organization_id is required")
	}
	if req.FieldName == "" {
		return nil, status.Error(codes.InvalidArgument, "field_name is required")
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

	config := logStreamConfigJSON{FieldName: req.FieldName}
	val, err := json.Marshal(config)
	if err != nil {
		slog.Error("Failed to marshal config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to marshal config")
	}

	err = store.UpsertOrgConfig(ctx, configdb.UpsertOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyLogStream,
		Value:          val,
	})
	if err != nil {
		slog.Error("Failed to set config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to set config")
	}

	return &adminproto.SetLogStreamConfigResponse{
		Config: &adminproto.LogStreamConfig{
			OrganizationId: req.OrganizationId,
			FieldName:      req.FieldName,
		},
	}, nil
}

// DeleteLogStreamConfig deletes the org-specific log stream configuration.
// After deletion, the org will use the system default.
func (s *Service) DeleteLogStreamConfig(ctx context.Context, req *adminproto.DeleteLogStreamConfigRequest) (*adminproto.DeleteLogStreamConfigResponse, error) {
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

	err = store.DeleteOrgConfig(ctx, configdb.DeleteOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyLogStream,
	})
	if err != nil {
		slog.Error("Failed to delete config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to delete config")
	}

	return &adminproto.DeleteLogStreamConfigResponse{}, nil
}
