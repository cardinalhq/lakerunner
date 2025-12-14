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
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/configservice"
)

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

	result, err := configservice.GetLogStreamConfigDirect(ctx, store, orgID)
	if err != nil {
		slog.Error("Failed to get log stream config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to get config")
	}

	return &adminproto.GetLogStreamConfigResponse{
		Config: &adminproto.LogStreamConfig{
			OrganizationId: req.OrganizationId,
			FieldName:      result.Config.FieldName,
		},
		IsDefault: result.IsDefault,
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

	if err := configservice.SetLogStreamConfigDirect(ctx, store, orgID, req.FieldName); err != nil {
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

	if err := configservice.DeleteLogStreamConfigDirect(ctx, store, orgID); err != nil {
		slog.Error("Failed to delete config", slog.Any("error", err))
		return nil, status.Error(codes.Internal, "failed to delete config")
	}

	return &adminproto.DeleteLogStreamConfigResponse{}, nil
}
