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

package admin

import (
	"context"
	"log/slog"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/cardinalhq/lakerunner/internal/adminconfig"
)

type authInterceptor struct {
	configProvider adminconfig.AdminConfigProvider
}

func newAuthInterceptor(configProvider adminconfig.AdminConfigProvider) *authInterceptor {
	return &authInterceptor{
		configProvider: configProvider,
	}
}

func (a *authInterceptor) unaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	if err := a.authenticate(ctx, info.FullMethod); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (a *authInterceptor) authenticate(ctx context.Context, method string) error {
	// Extract metadata from context
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		// Check if config allows no authentication (backward compatibility)
		valid, err := a.configProvider.ValidateAPIKey(ctx, "")
		if err == nil && valid {
			return nil
		}
		slog.Warn("Admin API request without metadata", slog.String("method", method))
		return status.Error(codes.Unauthenticated, "missing metadata")
	}

	// Get authorization header
	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		// Check if config allows no authentication (backward compatibility)
		valid, err := a.configProvider.ValidateAPIKey(ctx, "")
		if err == nil && valid {
			return nil
		}
		slog.Warn("Admin API request without authorization header", slog.String("method", method))
		return status.Error(codes.Unauthenticated, "missing authorization header")
	}

	authHeader := authHeaders[0]

	// Extract API key from Bearer token
	apiKey, err := extractAPIKey(authHeader)
	if err != nil {
		slog.Warn("Admin API request with invalid authorization format",
			slog.String("method", method),
			slog.String("error", err.Error()))
		return status.Error(codes.Unauthenticated, "invalid authorization format")
	}

	// Validate API key
	valid, err := a.configProvider.ValidateAPIKey(ctx, apiKey)
	if err != nil {
		slog.Error("Admin API key validation error",
			slog.String("method", method),
			slog.String("error", err.Error()))
		return status.Error(codes.Internal, "authentication error")
	}

	if !valid {
		slog.Warn("Admin API request with invalid API key", slog.String("method", method))
		return status.Error(codes.Unauthenticated, "invalid API key")
	}

	// Log successful authentication (without the key for security)
	if keyInfo, err := a.configProvider.GetAPIKeyInfo(ctx, apiKey); err == nil {
		slog.Debug("Admin API request authenticated",
			slog.String("method", method),
			slog.String("key_name", keyInfo.Name))
	}

	return nil
}

func extractAPIKey(authHeader string) (string, error) {
	const bearerPrefix = "Bearer "

	if !strings.HasPrefix(authHeader, bearerPrefix) {
		return "", status.Error(codes.Unauthenticated, "authorization header must start with 'Bearer '")
	}

	apiKey := strings.TrimPrefix(authHeader, bearerPrefix)
	if apiKey == "" {
		return "", status.Error(codes.Unauthenticated, "empty API key")
	}

	return apiKey, nil
}
