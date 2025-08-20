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

package adminconfig

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/cardinalhq/lakerunner/configdb"
)

type dbProvider struct {
	db configdb.Querier
}

var _ AdminConfigProvider = (*dbProvider)(nil)

func NewDBProvider(db configdb.Querier) AdminConfigProvider {
	return &dbProvider{
		db: db,
	}
}

func (p *dbProvider) ValidateAPIKey(ctx context.Context, apiKey string) (bool, error) {
	if apiKey == "" {
		return false, nil
	}

	keyHash := hashAPIKey(apiKey)

	_, err := p.db.GetAdminAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return false, nil
	}

	return true, nil
}

func (p *dbProvider) GetAPIKeyInfo(ctx context.Context, apiKey string) (*AdminAPIKey, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("empty API key")
	}

	keyHash := hashAPIKey(apiKey)

	row, err := p.db.GetAdminAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return nil, fmt.Errorf("API key not found")
	}

	return &AdminAPIKey{
		Name:           row.Name,
		Description:    safeStringDeref(row.Description),
		OrganizationID: nil, // Admin API keys don't belong to organizations
	}, nil
}

func hashAPIKey(apiKey string) string {
	h := sha256.Sum256([]byte(apiKey))
	return fmt.Sprintf("%x", h)
}

func safeStringDeref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// Admin API keys don't belong to organizations, so this method is not needed
// Use organization API key provider for organization-scoped API keys
