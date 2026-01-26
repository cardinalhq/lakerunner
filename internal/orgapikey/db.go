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

package orgapikey

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/configdb"
)

type dbProvider struct {
	db configdb.Querier
}

var _ OrganizationAPIKeyProvider = (*dbProvider)(nil)

func NewDBProvider(db configdb.Querier) OrganizationAPIKeyProvider {
	return &dbProvider{
		db: db,
	}
}

func (p *dbProvider) ValidateAPIKey(ctx context.Context, apiKey string) (*uuid.UUID, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("empty API key")
	}

	keyHash := hashAPIKey(apiKey)

	row, err := p.db.GetOrganizationAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return nil, fmt.Errorf("organization API key not found")
	}

	return &row.OrganizationID, nil
}

func (p *dbProvider) GetAPIKeyInfo(ctx context.Context, apiKey string) (*OrganizationAPIKey, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("empty API key")
	}

	keyHash := hashAPIKey(apiKey)

	row, err := p.db.GetOrganizationAPIKeyByHash(ctx, keyHash)
	if err != nil {
		return nil, fmt.Errorf("organization API key not found")
	}

	return &OrganizationAPIKey{
		Name:           row.Name,
		Description:    safeStringDeref(row.Description),
		OrganizationID: row.OrganizationID,
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
