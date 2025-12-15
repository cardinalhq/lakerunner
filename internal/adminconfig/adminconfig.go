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
	"os"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/configdb"
)

type AdminAPIKey struct {
	Name           string     `json:"name" yaml:"name"`
	Key            string     `json:"key" yaml:"key"`
	Description    string     `json:"description,omitempty" yaml:"description,omitempty"`
	OrganizationID *uuid.UUID `json:"organization_id,omitempty" yaml:"organization_id,omitempty"`
}

type AdminConfig struct {
	APIKeys []AdminAPIKey `json:"apikeys,omitempty" yaml:"apikeys,omitempty"`
}

type AdminConfigProvider interface {
	ValidateAPIKey(ctx context.Context, apiKey string) (bool, error)
	GetAPIKeyInfo(ctx context.Context, apiKey string) (*AdminAPIKey, error)
}

func SetupAdminConfig() (AdminConfigProvider, error) {
	ctx := context.Background()

	cdb, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return nil, err
	}

	initialAPIKey := os.Getenv("ADMIN_INITIAL_API_KEY")
	return NewDBProvider(cdb, initialAPIKey), nil
}
