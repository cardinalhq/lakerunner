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
)

type AdminAPIKey struct {
	Name        string `json:"name" yaml:"name"`
	Key         string `json:"key" yaml:"key"`
	Description string `json:"description,omitempty" yaml:"description,omitempty"`
}

type AdminConfig struct {
	APIKeys []AdminAPIKey `json:"apikeys,omitempty" yaml:"apikeys,omitempty"`
}

type AdminConfigProvider interface {
	ValidateAPIKey(ctx context.Context, apiKey string) (bool, error)
	GetAPIKeyInfo(ctx context.Context, apiKey string) (*AdminAPIKey, error)
}

func SetupAdminConfig() (AdminConfigProvider, error) {
	adminConfigPath := os.Getenv("ADMIN_CONFIG_FILE")
	if adminConfigPath == "" {
		adminConfigPath = "/app/config/admin.yaml"
	}

	return NewFileProvider(adminConfigPath)
}
