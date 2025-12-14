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

package configservice

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"
)

// LogStreamConfig holds configuration for log stream identification.
// Stored in DB as: {"field_name": "resource_service_name"}
//
// The stream field is always indexed with TrigramFull (IndexTrigramExact)
// to support both exact matching and substring/regex queries.
// This assumption is hardcoded and does not need to be stored in config.
type LogStreamConfig struct {
	FieldName string `json:"field_name"`
}

// defaultLogStreamConfig returns the hardcoded fallback config.
func defaultLogStreamConfig() LogStreamConfig {
	return LogStreamConfig{
		FieldName: "resource_service_name",
	}
}

// GetLogStreamConfig returns the log stream config for an organization.
// It checks the org-specific config first, then falls back to the system default,
// and finally to a hardcoded default if neither exists.
func (s *Service) GetLogStreamConfig(ctx context.Context, orgID uuid.UUID) LogStreamConfig {
	// Try org-specific config
	val, err := s.getConfigCached(ctx, orgID, configKeyLogStream)
	if err == nil && len(val) > 0 {
		var config LogStreamConfig
		if json.Unmarshal(val, &config) == nil && config.FieldName != "" {
			return config
		}
	}

	// Try system default (if not already checking default)
	if orgID != DefaultOrgID {
		val, err = s.getConfigCached(ctx, DefaultOrgID, configKeyLogStream)
		if err == nil && len(val) > 0 {
			var config LogStreamConfig
			if json.Unmarshal(val, &config) == nil && config.FieldName != "" {
				return config
			}
		}
	}

	// Hardcoded fallback
	return defaultLogStreamConfig()
}

// SetLogStreamConfig sets the log stream field for an organization.
func (s *Service) SetLogStreamConfig(ctx context.Context, orgID uuid.UUID, fieldName string) error {
	config := LogStreamConfig{FieldName: fieldName}
	val, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return s.setConfig(ctx, orgID, configKeyLogStream, val)
}
