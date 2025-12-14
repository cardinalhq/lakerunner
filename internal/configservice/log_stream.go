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

	"github.com/cardinalhq/lakerunner/configdb"
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

// parseLogStreamConfig attempts to parse a raw JSON value into LogStreamConfig.
// Returns the config and true if successful, or zero value and false if parsing
// fails or the field name is empty.
func parseLogStreamConfig(val json.RawMessage) (LogStreamConfig, bool) {
	if len(val) == 0 {
		return LogStreamConfig{}, false
	}
	var config LogStreamConfig
	if json.Unmarshal(val, &config) != nil || config.FieldName == "" {
		return LogStreamConfig{}, false
	}
	return config, true
}

// marshalLogStreamConfig marshals a field name into the JSON format for storage.
func marshalLogStreamConfig(fieldName string) (json.RawMessage, error) {
	return json.Marshal(LogStreamConfig{FieldName: fieldName})
}

// GetLogStreamConfig returns the log stream config for an organization.
// It checks the org-specific config first, then falls back to the system default,
// and finally to a hardcoded default if neither exists.
func (s *Service) GetLogStreamConfig(ctx context.Context, orgID uuid.UUID) LogStreamConfig {
	// Try org-specific config
	val, _ := s.getConfigCached(ctx, orgID, configKeyLogStream)
	if config, ok := parseLogStreamConfig(val); ok {
		return config
	}

	// Try system default (if not already checking default)
	if orgID != DefaultOrgID {
		val, _ = s.getConfigCached(ctx, DefaultOrgID, configKeyLogStream)
		if config, ok := parseLogStreamConfig(val); ok {
			return config
		}
	}

	// Hardcoded fallback
	return defaultLogStreamConfig()
}

// GetLogStreamConfigResult contains the result of a direct config lookup.
type GetLogStreamConfigResult struct {
	Config    LogStreamConfig
	IsDefault bool // True if the config comes from system default or hardcoded fallback
}

// GetLogStreamConfigDirect fetches the log stream config directly from the database,
// bypassing any cache. This is intended for admin interfaces that need current values.
func GetLogStreamConfigDirect(ctx context.Context, querier OrgConfigQuerier, orgID uuid.UUID) (GetLogStreamConfigResult, error) {
	// Try org-specific config
	val, _ := querier.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyLogStream,
	})
	if config, ok := parseLogStreamConfig(val); ok {
		return GetLogStreamConfigResult{Config: config, IsDefault: false}, nil
	}

	// Try system default
	val, _ = querier.GetOrgConfig(ctx, configdb.GetOrgConfigParams{
		OrganizationID: DefaultOrgID,
		Key:            configKeyLogStream,
	})
	if config, ok := parseLogStreamConfig(val); ok {
		return GetLogStreamConfigResult{Config: config, IsDefault: true}, nil
	}

	// Hardcoded fallback
	return GetLogStreamConfigResult{Config: defaultLogStreamConfig(), IsDefault: true}, nil
}

// SetLogStreamConfigDirect sets the log stream config directly in the database,
// bypassing any cache. This is intended for admin interfaces.
func SetLogStreamConfigDirect(ctx context.Context, querier OrgConfigQuerier, orgID uuid.UUID, fieldName string) error {
	val, err := marshalLogStreamConfig(fieldName)
	if err != nil {
		return err
	}
	return querier.UpsertOrgConfig(ctx, configdb.UpsertOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyLogStream,
		Value:          val,
	})
}

// DeleteLogStreamConfigDirect deletes the log stream config directly from the database,
// bypassing any cache. This is intended for admin interfaces.
func DeleteLogStreamConfigDirect(ctx context.Context, querier OrgConfigQuerier, orgID uuid.UUID) error {
	return querier.DeleteOrgConfig(ctx, configdb.DeleteOrgConfigParams{
		OrganizationID: orgID,
		Key:            configKeyLogStream,
	})
}
