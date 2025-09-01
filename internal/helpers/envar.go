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

package helpers

import (
	"os"
	"strings"
)

// GetBoolEnv reads a boolean environment variable.
// Returns true if the environment variable is set to "true", "1", "yes", "on", "enable", or "enabled" (case insensitive).
// Returns the defaultValue if the environment variable is not set or empty.
// Returns false for "false", "0", "no", "off", "disable", or "disabled" (case insensitive).
func GetBoolEnv(envVar string, defaultValue bool) bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv(envVar)))

	switch env {
	case "true", "1", "yes", "on", "enable", "enabled":
		return true
	case "false", "0", "no", "off", "disable", "disabled":
		return false
	case "":
		return defaultValue
	default:
		// For any other value, default to true if it's non-empty
		// This provides backward compatibility for cases where any non-empty value was treated as true
		return true
	}
}
