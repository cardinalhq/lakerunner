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

package filereader

import "strings"

func prefixAttribute(name, prefix string) string {
	if name == "" {
		return ""
	}
	if name[0] == '_' {
		// Convert dots to underscores even for underscore-prefixed fields
		return strings.ReplaceAll(name, ".", "_")
	}
	// Use underscore separator for PromQL/LogQL compatibility
	return prefix + "_" + strings.ReplaceAll(name, ".", "_")
}
