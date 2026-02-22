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

package workmanager

import (
	"encoding/json"
	"fmt"
)

// DispatchLeafWork is a bridge between query planner output and DispatchWork.
// It serializes the request to JSON and dispatches it as a work item.
func (m *Manager) DispatchLeafWork(queryID, leafID, affinityKey string, req any) (string, error) {
	spec, err := json.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("marshal spec: %w", err)
	}
	return m.DispatchWork(queryID, leafID, affinityKey, spec)
}
