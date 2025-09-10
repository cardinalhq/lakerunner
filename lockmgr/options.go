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

package lockmgr

import (
	"time"
)

type Options interface {
	apply(m *wqManager)
}

type heartbeatIntervalOption struct {
	d time.Duration
}

func (h *heartbeatIntervalOption) apply(m *wqManager) {
	if h.d <= 10*time.Second {
		h.d = 10 * time.Second
	}
	m.heartbeatInterval = h.d
}

// WithHeartbeatInterval sets the interval for heartbeating work items.
// Without this option, the default is 1 minute.
// If the interval is set to less than 10 seconds, it will be adjusted to 10 seconds.
func WithHeartbeatInterval(d time.Duration) Options {
	return &heartbeatIntervalOption{d: d}
}
