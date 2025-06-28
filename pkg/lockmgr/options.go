// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockmgr

import (
	"log/slog"
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

type loggerOption struct {
	ll *slog.Logger
}

func (l *loggerOption) apply(m *wqManager) {
	m.ll = l.ll
}

func WithLogger(ll *slog.Logger) Options {
	return &loggerOption{ll: ll}
}
