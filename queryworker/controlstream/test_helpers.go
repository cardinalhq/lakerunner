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

package controlstream

import (
	"context"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
)

// NewTestSession creates a Session for use in tests outside this package.
func NewTestSession(id string, cancel context.CancelFunc) *Session {
	return &Session{
		ID:     id,
		sendCh: make(chan *workcoordpb.WorkerMessage, SendBufferSize),
		cancel: cancel,
	}
}

// SendCh returns the session's send channel for test message inspection.
func (s *Session) SendCh() <-chan *workcoordpb.WorkerMessage {
	return s.sendCh
}
