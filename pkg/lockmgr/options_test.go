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
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWithHeartbeatInterval_Default(t *testing.T) {
	opt := WithHeartbeatInterval(30 * time.Second)
	m := &wqManager{}
	assert.NotEqual(t, 30*time.Second, m.heartbeatInterval, "expected heartbeatInterval to be 30 seconds before applying option")
	opt.apply(m)
	assert.Equal(t, 30*time.Second, m.heartbeatInterval, "expected heartbeatInterval to be 30 seconds")
}

func TestWithHeartbeatInterval_LessThanMinimum(t *testing.T) {
	opt := WithHeartbeatInterval(5 * time.Second)
	m := &wqManager{}
	assert.NotEqual(t, 10*time.Second, m.heartbeatInterval, "expected heartbeatInterval to be 10 seconds before applying option")
	opt.apply(m)
	assert.Equal(t, 10*time.Second, m.heartbeatInterval, "expected heartbeatInterval to be adjusted to 10 seconds")
}

func TestWithLogger(t *testing.T) {
	ll := slog.Default()
	opt := WithLogger(ll)
	m := &wqManager{}
	assert.NotEqual(t, ll, m.ll, "expected logger to be different before applying option")
	opt.apply(m)
	assert.Equal(t, ll, m.ll, "expected logger to be set correctly")
}
