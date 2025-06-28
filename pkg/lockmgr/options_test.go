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
