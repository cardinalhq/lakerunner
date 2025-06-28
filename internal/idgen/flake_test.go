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

package idgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSonyFlakeGenerator_NextID(t *testing.T) {
	gen, err := NewFlakeGenerator()
	require.NoError(t, err, "failed to create SonyFlakeGenerator")

	// Check that subsequent IDs are increasing
	id := gen.NextID()
	id2 := gen.NextID()
	assert.Greater(t, id2, id, "NextID() did not return increasing id")
}
