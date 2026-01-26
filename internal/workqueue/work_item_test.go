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

package workqueue

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestWorkItem_Accessors(t *testing.T) {
	testOrgID := uuid.New()
	testSpec := json.RawMessage(`{"key1": "value1", "key2": 123, "key3": true}`)

	wi := &WorkItem{
		id:             100,
		taskName:       "test-task",
		organizationID: testOrgID,
		instanceNum:    5,
		spec:           testSpec,
		tries:          3,
	}

	assert.Equal(t, int64(100), wi.ID())
	assert.Equal(t, "test-task", wi.TaskName())
	assert.Equal(t, testOrgID, wi.OrganizationID())
	assert.Equal(t, int16(5), wi.InstanceNum())
	assert.Equal(t, testSpec, wi.Spec())
	assert.Equal(t, int32(3), wi.Tries())
}

func TestWorkItem_Complete_WithoutManager(t *testing.T) {
	wi := &WorkItem{
		id:       100,
		taskName: "test-task",
		mgr:      nil,
	}

	err := wi.Complete()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manager is nil")
}

func TestWorkItem_Fail_WithoutManager(t *testing.T) {
	wi := &WorkItem{
		id:       100,
		taskName: "test-task",
		mgr:      nil,
	}

	err := wi.Fail(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "manager is nil")
}

func TestWorkItem_Complete_AlreadyClosed(t *testing.T) {
	wi := &WorkItem{
		id:       100,
		taskName: "test-task",
		closed:   true,
		mgr:      &Manager{},
	}

	err := wi.Complete()
	assert.NoError(t, err, "Completing already closed item should be idempotent")
}

func TestWorkItem_Fail_AlreadyClosed(t *testing.T) {
	wi := &WorkItem{
		id:       100,
		taskName: "test-task",
		closed:   true,
		mgr:      &Manager{},
	}

	err := wi.Fail(nil)
	assert.NoError(t, err, "Failing already closed item should be idempotent")
}

func TestWorkItem_Spec_ReturnsRawBytes(t *testing.T) {
	testSpec := json.RawMessage(`{"key1": "value1"}`)

	wi := &WorkItem{
		id:       100,
		taskName: "test-task",
		spec:     testSpec,
	}

	// Verify the spec returns the same raw bytes
	spec := wi.Spec()
	assert.Equal(t, testSpec, spec)
}

func TestWorkItem_Workable_Interface(t *testing.T) {
	var _ Workable = (*WorkItem)(nil)
}
