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

package accumulation

import (
	"testing"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestValidateGroupConsistency_ValidGroup(t *testing.T) {
	orgID := uuid.New()
	key := CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      101, // Different segment ID is OK
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.NoError(t, err)
}

func TestValidateGroupConsistency_EmptyGroup(t *testing.T) {
	group := &AccumulationGroup[CompactionKey]{
		Key:      CompactionKey{},
		Messages: []*AccumulatedMessage{},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "message_count", validationErr.Field)
	assert.Contains(t, validationErr.Message, "group cannot be empty")
}

func TestValidateGroupConsistency_InconsistentOrganizationID(t *testing.T) {
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	key := CompactionKey{
		OrganizationID: orgID1,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID1,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID2, // Different org ID
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "organization_id", validationErr.Field)
	assert.Equal(t, orgID1, validationErr.Expected)
	assert.Equal(t, orgID2, validationErr.Got)
	assert.Contains(t, validationErr.Message, "message 1")
}

func TestValidateGroupConsistency_InconsistentInstanceNum(t *testing.T) {
	orgID := uuid.New()

	key := CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    2, // Different instance
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "instance_num", validationErr.Field)
	assert.Equal(t, int16(1), validationErr.Expected)
	assert.Equal(t, int16(2), validationErr.Got)
}

func TestValidateGroupConsistency_InconsistentDateInt(t *testing.T) {
	orgID := uuid.New()

	key := CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250107, // Different date
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "date_int", validationErr.Field)
	assert.Equal(t, int32(20250108), validationErr.Expected)
	assert.Equal(t, int32(20250107), validationErr.Got)
}

func TestValidateGroupConsistency_InconsistentFrequencyMs(t *testing.T) {
	orgID := uuid.New()

	key := CompactionKey{
		OrganizationID: orgID,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID,
					DateInt:        20250108,
					FrequencyMs:    30000, // Different frequency
					InstanceNum:    1,
					SegmentID:      101,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "frequency_ms", validationErr.Field)
	assert.Equal(t, int32(60000), validationErr.Expected)
	assert.Equal(t, int32(30000), validationErr.Got)
}

func TestValidateGroupConsistency_FirstMessageInconsistent(t *testing.T) {
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	key := CompactionKey{
		OrganizationID: orgID1,
		DateInt:        20250108,
		FrequencyMs:    60000,
		InstanceNum:    1,
	}

	group := &AccumulationGroup[CompactionKey]{
		Key: key,
		Messages: []*AccumulatedMessage{
			{
				Message: &messages.MetricSegmentNotificationMessage{
					OrganizationID: orgID2, // First message is inconsistent
					DateInt:        20250108,
					FrequencyMs:    60000,
					InstanceNum:    1,
					SegmentID:      100,
				},
			},
		},
	}

	err := validateGroupConsistency(group)
	assert.Error(t, err)

	var validationErr *GroupValidationError
	assert.ErrorAs(t, err, &validationErr)
	assert.Equal(t, "organization_id", validationErr.Field)
	assert.Contains(t, validationErr.Message, "message 0")
}

func TestGroupValidationError_Error(t *testing.T) {
	err := &GroupValidationError{
		Field:    "test_field",
		Expected: "expected_value",
		Got:      "actual_value",
		Message:  "test message",
	}

	expectedErrorString := "group validation failed - test_field: expected expected_value, got actual_value (test message)"
	assert.Equal(t, expectedErrorString, err.Error())
}
