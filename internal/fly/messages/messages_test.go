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

package messages

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricIngestMessage_Marshal(t *testing.T) {
	orgID := uuid.New()
	msgID := uuid.New()
	now := time.Now().Round(time.Millisecond) // Round for JSON precision

	msg := &MetricIngestMessage{
		ID:             msgID,
		OrganizationID: orgID,
		CollectorName:  "collector-1",
		InstanceNum:    1,
		Bucket:         "metrics-bucket",
		ObjectID:       "path/to/object.parquet",
		FileSize:       1024 * 1024,
		QueuedAt:       now,
		Attempts:       2,
		LastError:      "connection timeout",
		LastAttempt:    now.Add(-time.Minute),
	}

	data, err := msg.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it's valid JSON
	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	assert.NoError(t, err)
	assert.Equal(t, msgID.String(), jsonMap["id"])
	assert.Equal(t, orgID.String(), jsonMap["organization_id"])
	assert.Equal(t, "collector-1", jsonMap["collector_name"])
}

func TestMetricIngestMessage_Unmarshal(t *testing.T) {
	orgID := uuid.New()
	msgID := uuid.New()
	now := time.Now().Round(time.Millisecond)

	jsonData := fmt.Sprintf(`{
		"id": "%s",
		"organization_id": "%s",
		"collector_name": "collector-1",
		"instance_num": 1,
		"bucket": "metrics-bucket",
		"object_id": "path/to/object.parquet",
		"file_size": 1048576,
		"queued_at": "%s",
		"attempts": 2,
		"last_error": "connection timeout",
		"last_attempt": "%s"
	}`, msgID, orgID, now.Format(time.RFC3339Nano), now.Add(-time.Minute).Format(time.RFC3339Nano))

	msg := &MetricIngestMessage{}
	err := msg.Unmarshal([]byte(jsonData))
	require.NoError(t, err)

	assert.Equal(t, msgID, msg.ID)
	assert.Equal(t, orgID, msg.OrganizationID)
	assert.Equal(t, "collector-1", msg.CollectorName)
	assert.Equal(t, int16(1), msg.InstanceNum)
	assert.Equal(t, "metrics-bucket", msg.Bucket)
	assert.Equal(t, "path/to/object.parquet", msg.ObjectID)
	assert.Equal(t, int64(1048576), msg.FileSize)
	assert.WithinDuration(t, now, msg.QueuedAt, time.Second)
	assert.Equal(t, 2, msg.Attempts)
	assert.Equal(t, "connection timeout", msg.LastError)
	assert.WithinDuration(t, now.Add(-time.Minute), msg.LastAttempt, time.Second)
}

func TestMetricIngestMessage_GetPartitionKey(t *testing.T) {
	orgID := uuid.New()
	msg := &MetricIngestMessage{
		OrganizationID: orgID,
	}

	key := msg.GetPartitionKey()
	assert.Equal(t, orgID.String(), key)
}

func TestMetricIngestBatch_Marshal(t *testing.T) {
	batchID := uuid.New()
	now := time.Now().Round(time.Millisecond)

	batch := &MetricIngestBatch{
		Messages: []MetricIngestMessage{
			{
				ID:             uuid.New(),
				OrganizationID: uuid.New(),
				CollectorName:  "collector-1",
				Bucket:         "bucket1",
				ObjectID:       "obj1",
			},
			{
				ID:             uuid.New(),
				OrganizationID: uuid.New(),
				CollectorName:  "collector-2",
				Bucket:         "bucket2",
				ObjectID:       "obj2",
			},
		},
		BatchID:   batchID,
		CreatedAt: now,
	}

	data, err := batch.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it's valid JSON
	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	assert.NoError(t, err)
	assert.Equal(t, batchID.String(), jsonMap["batch_id"])

	messages := jsonMap["messages"].([]interface{})
	assert.Len(t, messages, 2)
}

func TestMetricIngestBatch_Unmarshal(t *testing.T) {
	batchID := uuid.New()
	msgID1 := uuid.New()
	msgID2 := uuid.New()
	orgID1 := uuid.New()
	orgID2 := uuid.New()
	now := time.Now().Round(time.Millisecond)

	jsonData := fmt.Sprintf(`{
		"batch_id": "%s",
		"created_at": "%s",
		"messages": [
			{
				"id": "%s",
				"organization_id": "%s",
				"collector_name": "collector-1",
				"instance_num": 1,
				"bucket": "bucket1",
				"object_id": "obj1",
				"file_size": 1024,
				"queued_at": "%s",
				"attempts": 0,
				"last_attempt": "%s"
			},
			{
				"id": "%s",
				"organization_id": "%s",
				"collector_name": "collector-2",
				"instance_num": 2,
				"bucket": "bucket2",
				"object_id": "obj2",
				"file_size": 2048,
				"queued_at": "%s",
				"attempts": 0,
				"last_attempt": "%s"
			}
		]
	}`, batchID, now.Format(time.RFC3339Nano),
		msgID1, orgID1, now.Format(time.RFC3339Nano), time.Time{}.Format(time.RFC3339Nano),
		msgID2, orgID2, now.Format(time.RFC3339Nano), time.Time{}.Format(time.RFC3339Nano))

	batch := &MetricIngestBatch{}
	err := batch.Unmarshal([]byte(jsonData))
	require.NoError(t, err)

	assert.Equal(t, batchID, batch.BatchID)
	assert.WithinDuration(t, now, batch.CreatedAt, time.Second)
	assert.Len(t, batch.Messages, 2)

	assert.Equal(t, msgID1, batch.Messages[0].ID)
	assert.Equal(t, orgID1, batch.Messages[0].OrganizationID)
	assert.Equal(t, "collector-1", batch.Messages[0].CollectorName)

	assert.Equal(t, msgID2, batch.Messages[1].ID)
	assert.Equal(t, orgID2, batch.Messages[1].OrganizationID)
	assert.Equal(t, "collector-2", batch.Messages[1].CollectorName)
}

func TestLogIngestMessage_Marshal(t *testing.T) {
	orgID := uuid.New()
	msgID := uuid.New()
	now := time.Now().Round(time.Millisecond)

	msg := &LogIngestMessage{
		ID:             msgID,
		OrganizationID: orgID,
		CollectorName:  "log-collector",
		InstanceNum:    2,
		Bucket:         "logs-bucket",
		ObjectID:       "logs/2024/01/15/file.json.gz",
		FileSize:       2048 * 1024,
		QueuedAt:       now,
		Attempts:       1,
		LastError:      "",
		LastAttempt:    now,
	}

	data, err := msg.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it's valid JSON
	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	assert.NoError(t, err)
	assert.Equal(t, msgID.String(), jsonMap["id"])
	assert.Equal(t, orgID.String(), jsonMap["organization_id"])
	assert.Equal(t, "log-collector", jsonMap["collector_name"])
}

func TestLogIngestMessage_Unmarshal(t *testing.T) {
	orgID := uuid.New()
	msgID := uuid.New()
	now := time.Now().Round(time.Millisecond)

	jsonData := fmt.Sprintf(`{
		"id": "%s",
		"organization_id": "%s",
		"collector_name": "log-collector",
		"instance_num": 2,
		"bucket": "logs-bucket",
		"object_id": "logs/2024/01/15/file.json.gz",
		"file_size": 2097152,
		"queued_at": "%s",
		"attempts": 1,
		"last_attempt": "%s"
	}`, msgID, orgID, now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))

	msg := &LogIngestMessage{}
	err := msg.Unmarshal([]byte(jsonData))
	require.NoError(t, err)

	assert.Equal(t, msgID, msg.ID)
	assert.Equal(t, orgID, msg.OrganizationID)
	assert.Equal(t, "log-collector", msg.CollectorName)
	assert.Equal(t, int16(2), msg.InstanceNum)
	assert.Equal(t, "logs-bucket", msg.Bucket)
	assert.Equal(t, "logs/2024/01/15/file.json.gz", msg.ObjectID)
	assert.Equal(t, int64(2097152), msg.FileSize)
	assert.WithinDuration(t, now, msg.QueuedAt, time.Second)
	assert.Equal(t, 1, msg.Attempts)
	assert.Empty(t, msg.LastError)
	assert.WithinDuration(t, now, msg.LastAttempt, time.Second)
}

func TestLogIngestMessage_GetPartitionKey(t *testing.T) {
	orgID := uuid.New()
	msg := &LogIngestMessage{
		OrganizationID: orgID,
	}

	key := msg.GetPartitionKey()
	assert.Equal(t, orgID.String(), key)
}

func TestTraceIngestMessage_Marshal(t *testing.T) {
	orgID := uuid.New()
	msgID := uuid.New()
	now := time.Now().Round(time.Millisecond)

	msg := &TraceIngestMessage{
		ID:             msgID,
		OrganizationID: orgID,
		CollectorName:  "trace-collector",
		InstanceNum:    3,
		Bucket:         "traces-bucket",
		ObjectID:       "traces/2024/01/15/spans.proto",
		FileSize:       512 * 1024,
		QueuedAt:       now,
		Attempts:       0,
		LastError:      "",
		LastAttempt:    time.Time{},
	}

	data, err := msg.Marshal()
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it's valid JSON
	var jsonMap map[string]interface{}
	err = json.Unmarshal(data, &jsonMap)
	assert.NoError(t, err)
	assert.Equal(t, msgID.String(), jsonMap["id"])
	assert.Equal(t, orgID.String(), jsonMap["organization_id"])
	assert.Equal(t, "trace-collector", jsonMap["collector_name"])
}

func TestTraceIngestMessage_Unmarshal(t *testing.T) {
	orgID := uuid.New()
	msgID := uuid.New()
	now := time.Now().Round(time.Millisecond)

	jsonData := fmt.Sprintf(`{
		"id": "%s",
		"organization_id": "%s",
		"collector_name": "trace-collector",
		"instance_num": 3,
		"bucket": "traces-bucket",
		"object_id": "traces/2024/01/15/spans.proto",
		"file_size": 524288,
		"queued_at": "%s",
		"attempts": 0,
		"last_attempt": "%s"
	}`, msgID, orgID, now.Format(time.RFC3339Nano), time.Time{}.Format(time.RFC3339Nano))

	msg := &TraceIngestMessage{}
	err := msg.Unmarshal([]byte(jsonData))
	require.NoError(t, err)

	assert.Equal(t, msgID, msg.ID)
	assert.Equal(t, orgID, msg.OrganizationID)
	assert.Equal(t, "trace-collector", msg.CollectorName)
	assert.Equal(t, int16(3), msg.InstanceNum)
	assert.Equal(t, "traces-bucket", msg.Bucket)
	assert.Equal(t, "traces/2024/01/15/spans.proto", msg.ObjectID)
	assert.Equal(t, int64(524288), msg.FileSize)
	assert.WithinDuration(t, now, msg.QueuedAt, time.Second)
	assert.Equal(t, 0, msg.Attempts)
	assert.Empty(t, msg.LastError)
	assert.True(t, msg.LastAttempt.IsZero())
}

func TestTraceIngestMessage_GetPartitionKey(t *testing.T) {
	orgID := uuid.New()
	msg := &TraceIngestMessage{
		OrganizationID: orgID,
	}

	key := msg.GetPartitionKey()
	assert.Equal(t, orgID.String(), key)
}

func TestMessageTypes_EmptyValues(t *testing.T) {
	t.Run("MetricIngestMessage empty", func(t *testing.T) {
		msg := &MetricIngestMessage{}
		data, err := msg.Marshal()
		assert.NoError(t, err)

		unmarshaledMsg := &MetricIngestMessage{}
		err = unmarshaledMsg.Unmarshal(data)
		assert.NoError(t, err)
		assert.Equal(t, msg, unmarshaledMsg)
	})

	t.Run("LogIngestMessage empty", func(t *testing.T) {
		msg := &LogIngestMessage{}
		data, err := msg.Marshal()
		assert.NoError(t, err)

		unmarshaledMsg := &LogIngestMessage{}
		err = unmarshaledMsg.Unmarshal(data)
		assert.NoError(t, err)
		assert.Equal(t, msg, unmarshaledMsg)
	})

	t.Run("TraceIngestMessage empty", func(t *testing.T) {
		msg := &TraceIngestMessage{}
		data, err := msg.Marshal()
		assert.NoError(t, err)

		unmarshaledMsg := &TraceIngestMessage{}
		err = unmarshaledMsg.Unmarshal(data)
		assert.NoError(t, err)
		assert.Equal(t, msg, unmarshaledMsg)
	})
}

func TestMessageTypes_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{invalid json}`)

	t.Run("MetricIngestMessage invalid", func(t *testing.T) {
		msg := &MetricIngestMessage{}
		err := msg.Unmarshal(invalidJSON)
		assert.Error(t, err)
	})

	t.Run("LogIngestMessage invalid", func(t *testing.T) {
		msg := &LogIngestMessage{}
		err := msg.Unmarshal(invalidJSON)
		assert.Error(t, err)
	})

	t.Run("TraceIngestMessage invalid", func(t *testing.T) {
		msg := &TraceIngestMessage{}
		err := msg.Unmarshal(invalidJSON)
		assert.Error(t, err)
	})

	t.Run("MetricIngestBatch invalid", func(t *testing.T) {
		batch := &MetricIngestBatch{}
		err := batch.Unmarshal(invalidJSON)
		assert.Error(t, err)
	})
}

func TestMessageTypes_RoundTrip(t *testing.T) {
	// Test that marshal/unmarshal preserves data
	t.Run("MetricIngestMessage round trip", func(t *testing.T) {
		original := &MetricIngestMessage{
			ID:             uuid.New(),
			OrganizationID: uuid.New(),
			CollectorName:  "test",
			InstanceNum:    5,
			Bucket:         "bucket",
			ObjectID:       "object",
			FileSize:       9999,
			QueuedAt:       time.Now().Round(time.Millisecond),
			Attempts:       3,
			LastError:      "error",
			LastAttempt:    time.Now().Round(time.Millisecond),
		}

		data, err := original.Marshal()
		require.NoError(t, err)

		restored := &MetricIngestMessage{}
		err = restored.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, original, restored)
	})

	t.Run("LogIngestMessage round trip", func(t *testing.T) {
		original := &LogIngestMessage{
			ID:             uuid.New(),
			OrganizationID: uuid.New(),
			CollectorName:  "logger",
			InstanceNum:    10,
			Bucket:         "log-bucket",
			ObjectID:       "log-object",
			FileSize:       88888,
			QueuedAt:       time.Now().Round(time.Millisecond),
			Attempts:       1,
			LastError:      "timeout",
			LastAttempt:    time.Now().Round(time.Millisecond),
		}

		data, err := original.Marshal()
		require.NoError(t, err)

		restored := &LogIngestMessage{}
		err = restored.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, original, restored)
	})

	t.Run("TraceIngestMessage round trip", func(t *testing.T) {
		original := &TraceIngestMessage{
			ID:             uuid.New(),
			OrganizationID: uuid.New(),
			CollectorName:  "tracer",
			InstanceNum:    20,
			Bucket:         "trace-bucket",
			ObjectID:       "trace-object",
			FileSize:       777777,
			QueuedAt:       time.Now().Round(time.Millisecond),
			Attempts:       5,
			LastError:      "network error",
			LastAttempt:    time.Now().Round(time.Millisecond),
		}

		data, err := original.Marshal()
		require.NoError(t, err)

		restored := &TraceIngestMessage{}
		err = restored.Unmarshal(data)
		require.NoError(t, err)

		assert.Equal(t, original, restored)
	})
}
