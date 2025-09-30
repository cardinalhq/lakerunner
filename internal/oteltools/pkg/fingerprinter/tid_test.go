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

package fingerprinter

import (
	"testing"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/stretchr/testify/assert"
)

func TestComputeTID_NewBehavior(t *testing.T) {
	// Test that TID changes when specific fields change
	t.Run("TID changes with _cardinalhq_name", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric2",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when _cardinalhq_name changes")
	})

	t.Run("TID changes with _cardinalhq_metric_type", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"):        "metric1",
			wkk.NewRowKey("_cardinalhq_metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):           "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"):        "metric1",
			wkk.NewRowKey("_cardinalhq_metric_type"): "counter",
			wkk.NewRowKey("resource_host"):           "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when _cardinalhq_metric_type changes")
	})

	t.Run("TID changes with resource.* fields", func(t *testing.T) {
		// Test adding a resource field
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
			wkk.NewRowKey("resource_region"):  "us-east",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when resource field is added")

		// Test changing a resource field value
		tags3 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server2",
		}
		tid3 := ComputeTID(tags3)
		assert.NotEqual(t, tid1, tid3, "TID should change when resource field value changes")

		// Test removing a resource field
		tags4 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
		}
		tid4 := ComputeTID(tags4)
		assert.NotEqual(t, tid1, tid4, "TID should change when resource field is removed")
	})

	t.Run("TID changes with metric.* fields", func(t *testing.T) {
		// Test adding a metric field
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("metric_label1"):    "value1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("metric_label1"):    "value1",
			wkk.NewRowKey("metric_label2"):    "value2",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.NotEqual(t, tid1, tid2, "TID should change when metric field is added")

		// Test changing a metric field value
		tags3 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("metric_label1"):    "value3",
		}
		tid3 := ComputeTID(tags3)
		assert.NotEqual(t, tid1, tid3, "TID should change when metric field value changes")
	})

	t.Run("TID ignores non-string resource.* and metric.* fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
			wkk.NewRowKey("resource_count"):   123,   // non-string, should be ignored
			wkk.NewRowKey("metric_value"):     45.67, // non-string, should be ignored
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when non-string resource/metric fields are added")
	})

	t.Run("TID does not change with scope.* fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
			wkk.NewRowKey("scope_name"):       "my-scope",
			wkk.NewRowKey("scope_version"):    "1.0.0",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when scope fields are added")
	})

	t.Run("TID does not change with arbitrary fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
			wkk.NewRowKey("alice"):            "value",
			wkk.NewRowKey("bob"):              "another",
			wkk.NewRowKey("random_field"):     "ignored",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when arbitrary fields are added")
	})

	t.Run("TID ignores other _cardinalhq.* fields", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"):        "metric1",
			wkk.NewRowKey("_cardinalhq.metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):           "server1",
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"):        "metric1",
			wkk.NewRowKey("_cardinalhq.metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):           "server1",
			wkk.NewRowKey("_cardinalhq_timestamp"):   123456789,
			wkk.NewRowKey("_cardinalhq.description"): "some description",
			wkk.NewRowKey("_cardinalhq.unit"):        "bytes",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "TID should not change when other _cardinalhq fields are added")
	})

	t.Run("TID is deterministic", func(t *testing.T) {
		tags := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"):        "metric1",
			wkk.NewRowKey("_cardinalhq.metric_type"): "gauge",
			wkk.NewRowKey("resource_host"):           "server1",
			wkk.NewRowKey("resource_region"):         "us-east",
			wkk.NewRowKey("metric_label1"):           "value1",
			wkk.NewRowKey("metric_label2"):           "value2",
		}
		tid1 := ComputeTID(tags)
		tid2 := ComputeTID(tags)
		tid3 := ComputeTID(tags)
		assert.Equal(t, tid1, tid2, "TID should be deterministic")
		assert.Equal(t, tid1, tid3, "TID should be deterministic")
	})

	t.Run("Empty values are filtered", func(t *testing.T) {
		tags1 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
			wkk.NewRowKey("resource_region"):  "", // empty string should be filtered
		}
		tags2 := map[wkk.RowKey]any{
			wkk.NewRowKey("_cardinalhq_name"): "metric1",
			wkk.NewRowKey("resource_host"):    "server1",
		}
		tid1 := ComputeTID(tags1)
		tid2 := ComputeTID(tags2)
		assert.Equal(t, tid1, tid2, "Empty string values should be filtered out")
	})
}
