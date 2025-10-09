//go:build integration

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

package queries

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/testhelpers"
)

// TestListPromMetricTags_FiltersByMetricFingerprint tests that ListPromMetricTags
// correctly filters segments by metric fingerprint and returns only tags for that metric
func TestListPromMetricTags_FiltersByMetricFingerprint(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create label_name_map for http_requests_total metric
	labelMap1 := map[string]string{
		"resource_service_name": "resource.service.name",
		"metric_status_code":    "metric.status.code",
		"chq_timestamp":         "",
	}
	labelMap1JSON, err := json.Marshal(labelMap1)
	require.NoError(t, err)

	// Create label_name_map for cpu_usage_percent metric
	labelMap2 := map[string]string{
		"resource_service_name": "resource.service.name",
		"resource_host_name":    "resource.host.name",
		"chq_timestamp":         "",
	}
	labelMap2JSON, err := json.Marshal(labelMap2)
	require.NoError(t, err)

	// Compute fingerprints for metrics
	fp1 := fingerprint.ComputeFingerprint("metric_name", "http_requests_total")
	fp2 := fingerprint.ComputeFingerprint("metric_name", "cpu_usage_percent")

	// Insert segment for http_requests_total
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250109,
		FrequencyMs:    10000,
		SegmentID:      1001,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Fingerprints:   []int64{fp1},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		LabelNameMap:   labelMap1JSON,
	})
	require.NoError(t, err)

	// Insert segment for cpu_usage_percent
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250109,
		FrequencyMs:    10000,
		SegmentID:      1002,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    2000,
		FileSize:       100000,
		CreatedBy:      lrdb.CreatedByIngest,
		Fingerprints:   []int64{fp2},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		LabelNameMap:   labelMap2JSON,
	})
	require.NoError(t, err)

	// Query tags for http_requests_total only
	tags1, err := db.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgID,
		MetricFingerprint: fp1,
	})
	require.NoError(t, err)

	// Should return only tags from http_requests_total segment
	assert.Len(t, tags1, 3)
	assert.Contains(t, tags1, "resource_service_name")
	assert.Contains(t, tags1, "metric_status_code")
	assert.Contains(t, tags1, "chq_timestamp")
	assert.NotContains(t, tags1, "resource_host_name") // This is only in cpu_usage_percent

	// Query tags for cpu_usage_percent only
	tags2, err := db.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgID,
		MetricFingerprint: fp2,
	})
	require.NoError(t, err)

	// Should return only tags from cpu_usage_percent segment
	assert.Len(t, tags2, 3)
	assert.Contains(t, tags2, "resource_service_name")
	assert.Contains(t, tags2, "resource_host_name")
	assert.Contains(t, tags2, "chq_timestamp")
	assert.NotContains(t, tags2, "metric_status_code") // This is only in http_requests_total
}

// TestListPromMetricTags_NoSegments tests that query returns empty when no segments match
func TestListPromMetricTags_NoSegments(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()

	// Query for a metric that doesn't exist
	fp := fingerprint.ComputeFingerprint("metric_name", "nonexistent_metric")
	tags, err := db.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgID,
		MetricFingerprint: fp,
	})
	require.NoError(t, err)
	assert.Empty(t, tags)
}
