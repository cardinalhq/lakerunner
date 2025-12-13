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
		StartDateint:      20250109,
		EndDateint:        20250109,
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
		StartDateint:      20250109,
		EndDateint:        20250109,
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
		StartDateint:      20250109,
		EndDateint:        20250109,
		MetricFingerprint: fp,
	})
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestListPromMetricTags_DateintFiltering tests that dateint range filters correctly
func TestListPromMetricTags_DateintFiltering(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()

	// Create label_name_map
	labelMap := map[string]string{
		"resource_service_name": "resource.service.name",
		"metric_status_code":    "metric.status.code",
	}
	labelMapJSON, err := json.Marshal(labelMap)
	require.NoError(t, err)

	fp := fingerprint.ComputeFingerprint("metric_name", "http_requests_total")

	// Insert segment for Jan 9, 2025
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
		Fingerprints:   []int64{fp},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		LabelNameMap:   labelMapJSON,
	})
	require.NoError(t, err)

	// Insert segment for Jan 10, 2025
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1002,
		InstanceNum:    1,
		StartTs:        now.UnixMilli(),
		EndTs:          now.Add(time.Hour).UnixMilli(),
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Fingerprints:   []int64{fp},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		LabelNameMap:   labelMapJSON,
	})
	require.NoError(t, err)

	// Query with dateint range that includes both segments
	tags, err := db.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgID,
		StartDateint:      20250109,
		EndDateint:        20250110,
		MetricFingerprint: fp,
	})
	require.NoError(t, err)
	assert.Len(t, tags, 2)
	assert.Contains(t, tags, "resource_service_name")
	assert.Contains(t, tags, "metric_status_code")

	// Query with dateint range that includes only Jan 9
	tags, err = db.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgID,
		StartDateint:      20250109,
		EndDateint:        20250109,
		MetricFingerprint: fp,
	})
	require.NoError(t, err)
	assert.Len(t, tags, 2)
	assert.Contains(t, tags, "resource_service_name")
	assert.Contains(t, tags, "metric_status_code")

	// Query with dateint range that excludes all segments (future date)
	tags, err = db.ListPromMetricTags(ctx, lrdb.ListPromMetricTagsParams{
		OrganizationID:    orgID,
		StartDateint:      20250120,
		EndDateint:        20250121,
		MetricFingerprint: fp,
	})
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestListMetricNamesWithTypes tests that ListMetricNamesWithTypes correctly
// returns metric names with their types from the parallel arrays
func TestListMetricNamesWithTypes(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()
	baseTs := now.UnixMilli()

	// Insert segment with multiple metrics of different types
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1001,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12345},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"http_requests_total", "cpu_usage_percent", "request_duration_seconds"},
		MetricTypes:    []int16{lrdb.MetricTypeSum, lrdb.MetricTypeGauge, lrdb.MetricTypeHistogram},
	})
	require.NoError(t, err)

	// Query metrics with types
	results, err := db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
	})
	require.NoError(t, err)
	require.Len(t, results, 3)

	// Build a map for easier verification
	resultMap := make(map[string]int16)
	for _, r := range results {
		resultMap[r.MetricName] = r.MetricType
	}

	assert.Equal(t, lrdb.MetricTypeSum, resultMap["http_requests_total"])
	assert.Equal(t, lrdb.MetricTypeGauge, resultMap["cpu_usage_percent"])
	assert.Equal(t, lrdb.MetricTypeHistogram, resultMap["request_duration_seconds"])
}

// TestListMetricNamesWithTypes_Deduplication tests that the query returns
// distinct pairs when the same metric appears in multiple segments
func TestListMetricNamesWithTypes_Deduplication(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()
	baseTs := now.UnixMilli()

	// Insert first segment with some metrics
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1001,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 1800000,
		RecordCount:    500,
		FileSize:       25000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12345},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"http_requests_total", "cpu_usage_percent"},
		MetricTypes:    []int16{lrdb.MetricTypeSum, lrdb.MetricTypeGauge},
	})
	require.NoError(t, err)

	// Insert second segment with overlapping metrics
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1002,
		InstanceNum:    1,
		StartTs:        baseTs + 1800000,
		EndTs:          baseTs + 3600000,
		RecordCount:    500,
		FileSize:       25000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12346},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"http_requests_total", "memory_usage_bytes"},
		MetricTypes:    []int16{lrdb.MetricTypeSum, lrdb.MetricTypeGauge},
	})
	require.NoError(t, err)

	// Query should return 3 distinct metrics (http_requests_total appears in both)
	results, err := db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
	})
	require.NoError(t, err)
	require.Len(t, results, 3)

	resultMap := make(map[string]int16)
	for _, r := range results {
		resultMap[r.MetricName] = r.MetricType
	}

	assert.Equal(t, lrdb.MetricTypeSum, resultMap["http_requests_total"])
	assert.Equal(t, lrdb.MetricTypeGauge, resultMap["cpu_usage_percent"])
	assert.Equal(t, lrdb.MetricTypeGauge, resultMap["memory_usage_bytes"])
}

// TestListMetricNamesWithTypes_TimeRangeFiltering tests ts_range filtering
func TestListMetricNamesWithTypes_TimeRangeFiltering(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()
	baseTs := now.UnixMilli()

	// Insert segment covering 0-1h
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1001,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000, // 1 hour
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12345},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"early_metric"},
		MetricTypes:    []int16{lrdb.MetricTypeGauge},
	})
	require.NoError(t, err)

	// Insert segment covering 2h-3h
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1002,
		InstanceNum:    1,
		StartTs:        baseTs + 7200000,  // 2 hours
		EndTs:          baseTs + 10800000, // 3 hours
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12346},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"late_metric"},
		MetricTypes:    []int16{lrdb.MetricTypeSum},
	})
	require.NoError(t, err)

	// Query only first hour - should only find early_metric
	results, err := db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "early_metric", results[0].MetricName)
	assert.Equal(t, lrdb.MetricTypeGauge, results[0].MetricType)

	// Query 2h-3h - should only find late_metric
	results, err = db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs + 7200000,
		EndTs:          baseTs + 10800000,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "late_metric", results[0].MetricName)
	assert.Equal(t, lrdb.MetricTypeSum, results[0].MetricType)

	// Query entire range - should find both
	results, err = db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs,
		EndTs:          baseTs + 10800000,
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
}

// TestListMetricNamesWithTypes_UnpublishedFiltering tests that unpublished segments are excluded
func TestListMetricNamesWithTypes_UnpublishedFiltering(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()
	baseTs := now.UnixMilli()

	// Insert published segment
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1001,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12345},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"published_metric"},
		MetricTypes:    []int16{lrdb.MetricTypeGauge},
	})
	require.NoError(t, err)

	// Insert unpublished segment
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1002,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      false,
		Fingerprints:   []int64{12346},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"unpublished_metric"},
		MetricTypes:    []int16{lrdb.MetricTypeSum},
	})
	require.NoError(t, err)

	// Query should only return published metric
	results, err := db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "published_metric", results[0].MetricName)
}

// TestListMetricNamesWithTypes_NullArrays tests that segments with NULL arrays are excluded
func TestListMetricNamesWithTypes_NullArrays(t *testing.T) {
	ctx := context.Background()
	db := testhelpers.NewTestLRDBStore(t)

	orgID := uuid.New()
	now := time.Now()
	baseTs := now.UnixMilli()

	// Insert segment with metric_names and metric_types populated
	err := db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1001,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12345},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    []string{"good_metric"},
		MetricTypes:    []int16{lrdb.MetricTypeGauge},
	})
	require.NoError(t, err)

	// Insert segment with NULL arrays (old segment before migration)
	err = db.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgID,
		Dateint:        20250110,
		FrequencyMs:    10000,
		SegmentID:      1002,
		InstanceNum:    1,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
		RecordCount:    1000,
		FileSize:       50000,
		CreatedBy:      lrdb.CreatedByIngest,
		Published:      true,
		Fingerprints:   []int64{12346},
		SortVersion:    lrdb.CurrentMetricSortVersion,
		MetricNames:    nil, // NULL
		MetricTypes:    nil, // NULL
	})
	require.NoError(t, err)

	// Query should only return segment with populated arrays
	results, err := db.ListMetricNamesWithTypes(ctx, lrdb.ListMetricNamesWithTypesParams{
		OrganizationID: orgID,
		StartDateint:   20250110,
		EndDateint:     20250110,
		StartTs:        baseTs,
		EndTs:          baseTs + 3600000,
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "good_metric", results[0].MetricName)
}
