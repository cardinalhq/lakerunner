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

package metricsprocessing

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

const (
	logDerivedMetricType          = "sum"
	logDerivedMetricTelemetryType = "logs"
	logDerivedMetricValue         = 1.0
)

type logDerivedMetricParquet struct {
	FileName     string
	RecordCount  int64
	FileSize     int64
	StartTs      int64
	EndTs        int64
	Fingerprints []int64
	LabelNameMap []byte
	MetricNames  []string
	MetricTypes  []int16
}

func createLogDerivedMetricParquet(
	tmpDir string,
	dateint int32,
	metricSegmentID int64,
	metricName string,
	aggCounts map[factories.LogAggKey]int64,
) (*logDerivedMetricParquet, error) {
	if len(aggCounts) == 0 {
		return nil, nil
	}
	if metricName == "" {
		return nil, fmt.Errorf("metric name is required")
	}

	streamFieldName := chooseLogDerivedStreamField(aggCounts)

	type sortableRow struct {
		ts  int64
		tid int64
		row map[string]any
	}
	rows := make([]sortableRow, 0, len(aggCounts))

	const unsetTs = int64(-1)
	minTs := unsetTs
	maxTs := unsetTs

	tagValuesByName := map[string]mapset.Set[string]{
		"metric_name":        mapset.NewSet[string](metricName),
		"chq_telemetry_type": mapset.NewSet[string](logDerivedMetricTelemetryType),
		"log_level":          mapset.NewSet[string](),
		"resource_level":     mapset.NewSet[string](),
	}
	if streamFieldName != "" {
		tagValuesByName[streamFieldName] = mapset.NewSet[string]()
	}

	for key, count := range aggCounts {
		if count <= 0 {
			continue
		}

		sketch, err := helpers.GetSketch()
		if err != nil {
			return nil, fmt.Errorf("create sketch: %w", err)
		}
		if err := sketch.AddWithCount(logDerivedMetricValue, float64(count)); err != nil {
			helpers.PutSketch(sketch)
			return nil, fmt.Errorf("add sketch count: %w", err)
		}
		sketchBytes := helpers.EncodeAndReturnSketch(sketch)

		tidTags := map[wkk.RowKey]any{
			wkk.RowKeyCName:                 metricName,
			wkk.RowKeyCMetricType:           logDerivedMetricType,
			wkk.NewRowKey("resource_level"): key.LogLevel,
		}
		if streamFieldName != "" {
			tidTags[wkk.NewRowKey(streamFieldName)] = key.StreamFieldValue
		}
		tid := fingerprinter.ComputeTID(tidTags)

		ts := key.TimestampBucket
		row := map[string]any{
			"chq_timestamp":      ts,
			"chq_tsns":           ts * 1_000_000,
			"metric_name":        metricName,
			"chq_metric_type":    logDerivedMetricType,
			"chq_telemetry_type": logDerivedMetricTelemetryType,
			"chq_tid":            tid,
			"chq_sketch":         sketchBytes,
			"chq_rollup_sum":     float64(count),
			"chq_rollup_count":   count,
			"chq_rollup_avg":     logDerivedMetricValue,
			"chq_rollup_min":     logDerivedMetricValue,
			"chq_rollup_max":     logDerivedMetricValue,
			"chq_rollup_p25":     logDerivedMetricValue,
			"chq_rollup_p50":     logDerivedMetricValue,
			"chq_rollup_p75":     logDerivedMetricValue,
			"chq_rollup_p90":     logDerivedMetricValue,
			"chq_rollup_p95":     logDerivedMetricValue,
			"chq_rollup_p99":     logDerivedMetricValue,
			"log_level":          key.LogLevel,
			"resource_level":     key.LogLevel,
		}
		if streamFieldName != "" {
			row[streamFieldName] = key.StreamFieldValue
		}

		rows = append(rows, sortableRow{
			ts:  ts,
			tid: tid,
			row: row,
		})

		if minTs == unsetTs || ts < minTs {
			minTs = ts
		}
		if maxTs == unsetTs || ts > maxTs {
			maxTs = ts
		}

		tagValuesByName["log_level"].Add(key.LogLevel)
		tagValuesByName["resource_level"].Add(key.LogLevel)
		if streamFieldName != "" {
			tagValuesByName[streamFieldName].Add(key.StreamFieldValue)
		}
	}

	if len(rows) == 0 {
		return nil, nil
	}

	slices.SortFunc(rows, func(a, b sortableRow) int {
		if a.ts < b.ts {
			return -1
		}
		if a.ts > b.ts {
			return 1
		}
		if a.tid < b.tid {
			return -1
		}
		if a.tid > b.tid {
			return 1
		}
		return 0
	})

	columns := []string{
		"chq_timestamp",
		"chq_tsns",
		"metric_name",
		"chq_metric_type",
		"chq_telemetry_type",
		"chq_tid",
		"chq_sketch",
		"chq_rollup_sum",
		"chq_rollup_count",
		"chq_rollup_avg",
		"chq_rollup_min",
		"chq_rollup_max",
		"chq_rollup_p25",
		"chq_rollup_p50",
		"chq_rollup_p75",
		"chq_rollup_p90",
		"chq_rollup_p95",
		"chq_rollup_p99",
		"log_level",
		"resource_level",
	}
	if streamFieldName != "" && streamFieldName != "log_level" && streamFieldName != "resource_level" {
		columns = append(columns, streamFieldName)
	}

	schema := buildLogDerivedMetricSchema(streamFieldName)
	fileName := filepath.Join(tmpDir, fmt.Sprintf("metrics_log_events_%d_%d.parquet", dateint, metricSegmentID))
	file, err := os.Create(fileName)
	if err != nil {
		return nil, fmt.Errorf("create metric parquet file: %w", err)
	}

	writer := parquet.NewGenericWriter[map[string]any](file, schema,
		parquet.Compression(&parquet.Zstd),
	)

	outRows := make([]map[string]any, 0, len(rows))
	for i := range rows {
		outRows = append(outRows, rows[i].row)
	}

	if _, err := writer.Write(outRows); err != nil {
		_ = writer.Close()
		_ = file.Close()
		return nil, fmt.Errorf("write metric parquet rows: %w", err)
	}
	if err := writer.Close(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("close metric parquet writer: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close metric parquet file: %w", err)
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, fmt.Errorf("stat metric parquet file: %w", err)
	}

	fps := fingerprint.ToFingerprints(tagValuesByName).ToSlice()
	slices.Sort(fps)

	return &logDerivedMetricParquet{
		FileName:     fileName,
		RecordCount:  int64(len(rows)),
		FileSize:     stat.Size(),
		StartTs:      minTs,
		EndTs:        maxTs + int64(factories.AggFrequency),
		Fingerprints: fps,
		LabelNameMap: buildLabelNameMapFromSchema(columns),
		MetricNames:  []string{metricName},
		MetricTypes:  []int16{lrdb.MetricTypeSum},
	}, nil
}

func chooseLogDerivedStreamField(aggCounts map[factories.LogAggKey]int64) string {
	for key := range aggCounts {
		if key.StreamFieldName != "" {
			return key.StreamFieldName
		}
	}
	return "resource_customer_domain"
}

func buildLogDerivedMetricSchema(streamFieldName string) *parquet.Schema {
	group := parquet.Group{
		"chq_timestamp":      parquet.Leaf(parquet.Int64Type),
		"chq_tsns":           parquet.Leaf(parquet.Int64Type),
		"metric_name":        parquet.String(),
		"chq_metric_type":    parquet.String(),
		"chq_telemetry_type": parquet.String(),
		"chq_tid":            parquet.Leaf(parquet.Int64Type),
		"chq_sketch":         parquet.Leaf(parquet.ByteArrayType),
		"chq_rollup_sum":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_count":   parquet.Leaf(parquet.Int64Type),
		"chq_rollup_avg":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_min":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_max":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_p25":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_p50":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_p75":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_p90":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_p95":     parquet.Leaf(parquet.DoubleType),
		"chq_rollup_p99":     parquet.Leaf(parquet.DoubleType),
		"log_level":          parquet.String(),
		"resource_level":     parquet.String(),
	}
	if streamFieldName != "" && streamFieldName != "log_level" && streamFieldName != "resource_level" {
		group[streamFieldName] = parquet.String()
	}
	return parquet.NewSchema("log_event_metrics", group)
}
