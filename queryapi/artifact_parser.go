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

package queryapi

import (
	"bytes"
	"fmt"
	"io"
	"math/big"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/promql"
)

const readBatchSize = 1024

// parseMetricsArtifact reads a Parquet artifact produced by a metrics query
// and returns SketchInput rows. It mirrors the worker-side sketchInputMapper.
func parseMetricsArtifact(data []byte, req PushDownRequest) ([]promql.SketchInput, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader, cleanup, err := newMapReader(data)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var results []promql.SketchInput
	if err := readRows(reader, func(row map[string]any) {
		si := mapMetricsRow(row, req)
		results = append(results, si)
	}); err != nil {
		return nil, fmt.Errorf("read metrics artifact: %w", err)
	}
	return results, nil
}

// parseSummaryArtifact reads a Parquet artifact from a summary query.
func parseSummaryArtifact(data []byte, req PushDownRequest) ([]promql.SketchInput, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader, cleanup, err := newMapReader(data)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var results []promql.SketchInput
	if err := readRows(reader, func(row map[string]any) {
		si := mapSummaryRow(row, req)
		results = append(results, si)
	}); err != nil {
		return nil, fmt.Errorf("read summary artifact: %w", err)
	}
	return results, nil
}

// parseLogsArtifact reads a Parquet artifact produced by a logs or spans query.
func parseLogsArtifact(data []byte) ([]promql.Exemplar, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader, cleanup, err := newMapReader(data)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var results []promql.Exemplar
	if err := readRows(reader, func(row map[string]any) {
		ex := mapExemplarRow(row)
		results = append(results, ex)
	}); err != nil {
		return nil, fmt.Errorf("read logs artifact: %w", err)
	}
	return results, nil
}

// parseTagValuesArtifact reads a Parquet artifact produced by a tag values query.
func parseTagValuesArtifact(data []byte) ([]promql.TagValue, error) {
	if len(data) == 0 {
		return nil, nil
	}
	reader, cleanup, err := newMapReader(data)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	var results []promql.TagValue
	if err := readRows(reader, func(row map[string]any) {
		tv := mapTagValueRow(row)
		results = append(results, tv)
	}); err != nil {
		return nil, fmt.Errorf("read tag values artifact: %w", err)
	}
	return results, nil
}

// newMapReader creates a parquet generic reader for map[string]any from bytes.
func newMapReader(data []byte) (*parquet.GenericReader[map[string]any], func(), error) {
	r := bytes.NewReader(data)
	pf, err := parquet.OpenFile(r, int64(len(data)))
	if err != nil {
		return nil, nil, fmt.Errorf("open parquet: %w", err)
	}
	reader := parquet.NewGenericReader[map[string]any](pf, pf.Schema())
	cleanup := func() { _ = reader.Close() }
	return reader, cleanup, nil
}

// readRows reads all rows from a generic reader, calling fn for each.
func readRows(reader *parquet.GenericReader[map[string]any], fn func(map[string]any)) error {
	buf := make([]map[string]any, readBatchSize)
	for {
		for i := range buf {
			buf[i] = make(map[string]any)
		}
		n, err := reader.Read(buf)
		for _, row := range buf[:n] {
			fn(row)
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if n == 0 {
			return nil
		}
	}
}

// mapMetricsRow mirrors the worker-side sketchInputMapper.
func mapMetricsRow(row map[string]any, req PushDownRequest) promql.SketchInput {
	var ts int64
	agg := map[string]float64{}
	tags := make(map[string]any, len(row))

	if req.BaseExpr != nil {
		tags["name"] = req.BaseExpr.Metric
		for _, m := range req.BaseExpr.Matchers {
			if m.Op == promql.MatchEq {
				tags[m.Label] = m.Value
			}
		}
	}

	for col, val := range row {
		switch col {
		case "bucket_ts":
			ts = pqToInt64(val)
		case promql.SUM, promql.COUNT, promql.MIN, promql.MAX:
			if val != nil {
				if f, ok := pqToFloat64(val); ok {
					agg[col] = f
				}
			}
		default:
			if val != nil {
				tags[col] = val
			}
		}
	}

	si := promql.SketchInput{
		Timestamp: ts,
		SketchTags: promql.SketchTags{
			Tags:       tags,
			SketchType: promql.SketchMAP,
		},
	}
	if len(agg) > 0 {
		si.SketchTags.Agg = agg
	}
	if req.BaseExpr != nil {
		si.ExprID = req.BaseExpr.ID
		si.Frequency = int64(req.Step.Seconds())
	}
	si.OrganizationID = req.OrganizationID.String()
	return si
}

// mapSummaryRow mirrors the worker-side summarySketchMapper.
func mapSummaryRow(row map[string]any, req PushDownRequest) promql.SketchInput {
	tags := make(map[string]any, len(row))

	if req.BaseExpr != nil {
		tags["name"] = req.BaseExpr.Metric
		for _, m := range req.BaseExpr.Matchers {
			if m.Op == promql.MatchEq {
				tags[m.Label] = m.Value
			}
		}
	}

	var sketchBytes []byte
	for col, val := range row {
		switch col {
		case "chq_sketch":
			if val != nil {
				if b, ok := val.([]byte); ok {
					sketchBytes = b
				}
			}
		default:
			if val != nil {
				tags[col] = val
			}
		}
	}

	si := promql.SketchInput{
		Timestamp: 0,
		SketchTags: promql.SketchTags{
			Tags:       tags,
			SketchType: promql.SketchDDS,
			Bytes:      sketchBytes,
		},
	}
	if req.BaseExpr != nil {
		si.ExprID = req.BaseExpr.ID
		si.Frequency = int64(req.Step.Seconds())
	}
	si.OrganizationID = req.OrganizationID.String()
	return si
}

// mapExemplarRow mirrors the worker-side exemplarMapper.
func mapExemplarRow(row map[string]any) promql.Exemplar {
	tags := make(map[string]any, len(row))
	ex := promql.Exemplar{}
	hasTimestamp := false

	for col, val := range row {
		switch col {
		case "chq_timestamp":
			ex.Timestamp = pqToInt64(val)
			hasTimestamp = true
		case "chq_tsns":
			ex.TimestampNs = pqToInt64(val)
			tags["chq_tsns"] = val
		case "metric_name":
			// skip
		default:
			if val != nil {
				tags[col] = val
			}
		}
	}

	if !hasTimestamp && ex.TimestampNs != 0 {
		ex.Timestamp = ex.TimestampNs / 1_000_000
	}
	ex.Tags = tags
	return ex
}

// mapTagValueRow mirrors the worker-side tagValuesMapper.
func mapTagValueRow(row map[string]any) promql.TagValue {
	tv := promql.TagValue{}
	if val, ok := row["tag_value"]; ok && val != nil {
		tv.Value = pqAsString(val)
	}
	return tv
}

func pqToInt64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	default:
		return 0
	}
}

func pqToFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	case uint64:
		return float64(n), true
	case *big.Int:
		f, _ := new(big.Float).SetInt(n).Float64()
		return f, true
	default:
		return 0, false
	}
}

func pqAsString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return fmt.Sprintf("%v", x)
	}
}
