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

package promql

import (
	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/axiomhq/hyperloglog"
)

type SketchType string

const (
	SketchHLL SketchType = "hll" // cardinality
	SketchMAP SketchType = "map" // pre-aggregated (sum, count, min, max, etc.)
	SketchDDS SketchType = "dds" // distinct count sketch
	SUM                  = "sum"
	COUNT                = "count"
	MIN                  = "min"
	MAX                  = "max"
)

type SketchTags struct {
	Tags       map[string]any     `json:"tags"`
	SketchType SketchType         `json:"sketchType"`
	Bytes      []byte             `json:"bytes,omitempty"`
	Agg        map[string]float64 `json:"agg,omitempty"`
}

func (t *SketchTags) getAggValue(name string) float64 {
	if t.SketchType != SketchMAP || t.Agg == nil {
		return 0
	}
	value, exists := t.Agg[name]
	if !exists {
		return 0
	}
	return value
}

type SketchInput struct {
	ExprID         string     `json:"exprID"`
	OrganizationID string     `json:"organizationID"`
	Timestamp      int64      `json:"timestamp"`
	Frequency      int64      `json:"frequency"` // in seconds
	SketchTags     SketchTags `json:"sketchTags"`
}

type Exemplar struct {
	Timestamp int64          `json:"timestamp"`
	Tags      map[string]any `json:"tags"`
}

func (ex Exemplar) GetTimestamp() int64 { return ex.Timestamp }

func (si SketchInput) GetTimestamp() int64 { return si.Timestamp }

type SketchGroup struct {
	Timestamp int64
	Group     map[string][]SketchInput // key = BaseExpr.ID
}

type ValueKind int

const (
	ValScalar ValueKind = iota
	ValHLL
	ValDDS
	ValMap = 4
)

type Value struct {
	Kind   ValueKind           `json:"-"`
	Num    float64             `json:"num"`
	HLL    *hyperloglog.Sketch `json:"hll,omitempty"`
	DDS    *ddsketch.DDSketch  `json:"dds,omitempty"`
	AggMap map[string]float64  `json:"map,omitempty"`
}

type EvalResult struct {
	Timestamp int64          `json:"timestamp"`
	Tags      map[string]any `json:"tags"`
	Value     Value          `json:"value,omitempty"`
}
