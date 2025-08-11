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
	OrganizationID string
	Timestamp      int64
	Frequency      int64
	SketchTags     SketchTags
}

type SketchGroup struct {
	Timestamp int64
	Group     map[string][]SketchInput // key = BaseExpr.ID
}

type ValueKind int

const (
	ValScalar ValueKind = iota
	ValHLL
	ValDDS
)

type Value struct {
	Kind ValueKind
	Num  float64
	HLL  *hyperloglog.Sketch
	DDS  *ddsketch.DDSketch
}

type EvalResult struct {
	Timestamp int64
	Tags      map[string]any
	Value     Value
}
