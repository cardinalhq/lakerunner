package promql

import (
	"fmt"
	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/pb/sketchpb"
	"github.com/axiomhq/hyperloglog"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/common/model"
	"log/slog"
	"math"
	"strings"
	"time"
)

// LeafNode corresponds to one BaseExpr pushdown (one worker stream).
type LeafNode struct {
	BE BaseExpr
}

func (n *LeafNode) Hints() ExecHints {
	return ExecHints{
		WantTopK:    n.BE.WantTopK,
		WantBottomK: n.BE.WantBottomK,
		WantCount:   n.BE.WantCount,
		WantDDS:     n.BE.WantDDS,
	}
}

func (n *LeafNode) Eval(sg SketchGroup, step time.Duration) map[string]EvalResult {
	rows := sg.Group[n.BE.ID]
	if len(rows) == 0 {
		return map[string]EvalResult{}
	}

	keyFor := func(tags map[string]any) string {
		if len(n.BE.GroupBy) == 0 {
			return "default"
		}
		parts := make([]string, 0, len(n.BE.GroupBy))
		for _, lbl := range n.BE.GroupBy {
			if v, ok := tags[lbl]; ok {
				parts = append(parts, fmt.Sprintf("%s=%v", lbl, v))
			}
		}
		if len(parts) == 0 {
			return "default"
		}
		return strings.Join(parts, ",")
	}

	out := make(map[string]EvalResult, len(rows))
	for _, si := range rows {
		var v Value

		switch si.SketchTags.SketchType {
		case SketchHLL:
			if len(si.SketchTags.Bytes) == 0 {
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			var h hyperloglog.Sketch
			if err := h.UnmarshalBinary(si.SketchTags.Bytes); err != nil {
				slog.Error("failed to unmarshal HLL sketch", "error", err)
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			v = Value{Kind: ValHLL, HLL: &h}

		case SketchDDS:
			if len(si.SketchTags.Bytes) == 0 {
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			var pb sketchpb.DDSketch
			if err := proto.Unmarshal(si.SketchTags.Bytes, &pb); err != nil {
				slog.Error("failed to unmarshal DDSketch proto", "error", err)
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			sk, err := ddsketch.FromProto(&pb)
			if err != nil {
				slog.Error("failed to build DDSketch from proto", "error", err)
				v = Value{Kind: ValScalar, Num: math.NaN()}
				break
			}
			v = Value{Kind: ValDDS, DDS: sk}

		case SketchMAP:
			secs := step.Seconds()
			v = Value{Kind: ValScalar, Num: evalLeafValueWithSecs(n.BE, si, secs)}

		default:
			// Unknown sketch type
			v = Value{Kind: ValScalar, Num: math.NaN()}
		}

		k := keyFor(si.SketchTags.Tags)
		out[k] = EvalResult{
			Timestamp: si.Timestamp,
			Value:     v,
			Tags:      si.SketchTags.Tags,
		}
	}
	return out
}

func rangeSeconds(be BaseExpr, stepSecs float64) float64 {
	if be.Range == "" {
		return stepSecs
	}
	d, err := model.ParseDuration(be.Range) // or your own parser
	if err != nil {
		return math.NaN()
	}
	return time.Duration(d).Seconds()
}

// Same as your evalLeafValue but pass secs to avoid recomputing per-row.
func evalLeafValueWithSecs(be BaseExpr, in SketchInput, secs float64) float64 {
	if in.SketchTags.SketchType != SketchMAP {
		return math.NaN()
	}
	if be.WantCount {
		return in.SketchTags.getAggValue(COUNT)
	}
	rngSecs := rangeSeconds(be, secs)
	switch be.FuncName {
	case "rate":
		if secs <= 0 {
			return math.NaN()
		}
		return in.SketchTags.getAggValue(SUM) / rngSecs
	case "increase", "sum_over_time", "":
		return in.SketchTags.getAggValue(SUM)
	case "avg_over_time":
		sum := in.SketchTags.getAggValue(SUM)
		cnt := in.SketchTags.getAggValue(COUNT)
		if cnt == 0 {
			return math.NaN()
		}
		return sum / cnt
	case "min_over_time":
		return in.SketchTags.getAggValue(MIN)
	case "max_over_time":
		return in.SketchTags.getAggValue(MAX)
	default:
		return math.NaN()
	}
}
