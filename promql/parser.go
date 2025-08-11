package promql

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	promparser "github.com/prometheus/prometheus/promql/parser"
)

//
// ------------------------- AST TYPES -------------------------
//`

type Expr struct {
	Kind      ExprKind           `json:"kind"`
	Selector  *Selector          `json:"selector,omitempty"`
	Range     *RangeExpr         `json:"range,omitempty"`
	Func      *FuncCall          `json:"func,omitempty"`
	Agg       *AggExpr           `json:"agg,omitempty"`
	TopK      *TopKExpr          `json:"topk,omitempty"`
	BottomK   *TopKExpr          `json:"bottomk,omitempty"`
	BinOp     *BinaryExpr        `json:"binop,omitempty"`
	HistQuant *HistogramQuantile `json:"histogramQuantile,omitempty"`
	ClampMin  *ClampMinExpr      `json:"clampMin,omitempty"`
	ClampMax  *ClampMaxExpr      `json:"clampMax,omitempty"` // NEW
}

type ExprKind string

const (
	KindSelector          ExprKind = "selector"
	KindRange             ExprKind = "range"
	KindFunc              ExprKind = "func"
	KindAgg               ExprKind = "agg"
	KindTopK              ExprKind = "topk"
	KindBottomK           ExprKind = "bottomk"
	KindBinary            ExprKind = "binary"
	KindHistogramQuantile ExprKind = "histogram_quantile"
	KindClampMin          ExprKind = "clamp_min"
	KindClampMax          ExprKind = "clamp_max" // NEW
)

// Selector Leaf: metric selector
type Selector struct {
	Metric   string       `json:"metric,omitempty"`
	Matchers []LabelMatch `json:"matchers,omitempty"`
	Offset   string       `json:"offset,omitempty"` // e.g. "5m"
}

type LabelMatch struct {
	Label string  `json:"label"`
	Op    MatchOp `json:"op"` // =, !=, =~, !~
	Value string  `json:"value"`
}

type MatchOp string

const (
	MatchEq  MatchOp = "="
	MatchNe  MatchOp = "!="
	MatchRe  MatchOp = "=~"
	MatchNre MatchOp = "!~"
)

// RangeExpr Range wrapper
type RangeExpr struct {
	Expr         Expr   `json:"expr"`                   // inner instant expr
	Range        string `json:"range"`                  // e.g. "5m"
	SubqueryStep string `json:"subqueryStep,omitempty"` // optional "[range:step]"
}

// FuncCall Functions
type FuncCall struct {
	// Name: "rate" | "irate" | "increase" | "..._over_time" | "scalar"
	// Plus unary math: "abs","ceil","floor","exp","ln","log2","log10","sqrt","sgn"
	Name string   `json:"name"`
	ArgQ *float64 `json:"q,omitempty"`    // for quantile_over_time or scalar(number)
	Expr *Expr    `json:"expr,omitempty"` // for rate/over_time/scalar(expr)/unary math
}

// AggExpr Aggregations
type AggExpr struct {
	Op      AggOp    `json:"op"` // sum|avg|min|max|count
	Expr    Expr     `json:"expr"`
	By      []string `json:"by,omitempty"` // mutually exclusive with Without
	Without []string `json:"without,omitempty"`
}

type AggOp string

const (
	AggSum   AggOp = "sum"
	AggAvg   AggOp = "avg"
	AggMin   AggOp = "min"
	AggMax   AggOp = "max"
	AggCount AggOp = "count"
)

// TopKExpr Top/Bottom K
type TopKExpr struct {
	K    int  `json:"k"`
	Expr Expr `json:"expr"`
}

// BinaryExpr Arithmetic (MVP: + - * /)
type BinaryExpr struct {
	Op    BinOp        `json:"op"`
	LHS   Expr         `json:"lhs"`
	RHS   Expr         `json:"rhs"`
	Match *VectorMatch `json:"match,omitempty"` // optional: on()/ignoring(), group_left/right
}

type BinOp string

const (
	OpAdd BinOp = "+"
	OpSub BinOp = "-"
	OpMul BinOp = "*"
	OpDiv BinOp = "/"
)

type VectorMatch struct {
	On       []string `json:"on,omitempty"`
	Ignoring []string `json:"ignoring,omitempty"`
	Group    string   `json:"group,omitempty"`  // "", "left", "right"
	Labels   []string `json:"labels,omitempty"` // for group_left/right
}

// HistogramQuantile Histogram quantile
type HistogramQuantile struct {
	Q    float64 `json:"q"`
	Expr Expr    `json:"expr"` // usually sum by (le,...)(rate(<metric>_bucket[...]))
}

// ClampMinExpr ClampMin
type ClampMinExpr struct {
	Min  float64 `json:"min"`
	Expr Expr    `json:"expr"`
}

// ClampMaxExpr ClampMax (NEW)
type ClampMaxExpr struct {
	Max  float64 `json:"max"`
	Expr Expr    `json:"expr"`
}

//
// ------------------------- TRANSPILER -------------------------
//

func FromPromQL(q string) (Expr, error) {
	expr, err := promparser.ParseExpr(q)
	if err != nil {
		return Expr{}, err
	}
	return fromNode(expr)
}

func fromNode(n promparser.Node) (Expr, error) {
	switch v := n.(type) {

	case *promparser.VectorSelector:
		return Expr{
			Kind: KindSelector,
			Selector: &Selector{
				Metric:   v.Name,
				Matchers: toMatchers(v.LabelMatchers),
				Offset:   promDur(v.OriginalOffset),
			},
		}, nil

	case *promparser.MatrixSelector:
		vs := v.VectorSelector.(*promparser.VectorSelector)
		inner := Expr{
			Kind: KindSelector,
			Selector: &Selector{
				Metric:   vs.Name,
				Matchers: toMatchers(vs.LabelMatchers),
				Offset:   promDur(vs.OriginalOffset),
			},
		}
		return Expr{
			Kind: KindRange,
			Range: &RangeExpr{
				Expr:  inner,
				Range: promDur(v.Range),
			},
		}, nil

	case *promparser.SubqueryExpr:
		inner, err := fromNode(v.Expr)
		if err != nil {
			return Expr{}, err
		}
		r := RangeExpr{
			Expr:  inner,
			Range: promDur(v.Range),
		}
		if v.Step > 0 {
			r.SubqueryStep = promDur(v.Step)
		}
		return Expr{Kind: KindRange, Range: &r}, nil

	case *promparser.Call:
		fn := v.Func.Name

		// topk / bottomk
		if fn == "topk" || fn == "bottomk" {
			if len(v.Args) != 2 {
				return Expr{}, errUnsupported(fn+" arity", "")
			}
			kn, ok := v.Args[0].(*promparser.NumberLiteral)
			if !ok {
				return Expr{}, errUnsupported(fn+" k", "non-number k")
			}
			inner, err := fromNode(v.Args[1])
			if err != nil {
				return Expr{}, err
			}
			k := int(kn.Val)
			if fn == "topk" {
				return Expr{Kind: KindTopK, TopK: &TopKExpr{K: k, Expr: inner}}, nil
			}
			return Expr{Kind: KindBottomK, BottomK: &TopKExpr{K: k, Expr: inner}}, nil
		}

		// histogram_quantile(q, expr)
		if fn == "histogram_quantile" {
			if len(v.Args) != 2 {
				return Expr{}, errUnsupported("histogram_quantile arity", "")
			}
			qsc, ok := v.Args[0].(*promparser.NumberLiteral)
			if !ok {
				return Expr{}, errUnsupported("histogram_quantile q", "")
			}
			inner, err := fromNode(v.Args[1])
			if err != nil {
				return Expr{}, err
			}
			return Expr{
				Kind:      KindHistogramQuantile,
				HistQuant: &HistogramQuantile{Q: qsc.Val, Expr: inner},
			}, nil
		}

		// clamp_min(expr, min)
		if fn == "clamp_min" {
			if len(v.Args) != 2 {
				return Expr{}, errUnsupported("clamp_min arity", "")
			}
			x, err := fromNode(v.Args[0])
			if err != nil {
				return Expr{}, err
			}
			minNum, ok := v.Args[1].(*promparser.NumberLiteral)
			if !ok {
				return Expr{}, errUnsupported("clamp_min min (must be scalar)", "")
			}
			return Expr{
				Kind:     KindClampMin,
				ClampMin: &ClampMinExpr{Min: minNum.Val, Expr: x},
			}, nil
		}

		// clamp_max(expr, max)  (NEW)
		if fn == "clamp_max" {
			if len(v.Args) != 2 {
				return Expr{}, errUnsupported("clamp_max arity", "")
			}
			x, err := fromNode(v.Args[0])
			if err != nil {
				return Expr{}, err
			}
			maxNum, ok := v.Args[1].(*promparser.NumberLiteral)
			if !ok {
				return Expr{}, errUnsupported("clamp_max max (must be scalar)", "")
			}
			return Expr{
				Kind:     KindClampMax,
				ClampMax: &ClampMaxExpr{Max: maxNum.Val, Expr: x},
			}, nil
		}

		// scalar(...) (PromQL function)
		if fn == "scalar" {
			if len(v.Args) != 1 {
				return Expr{}, errUnsupported("scalar arity", "")
			}
			// If argument is a number literal, encode as scalar-number
			if num, ok := v.Args[0].(*promparser.NumberLiteral); ok {
				q := num.Val
				return Expr{
					Kind: KindFunc,
					Func: &FuncCall{Name: "scalar", ArgQ: &q},
				}, nil
			}
			inner, err := fromNode(v.Args[0])
			if err != nil {
				return Expr{}, err
			}
			return Expr{
				Kind: KindFunc,
				Func: &FuncCall{Name: "scalar", Expr: &inner},
			}, nil
		}

		// Unary math functions: abs, ceil, floor, exp, ln, log2, log10, sqrt, sgn
		switch fn {
		case "abs", "ceil", "floor", "exp", "ln", "log2", "log10", "sqrt", "sgn":
			if len(v.Args) != 1 {
				return Expr{}, errUnsupported(fn+" arity", "")
			}
			argExpr, err := fromNode(v.Args[0])
			if err != nil {
				return Expr{}, err
			}
			return Expr{
				Kind: KindFunc,
				Func: &FuncCall{Name: fn, Expr: &argExpr},
			}, nil
		}

		// rate/irate/increase and *_over_time
		switch fn {
		case "rate", "irate", "increase",
			"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "quantile_over_time":
			var qptr *float64
			var argExpr Expr
			if fn == "quantile_over_time" {
				if len(v.Args) != 2 {
					return Expr{}, errUnsupported("quantile_over_time arity", "")
				}
				qsc, ok := v.Args[0].(*promparser.NumberLiteral)
				if !ok {
					return Expr{}, errUnsupported("quantile_over_time q", "")
				}
				qptr = &qsc.Val
				var err error
				argExpr, err = fromNode(v.Args[1])
				if err != nil {
					return Expr{}, err
				}
			} else {
				if len(v.Args) != 1 {
					return Expr{}, errUnsupported("function arity", fn)
				}
				var err error
				argExpr, err = fromNode(v.Args[0])
				if err != nil {
					return Expr{}, err
				}
			}
			return Expr{
				Kind: KindFunc,
				Func: &FuncCall{Name: fn, ArgQ: qptr, Expr: &argExpr},
			}, nil
		default:
			return Expr{}, errUnsupported("function", fn)
		}

	case *promparser.AggregateExpr:
		inner, err := fromNode(v.Expr)
		if err != nil {
			return Expr{}, err
		}

		// Handle topk/bottomk which are AggregateExpr with a Param (k)
		switch v.Op {
		case promparser.TOPK, promparser.BOTTOMK:
			if v.Param == nil {
				return Expr{}, errUnsupported("topk/bottomk k", "missing param")
			}
			num, ok := v.Param.(*promparser.NumberLiteral)
			if !ok {
				return Expr{}, errUnsupported("topk/bottomk k", "non-number k")
			}
			k := int(num.Val)
			if v.Op == promparser.TOPK {
				return Expr{Kind: KindTopK, TopK: &TopKExpr{K: k, Expr: inner}}, nil
			}
			return Expr{Kind: KindBottomK, BottomK: &TopKExpr{K: k, Expr: inner}}, nil
		}

		// Regular aggregations (sum/avg/min/max/count) with by/without
		a := AggExpr{
			Op:   toAggOp(v.Op),
			Expr: inner,
		}
		if v.Without {
			a.Without = v.Grouping
		} else if len(v.Grouping) > 0 {
			a.By = v.Grouping
		}
		return Expr{Kind: KindAgg, Agg: &a}, nil

	case *promparser.BinaryExpr:
		lhs, err := fromNode(v.LHS)
		if err != nil {
			return Expr{}, err
		}
		rhs, err := fromNode(v.RHS)
		if err != nil {
			return Expr{}, err
		}
		be := BinaryExpr{
			Op:  toBinOp(v.Op),
			LHS: lhs, RHS: rhs,
		}
		if v.VectorMatching != nil {
			m := &VectorMatch{}
			if v.VectorMatching.On {
				m.On = v.VectorMatching.MatchingLabels
			} else {
				m.Ignoring = v.VectorMatching.MatchingLabels
			}
			switch v.VectorMatching.Card {
			case promparser.CardManyToOne:
				m.Group = "left"
				m.Labels = v.VectorMatching.Include
			case promparser.CardOneToMany:
				m.Group = "right"
				m.Labels = v.VectorMatching.Include
			default:
			}
			be.Match = m
		}
		return Expr{Kind: KindBinary, BinOp: &be}, nil

	case *promparser.ParenExpr:
		return fromNode(v.Expr)

	case *promparser.NumberLiteral:
		val := v.Val
		return Expr{
			Kind: KindFunc,
			Func: &FuncCall{Name: "scalar", ArgQ: &val},
		}, nil

	default:
		return Expr{}, errUnsupported("node", fmt.Sprintf("%T", n))
	}
}

// promDur formats time.Duration like PromQL ("1m", "5m", "2h5m") and returns "" if zero.
func promDur(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return model.Duration(d).String()
}

// Strip the synthetic __name__ matcher the parser adds when a metric name is present.
func toMatchers(ms []*labels.Matcher) []LabelMatch {
	out := make([]LabelMatch, 0, len(ms))
	for _, m := range ms {
		if m == nil {
			continue
		}
		if m.Name == "__name__" {
			// Skip; Selector.Metric already carries the metric name.
			continue
		}
		out = append(out, LabelMatch{
			Label: m.Name,
			Op:    toMatchOp(m.Type),
			Value: m.Value,
		})
	}
	return out
}

func toMatchOp(t labels.MatchType) MatchOp {
	switch t {
	case labels.MatchEqual:
		return MatchEq
	case labels.MatchNotEqual:
		return MatchNe
	case labels.MatchRegexp:
		return MatchRe
	case labels.MatchNotRegexp:
		return MatchNre
	default:
		return MatchEq
	}
}

func toAggOp(op promparser.ItemType) AggOp {
	switch op {
	case promparser.SUM:
		return AggSum
	case promparser.AVG:
		return AggAvg
	case promparser.MIN:
		return AggMin
	case promparser.MAX:
		return AggMax
	case promparser.COUNT:
		return AggCount
	default:
		return AggSum
	}
}

func toBinOp(op promparser.ItemType) BinOp {
	switch op {
	case promparser.ADD:
		return OpAdd
	case promparser.SUB:
		return OpSub
	case promparser.MUL:
		return OpMul
	case promparser.DIV:
		return OpDiv
	default:
		return OpAdd
	}
}

type unsupportedError struct{ what, name string }

func (e unsupportedError) Error() string { return "unsupported " + e.what + ": " + e.name }

func errUnsupported(what, name string) error { return unsupportedError{what, name} }

// Pretty-print any AST node (handy while testing)
func toJSON(v any) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
