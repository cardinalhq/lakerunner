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

package queryapi

import (
	"context"
	"time"

	"github.com/cardinalhq/lakerunner/promql"
)

// EvalFlowOptions tunes buffering / aggregation behavior.
type EvalFlowOptions struct {
	// NumBuffers is the number of time-buckets the aggregator keeps before flushing
	// (small ring buffer). 3 is a good default.
	NumBuffers int
	// OutBuffer is the channel buffer size for result maps.
	OutBuffer int
}

// EvalFlow connects a stream of SketchInput -> aggregator -> root.Eval,
// and returns a channel of evaluated results (one map per flushed time-bucket).
type EvalFlow struct {
	root promql.ExecNode
	step time.Duration
	agg  *promql.TimeGroupedSketchAggregator

	outBuf int
}

// NewEvalFlow builds a flow for a compiled plan.
// `leaves` are used to build a BaseExpr lookup by ID for the aggregator.
func NewEvalFlow(
	root promql.ExecNode,
	leaves []promql.BaseExpr,
	step time.Duration,
	opts EvalFlowOptions,
) *EvalFlow {
	if opts.NumBuffers <= 0 {
		opts.NumBuffers = 3
	}
	if opts.OutBuffer <= 0 {
		opts.OutBuffer = 256
	}

	// Map BaseExpr.ID -> BaseExpr for fast lookup from SketchInput.
	beByID := make(map[string]promql.BaseExpr, len(leaves))
	for _, be := range leaves {
		beByID[be.ID] = be
	}

	lookup := func(si promql.SketchInput) (promql.BaseExpr, bool) {
		// We expect workers to set si.ExprID (or similar). If your field is named
		// differently, adjust here.
		if be, ok := beByID[si.ExprID]; ok {
			return be, true
		}
		return promql.BaseExpr{}, false
	}

	return &EvalFlow{
		root:   root,
		step:   step,
		agg:    promql.NewTimeGroupedSketchAggregator(opts.NumBuffers, lookup),
		outBuf: opts.OutBuffer,
	}
}

// Run consumes a globally merged time-sorted stream of SketchInput and produces
// a channel of evaluated results (one per flushed time-bucket).
func (f *EvalFlow) Run(
	ctx context.Context,
	in <-chan promql.SketchInput,
) <-chan map[string]promql.EvalResult {
	out := make(chan map[string]promql.EvalResult, f.outBuf)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				// Flush what we have and exit.
				f.flushAll(out)
				return

			case si, ok := <-in:
				if !ok {
					// End of input: flush remaining buckets.
					f.flushAll(out)
					return
				}
				// Add this single item; aggregator may return completed buckets.
				for _, sg := range f.agg.AddBatch([]promql.SketchInput{si}) {
					res := f.root.Eval(sg, f.step)
					if len(res) > 0 {
						out <- res
					}
				}
			}
		}
	}()

	return out
}

func (f *EvalFlow) flushAll(out chan<- map[string]promql.EvalResult) {
	for _, sg := range f.agg.FlushAll() {
		res := f.root.Eval(sg, f.step)
		if len(res) > 0 {
			out <- res
		}
	}
}
