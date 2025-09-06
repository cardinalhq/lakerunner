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
	"fmt"
	"github.com/cardinalhq/lakerunner/logql"
	"strings"
)

type RewriteResult struct {
	PromQL string
	Leaves map[string]logql.LogLeaf
}

// RewriteToPromQL turns an LExecNode into a PromQL string and collects the leaves it references.
// You’ll feed the resulting PromQL to your existing evaluator, and implement a SeriesProvider
// that serves the synthetic metrics/labels for those leaf IDs.
func RewriteToPromQL(root logql.LExecNode) (RewriteResult, error) {
	seen := make(map[string]logql.LogLeaf)
	var need = func(l logql.LogLeaf) {
		seen[l.ID] = l
	}

	var w func(logql.LExecNode) (string, error)
	w = func(n logql.LExecNode) (string, error) {
		switch t := n.(type) {
		case *logql.LScalarNode:
			return fmt.Sprintf("%g", t.Value), nil

		case *logql.LLeafNode:
			// Bare leaf → not a number. Either error or pick a default policy.
			return "", fmt.Errorf("bare log selector cannot be rewritten to PromQL without a range aggregation")

		case *logql.LRangeAggNode:
			switch lc := t.Child.(type) {
			case *logql.LLeafNode:
				leaf := lc.Leaf
				need(leaf)

				// Decide which synthetic metric family to use
				var fam, promOp string
				switch strings.ToLower(t.Op) {
				case "count_over_time":
					fam, promOp = SynthLogCount, "increase"
				case "rate":
					fam, promOp = SynthLogCount, "rate"
				case "bytes_over_time":
					fam, promOp = SynthLogBytes, "increase"
				case "bytes_rate":
					fam, promOp = SynthLogBytes, "rate"
				case "min_over_time":
					fam, promOp = SynthLogUnwrap, "min_over_time"
				case "max_over_time":
					fam, promOp = SynthLogUnwrap, "max_over_time"
				case "avg_over_time":
					fam, promOp = SynthLogUnwrap, "avg_over_time"

				default:
					return "", fmt.Errorf("unsupported range agg: %s", t.Op)
				}

				rng := leaf.Range
				if rng == "" {
					return "", fmt.Errorf("missing range on leaf for %s", t.Op)
				}

				selector := fmt.Sprintf(`%s{%s="%s"}`, fam, LeafMatcher, leaf.ID)

				var off string
				if leaf.Offset != "" {
					off = " offset " + leaf.Offset
				}

				return fmt.Sprintf(`%s(%s[%s])%s`, promOp, selector, rng, off), nil

			default:
				return "", fmt.Errorf("range agg child not a leaf: %T", t.Child)
			}

		case *logql.LAggNode:
			child, err := w(t.Child)
			if err != nil {
				return "", err
			}
			byList := make([]string, 0, len(t.By))
			for _, b := range t.By {
				if strings.Contains(b, ".") {
					b = "\"" + b + "\""
				}
				byList = append(byList, b)
			}
			withoutList := make([]string, 0, len(t.Without))
			for _, b := range t.Without {
				if strings.Contains(b, ".") {
					b = "\"" + b + "\""
				}
				withoutList = append(withoutList, b)
			}
			var grp string
			if len(byList) > 0 {
				grp = " by (" + strings.Join(byList, ",") + ")"
			} else if len(withoutList) > 0 {
				grp = " without (" + strings.Join(withoutList, ",") + ")"
			}
			return fmt.Sprintf(`%s%s (%s)`, t.Op, grp, child), nil

		case *logql.LBinOpNode:
			lhs, err := w(t.LHS)
			if err != nil {
				return "", err
			}
			rhs, err := w(t.RHS)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s) %s (%s)", lhs, t.Op, rhs), nil

		default:
			return "", fmt.Errorf("unknown exec node: %T", n)
		}
	}

	prom, err := w(root)
	if err != nil {
		return RewriteResult{}, err
	}
	return RewriteResult{PromQL: prom, Leaves: seen}, nil
}
