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

package expressionindex

import "fmt"

// Signal identifies which ingest/query pipeline an expression belongs to.
type Signal string

const (
	SignalMetrics Signal = "metrics"
	SignalLogs    Signal = "logs"
	SignalTraces  Signal = "traces"
)

func (s Signal) valid() bool {
	switch s {
	case SignalMetrics, SignalLogs, SignalTraces:
		return true
	default:
		return false
	}
}

// MatchOp is a label matcher operation.
type MatchOp string

const (
	MatchEq  MatchOp = "="
	MatchNe  MatchOp = "!="
	MatchRe  MatchOp = "=~"
	MatchNre MatchOp = "!~"
)

func (op MatchOp) valid() bool {
	switch op {
	case MatchEq, MatchNe, MatchRe, MatchNre:
		return true
	default:
		return false
	}
}

// Matcher is a normalized matcher independent of PromQL/LogQL packages.
type Matcher struct {
	Label string
	Op    MatchOp
	Value string
}

// Expression is the canonical index entry for ingest-time "may-match" lookup.
type Expression struct {
	ID       string
	Signal   Signal
	Metric   string
	Matchers []Matcher
}

func (e Expression) validate() error {
	if e.ID == "" {
		return fmt.Errorf("expression id is required")
	}
	if !e.Signal.valid() {
		return fmt.Errorf("invalid signal %q", e.Signal)
	}
	for i := range e.Matchers {
		m := e.Matchers[i]
		if m.Label == "" {
			return fmt.Errorf("matcher[%d] label is required", i)
		}
		if !m.Op.valid() {
			return fmt.Errorf("matcher[%d] invalid op %q", i, m.Op)
		}
	}
	return nil
}
