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

import (
	"sort"
	"sync"
)

type idSet map[string]struct{}

func newIDSet() idSet {
	return make(idSet)
}

func (s idSet) add(id string) {
	s[id] = struct{}{}
}

func (s idSet) remove(id string) {
	delete(s, id)
}

func (s idSet) copy() idSet {
	out := make(idSet, len(s))
	for id := range s {
		out[id] = struct{}{}
	}
	return out
}

func (s idSet) empty() bool {
	return len(s) == 0
}

type indexedExpression struct {
	expr       Expression
	matchers   []compiledMatcher
	eqMatchers []compiledMatcher
}

type signalIndex struct {
	all       idSet
	noMetric  idSet
	byMetric  map[string]idSet
	eqPosting map[string]map[string]idSet // label -> value -> expression IDs
}

func newSignalIndex() *signalIndex {
	return &signalIndex{
		all:       newIDSet(),
		noMetric:  newIDSet(),
		byMetric:  make(map[string]idSet),
		eqPosting: make(map[string]map[string]idSet),
	}
}

func (s *signalIndex) deleteIfEmptyMetric(metric string) {
	if ids, ok := s.byMetric[metric]; ok && ids.empty() {
		delete(s.byMetric, metric)
	}
}

func (s *signalIndex) deleteIfEmptyPosting(label string, value string) {
	values, ok := s.eqPosting[label]
	if !ok {
		return
	}
	if ids, ok := values[value]; ok && ids.empty() {
		delete(values, value)
	}
	if len(values) == 0 {
		delete(s.eqPosting, label)
	}
}

// QueryIndex supports fast "may-match" lookups for expressions.
// It is safe for concurrent use.
type QueryIndex struct {
	mu sync.RWMutex

	exprByKey map[string]*indexedExpression
	bySignal  map[Signal]*signalIndex
	totalExpr int
}

func NewQueryIndex() *QueryIndex {
	return &QueryIndex{
		exprByKey: make(map[string]*indexedExpression),
		bySignal:  make(map[Signal]*signalIndex),
	}
}

func (q *QueryIndex) Size() int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.totalExpr
}

// SignalSize returns the number of indexed expressions for a signal.
func (q *QueryIndex) SignalSize(signal Signal) int {
	q.mu.RLock()
	defer q.mu.RUnlock()
	si := q.bySignal[signal]
	if si == nil {
		return 0
	}
	return len(si.all)
}

// HasSignal reports whether at least one expression exists for the signal.
func (q *QueryIndex) HasSignal(signal Signal) bool {
	return q.SignalSize(signal) > 0
}

func (q *QueryIndex) Replace(expressions []Expression) error {
	next := NewQueryIndex()
	for i := range expressions {
		if err := next.Add(expressions[i]); err != nil {
			return err
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.exprByKey = next.exprByKey
	q.bySignal = next.bySignal
	q.totalExpr = next.totalExpr
	return nil
}

func (q *QueryIndex) Add(expr Expression) error {
	if err := expr.validate(); err != nil {
		return err
	}
	compiled, err := compileMatchers(expr.Matchers)
	if err != nil {
		return err
	}

	ix := &indexedExpression{
		expr:     expr,
		matchers: compiled,
	}
	for i := range compiled {
		if compiled[i].op == MatchEq {
			ix.eqMatchers = append(ix.eqMatchers, compiled[i])
		}
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	exprKey := makeExprKey(expr.Signal, expr.ID)
	if existing, ok := q.exprByKey[exprKey]; ok {
		q.removeLocked(existing)
		delete(q.exprByKey, exprKey)
	}

	si := q.bySignal[expr.Signal]
	if si == nil {
		si = newSignalIndex()
		q.bySignal[expr.Signal] = si
	}

	si.all.add(expr.ID)
	if expr.Metric == "" {
		si.noMetric.add(expr.ID)
	} else {
		metricIDs, ok := si.byMetric[expr.Metric]
		if !ok {
			metricIDs = newIDSet()
			si.byMetric[expr.Metric] = metricIDs
		}
		metricIDs.add(expr.ID)
	}

	for i := range ix.eqMatchers {
		m := ix.eqMatchers[i]
		values, ok := si.eqPosting[m.label]
		if !ok {
			values = make(map[string]idSet)
			si.eqPosting[m.label] = values
		}
		ids, ok := values[m.value]
		if !ok {
			ids = newIDSet()
			values[m.value] = ids
		}
		ids.add(expr.ID)
	}

	q.exprByKey[exprKey] = ix
	q.totalExpr = len(q.exprByKey)
	return nil
}

func (q *QueryIndex) Remove(exprID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	removed := false
	for signal := range q.bySignal {
		exprKey := makeExprKey(signal, exprID)
		existing, ok := q.exprByKey[exprKey]
		if !ok {
			continue
		}
		q.removeLocked(existing)
		delete(q.exprByKey, exprKey)
		removed = true
	}
	q.totalExpr = len(q.exprByKey)
	return removed
}

func (q *QueryIndex) removeLocked(ix *indexedExpression) {
	si := q.bySignal[ix.expr.Signal]
	if si == nil {
		return
	}

	si.all.remove(ix.expr.ID)
	if ix.expr.Metric == "" {
		si.noMetric.remove(ix.expr.ID)
	} else {
		if metricIDs, ok := si.byMetric[ix.expr.Metric]; ok {
			metricIDs.remove(ix.expr.ID)
			si.deleteIfEmptyMetric(ix.expr.Metric)
		}
	}

	for i := range ix.eqMatchers {
		m := ix.eqMatchers[i]
		if values, ok := si.eqPosting[m.label]; ok {
			if ids, ok := values[m.value]; ok {
				ids.remove(ix.expr.ID)
				si.deleteIfEmptyPosting(m.label, m.value)
			}
		}
	}

	if si.all.empty() {
		delete(q.bySignal, ix.expr.Signal)
	}
}

// FindCandidates returns expressions that may match a datapoint identified by
// (signal, metric, tags). Output is deterministic (sorted by expression ID).
func (q *QueryIndex) FindCandidates(signal Signal, metric string, tags map[string]string) []Expression {
	q.mu.RLock()
	defer q.mu.RUnlock()

	si := q.bySignal[signal]
	if si == nil {
		return nil
	}

	base := q.baseIDsForMetric(si, metric)
	if len(base) == 0 {
		return nil
	}

	eqMatches := make(map[string]int, len(base))
	for label, values := range si.eqPosting {
		value := ""
		if tags != nil {
			value = tags[label]
		}
		ids, ok := values[value]
		if !ok {
			continue
		}
		for id := range ids {
			if _, keep := base[id]; keep {
				eqMatches[id]++
			}
		}
	}

	out := make([]Expression, 0, len(base))
	for id := range base {
		ix := q.exprByKey[makeExprKey(signal, id)]
		if ix == nil {
			continue
		}
		if len(ix.eqMatchers) > 0 && eqMatches[id] < len(ix.eqMatchers) {
			continue
		}
		if ix.expr.Metric != "" && metric != "" && ix.expr.Metric != metric {
			continue
		}
		if !matchAll(ix.matchers, tags) {
			continue
		}
		out = append(out, ix.expr)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func makeExprKey(signal Signal, exprID string) string {
	return string(signal) + "\x00" + exprID
}

func (q *QueryIndex) baseIDsForMetric(si *signalIndex, metric string) idSet {
	if metric == "" {
		return si.all.copy()
	}

	out := si.noMetric.copy()
	if ids, ok := si.byMetric[metric]; ok {
		for id := range ids {
			out[id] = struct{}{}
		}
	}
	return out
}
