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
	"fmt"
	"regexp"
)

type compiledMatcher struct {
	label string
	op    MatchOp
	value string
	re    *regexp.Regexp
}

func compileMatchers(matchers []Matcher) ([]compiledMatcher, error) {
	if len(matchers) == 0 {
		return nil, nil
	}

	out := make([]compiledMatcher, 0, len(matchers))
	for i := range matchers {
		m := matchers[i]
		cm := compiledMatcher{
			label: m.Label,
			op:    m.Op,
			value: m.Value,
		}

		switch m.Op {
		case MatchRe, MatchNre:
			// Prom-style label regex semantics are full-string matches.
			pattern := "^(?:" + m.Value + ")$"
			re, err := regexp.Compile(pattern)
			if err != nil {
				return nil, fmt.Errorf("compile matcher[%d] regex: %w", i, err)
			}
			cm.re = re
		}

		out = append(out, cm)
	}
	return out, nil
}

func matchAll(matchers []compiledMatcher, tags map[string]string) bool {
	for i := range matchers {
		if !matchOne(matchers[i], tags) {
			return false
		}
	}
	return true
}

func matchOne(m compiledMatcher, tags map[string]string) bool {
	v := ""
	if tags != nil {
		v = tags[m.label]
	}

	switch m.op {
	case MatchEq:
		return v == m.value
	case MatchNe:
		return v != m.value
	case MatchRe:
		return m.re != nil && m.re.MatchString(v)
	case MatchNre:
		return m.re != nil && !m.re.MatchString(v)
	default:
		return false
	}
}
