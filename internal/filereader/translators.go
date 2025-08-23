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

package filereader

import (
	"fmt"
)

// NoopTranslator returns rows unchanged for high performance.
// This is a code example of the most efficient translator implementation.
type NoopTranslator struct{}

// NewNoopTranslator creates a translator that passes rows through unchanged.
func NewNoopTranslator() *NoopTranslator {
	return &NoopTranslator{}
}

// TranslateRow returns the input row unchanged for maximum performance.
func (nt *NoopTranslator) TranslateRow(in Row) (Row, bool, error) {
	return in, true, nil // true = same reference
}

// TagsTranslator adds static tags to every row.
type TagsTranslator struct {
	tags map[string]string
}

// NewTagsTranslator creates a translator that adds the given tags to each row.
func NewTagsTranslator(tags map[string]string) *TagsTranslator {
	return &TagsTranslator{tags: tags}
}

// TranslateRow adds tags to the row.
func (tt *TagsTranslator) TranslateRow(in Row) (Row, bool, error) {
	result := make(Row)
	for k, v := range in {
		result[k] = v
	}

	for k, v := range tt.tags {
		result[k] = v
	}

	return result, false, nil // false = different reference
}

// ChainTranslator applies multiple translators in sequence.
type ChainTranslator struct {
	translators []RowTranslator
}

// NewChainTranslator creates a translator that applies multiple translators in order.
func NewChainTranslator(translators ...RowTranslator) *ChainTranslator {
	return &ChainTranslator{translators: translators}
}

// TranslateRow applies all translators in sequence.
func (ct *ChainTranslator) TranslateRow(in Row) (Row, bool, error) {
	result := in
	anyCopyMade := false // Track if any translator made a copy

	for i, translator := range ct.translators {
		var err error
		var sameRef bool
		result, sameRef, err = translator.TranslateRow(result)
		if err != nil {
			return nil, false, fmt.Errorf("translator %d failed: %w", i, err)
		}

		// If any translator returns a different reference, we've made a copy
		if !sameRef {
			anyCopyMade = true
		}
	}

	// Final result is same reference as input only if no copies were made
	return result, !anyCopyMade, nil
}
