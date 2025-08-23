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

// NoopTranslator is a pass-through translator that returns rows unchanged.
type NoopTranslator struct{}

// NewNoopTranslator creates a new NoopTranslator.
func NewNoopTranslator() *NoopTranslator {
	return &NoopTranslator{}
}

// TranslateRow returns the row unchanged.
func (nt *NoopTranslator) TranslateRow(in Row) (Row, error) {
	return in, nil
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
func (tt *TagsTranslator) TranslateRow(in Row) (Row, error) {
	result := make(Row)
	for k, v := range in {
		result[k] = v
	}

	for k, v := range tt.tags {
		result[k] = v
	}

	return result, nil
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
func (ct *ChainTranslator) TranslateRow(in Row) (Row, error) {
	result := in
	for i, translator := range ct.translators {
		var err error
		result, err = translator.TranslateRow(result)
		if err != nil {
			return nil, fmt.Errorf("translator %d failed: %w", i, err)
		}
	}
	return result, nil
}
