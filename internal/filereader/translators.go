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
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// NoopTranslator returns rows unchanged for high performance.
// This is a code example of the most efficient translator implementation.
type NoopTranslator struct{}

// NewNoopTranslator creates a translator that passes rows through unchanged.
func NewNoopTranslator() *NoopTranslator {
	return &NoopTranslator{}
}

// TranslateRow does nothing for maximum performance.
func (nt *NoopTranslator) TranslateRow(ctx context.Context, row *Row) error {
	// No-op - row is unchanged
	return nil
}

// TagsTranslator adds static tags to every row.
type TagsTranslator struct {
	tags map[string]string
}

// NewTagsTranslator creates a translator that adds the given tags to each row.
func NewTagsTranslator(tags map[string]string) *TagsTranslator {
	return &TagsTranslator{tags: tags}
}

// TranslateRow adds tags to the row in-place.
func (tt *TagsTranslator) TranslateRow(ctx context.Context, row *Row) error {
	for k, v := range tt.tags {
		(*row)[wkk.NewRowKey(k)] = v
	}

	return nil
}

// ChainTranslator applies multiple translators in sequence.
type ChainTranslator struct {
	translators []RowTranslator
}

// NewChainTranslator creates a translator that applies multiple translators in order.
func NewChainTranslator(translators ...RowTranslator) *ChainTranslator {
	return &ChainTranslator{translators: translators}
}

// TranslateRow applies all translators in sequence to the row.
func (ct *ChainTranslator) TranslateRow(ctx context.Context, row *Row) error {
	for i, translator := range ct.translators {
		if err := translator.TranslateRow(ctx, row); err != nil {
			return fmt.Errorf("translator %d failed: %w", i, err)
		}
	}

	return nil
}
