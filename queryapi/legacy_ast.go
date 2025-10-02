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
	"encoding/json"
	"fmt"
)

// GraphRequest represents a legacy query API request.
type GraphRequest struct {
	BaseExpressions map[string]BaseExpression `json:"baseExpressions"`
	Formulae        []string                  `json:"formulae,omitempty"`
}

// BaseExpression represents a base query expression in the legacy API.
type BaseExpression struct {
	Dataset       string      `json:"dataset"` // Must be "logs"
	Limit         int         `json:"limit"`
	Order         string      `json:"order"` // "DESC" or "ASC"
	ReturnResults bool        `json:"returnResults"`
	Filter        QueryClause `json:"filter"`
}

// QueryClause is an interface for all filter types.
type QueryClause interface {
	isQueryClause()
}

// Filter represents a single filter condition.
type Filter struct {
	K         string   `json:"k"`        // Label key with dots
	V         []string `json:"v"`        // Values
	Op        string   `json:"op"`       // eq, in, contains, gt, gte, lt, lte, regex
	DataType  string   `json:"dataType"` // string, number, etc.
	Extracted bool     `json:"extracted"`
	Computed  bool     `json:"computed"`
}

func (Filter) isQueryClause() {}

// BinaryClause represents a boolean combination of two clauses.
type BinaryClause struct {
	Q1 QueryClause `json:"q1"`
	Q2 QueryClause `json:"q2"`
	Op string      `json:"op"` // "and" or "or"
}

func (BinaryClause) isQueryClause() {}

// UnmarshalJSON for BaseExpression to handle the QueryClause interface.
func (be *BaseExpression) UnmarshalJSON(data []byte) error {
	type Alias BaseExpression
	aux := &struct {
		Filter json.RawMessage `json:"filter"`
		*Alias
	}{
		Alias: (*Alias)(be),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	clause, err := unmarshalQueryClause(aux.Filter)
	if err != nil {
		return err
	}
	be.Filter = clause

	return nil
}

// unmarshalQueryClause determines the type of QueryClause and unmarshals it.
func unmarshalQueryClause(data json.RawMessage) (QueryClause, error) {
	// Try to determine if it's a Filter or BinaryClause
	var typeCheck map[string]any
	if err := json.Unmarshal(data, &typeCheck); err != nil {
		return nil, err
	}

	// BinaryClause has "q1", "q2", and "op" fields
	if _, hasQ1 := typeCheck["q1"]; hasQ1 {
		var bc BinaryClause
		if err := json.Unmarshal(data, &bc); err != nil {
			return nil, err
		}
		return bc, nil
	}

	// Filter has "k", "v", and "op" fields
	if _, hasK := typeCheck["k"]; hasK {
		var f Filter
		if err := json.Unmarshal(data, &f); err != nil {
			return nil, err
		}
		return f, nil
	}

	return nil, fmt.Errorf("unknown QueryClause type")
}

// UnmarshalJSON for BinaryClause to handle nested QueryClause interfaces.
func (bc *BinaryClause) UnmarshalJSON(data []byte) error {
	aux := &struct {
		Q1 json.RawMessage `json:"q1"`
		Q2 json.RawMessage `json:"q2"`
		Op string          `json:"op"`
	}{}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	q1, err := unmarshalQueryClause(aux.Q1)
	if err != nil {
		return fmt.Errorf("failed to unmarshal q1: %w", err)
	}

	q2, err := unmarshalQueryClause(aux.Q2)
	if err != nil {
		return fmt.Errorf("failed to unmarshal q2: %w", err)
	}

	bc.Q1 = q1
	bc.Q2 = q2
	bc.Op = aux.Op

	return nil
}
