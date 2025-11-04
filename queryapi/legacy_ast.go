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

// BinaryClause represents a boolean combination of multiple clauses.
// Despite the name, it can handle N-way AND/OR operations.
type BinaryClause struct {
	Clauses []QueryClause `json:"-"`  // Parsed from q1, q2, q3, ... qN
	Op      string        `json:"op"` // "and" or "or"
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
// Supports arbitrary number of sub-clauses: q1, q2, q3, ..., qN
func (bc *BinaryClause) UnmarshalJSON(data []byte) error {
	// First unmarshal into a map to find all qN keys
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	// Extract the operator
	var opStruct struct {
		Op string `json:"op"`
	}
	if err := json.Unmarshal(data, &opStruct); err != nil {
		return err
	}
	bc.Op = opStruct.Op

	// Find all qN keys and parse them in order
	clauses := make(map[string]QueryClause)
	maxN := 0

	for key, rawClause := range raw {
		if len(key) > 1 && key[0] == 'q' {
			// Try to parse as qN where N is a number
			var n int
			if _, err := fmt.Sscanf(key, "q%d", &n); err == nil {
				if n > maxN {
					maxN = n
				}
				clause, err := unmarshalQueryClause(rawClause)
				if err != nil {
					return fmt.Errorf("failed to unmarshal %s: %w", key, err)
				}
				clauses[key] = clause
			}
		}
	}

	// Build ordered list of clauses from q1, q2, q3, ...
	bc.Clauses = make([]QueryClause, 0, maxN)
	for i := 1; i <= maxN; i++ {
		key := fmt.Sprintf("q%d", i)
		if clause, ok := clauses[key]; ok {
			bc.Clauses = append(bc.Clauses, clause)
		}
	}

	if len(bc.Clauses) == 0 {
		return fmt.Errorf("BinaryClause must have at least one sub-clause")
	}

	return nil
}

// MarshalJSON for BinaryClause to convert the Clauses slice back to q1, q2, q3... format.
func (bc BinaryClause) MarshalJSON() ([]byte, error) {
	// Build a map with op and q1, q2, q3, ... fields
	result := make(map[string]json.RawMessage)

	// Add the operator
	opJSON, err := json.Marshal(bc.Op)
	if err != nil {
		return nil, err
	}
	result["op"] = opJSON

	// Add each clause as q1, q2, q3, ...
	for i, clause := range bc.Clauses {
		clauseJSON, err := json.Marshal(clause)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal clause q%d: %w", i+1, err)
		}
		result[fmt.Sprintf("q%d", i+1)] = clauseJSON
	}

	return json.Marshal(result)
}
