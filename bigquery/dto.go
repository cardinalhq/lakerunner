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

package bigquery

type TableSchema struct {
	TableID string            `json:"table_id"` // dataset.table
	Columns map[string]string `json:"columns"`  // col -> type
}

type EdgeDTO struct {
	From       string      `json:"from"`
	To         string      `json:"to"`
	Kind       EdgeKind    `json:"kind"`
	Cols       [][2]string `json:"cols"`
	Confidence float64     `json:"confidence"`
	Constraint string      `json:"constraint,omitempty"`
	Note       string      `json:"note,omitempty"`
}

type TableGraphDTO struct {
	Tables []TableSchema `json:"tables"`
	Edges  []EdgeDTO     `json:"edges"`
}

type RelevantQuestion struct {
	Sql        string  `json:"sql"`
	Question   string  `json:"question"`
	Similarity float64 `json:"similarity"`
}

type ValidationResult struct {
	Cost     float64  `json:"cost"`
	CTEs     []string `json:"ctes"`
	Valid    bool     `json:"valid"`
	ErrorMsg string   `json:"error_msg,omitempty"`
}

type CTE struct {
	Name     string   `json:"name"`
	SQL      string   `json:"sql"`
	Tables   []string `json:"tables"`
	Children []CTE    `json:"children,omitempty"`
	Parents  []CTE    `json:"parents,omitempty"`
}

type CTELineage struct {
	CTEs             []CTE    `json:"ctes"`
	ReferencedTables []string `json:"referenced_tables"`
}
