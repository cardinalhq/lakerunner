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

package metricsprocessing

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/parquet-go/parquet-go"
)

// extractLabelNameMap reads the schema of a parquet file and creates a mapping
// from underscored label names to dotted label names for the legacy API.
func extractLabelNameMap(parquetFile string) ([]byte, error) {
	file, err := os.Open(parquetFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat parquet file: %w", err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	schema := pf.Schema()
	mapping := make(map[string]string)

	// Iterate through all columns in the schema
	for _, field := range schema.Fields() {
		columnName := field.Name()

		// Skip non-label columns (internal parquet metadata, value columns, etc.)
		if !isLabelColumn(columnName) {
			continue
		}

		// If the column name has underscores and looks like it was originally dotted,
		// create a mapping from underscored to dotted
		if strings.Contains(columnName, "_") {
			dottedName := underscoreToDotted(columnName)
			mapping[columnName] = dottedName
		}
	}

	// If we have mappings, serialize to JSON
	if len(mapping) > 0 {
		jsonBytes, err := json.Marshal(mapping)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal label name map: %w", err)
		}
		return jsonBytes, nil
	}

	// Return nil if no mappings found
	return nil, nil
}

// isLabelColumn determines if a column is a label/tag column that should be mapped.
func isLabelColumn(columnName string) bool {
	// Known label prefixes
	labelPrefixes := []string{
		"_cardinalhq_",
		"resource_",
		"log_",
		"metric_",
		"span_",
		"trace_",
	}

	for _, prefix := range labelPrefixes {
		if strings.HasPrefix(columnName, prefix) {
			return true
		}
	}

	// Skip known non-label columns
	nonLabelColumns := []string{
		"_cardinalhq_timestamp",
		"_cardinalhq_message",
		"_cardinalhq_fingerprint",
		"_cardinalhq_trace_id",
		"_cardinalhq_span_id",
	}

	for _, nonLabel := range nonLabelColumns {
		if columnName == nonLabel {
			// These are values, not labels, but they still need mapping
			return true
		}
	}

	return false
}

// underscoreToDotted converts an underscored name to a dotted name.
func underscoreToDotted(underscored string) string {
	// Known prefixes that should be converted
	prefixes := []string{"_cardinalhq_", "resource_", "log_", "metric_", "span_", "trace_"}

	for _, prefix := range prefixes {
		if strings.HasPrefix(underscored, prefix) {
			return strings.ReplaceAll(underscored, "_", ".")
		}
	}

	// If it doesn't match a known prefix, return as-is
	return underscored
}
