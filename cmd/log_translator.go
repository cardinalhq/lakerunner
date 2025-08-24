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

package cmd

import (
	"fmt"

	"github.com/cardinalhq/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// LogTranslator adds fingerprinting and resource metadata to log rows
type LogTranslator struct {
	orgID              string
	bucket             string
	objectID           string
	trieClusterManager *fingerprinter.TrieClusterManager
}

// TranslateRow adds fingerprint and resource fields to each row
// Assumes all other log fields are properly set when the record comes in
func (t *LogTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Always recalculate fingerprint from log content, ignoring any existing _cardinalhq.fingerprint field
	fingerprint, err := t.calculateFingerprint(*row)
	if err != nil {
		return fmt.Errorf("failed to calculate fingerprint: %w", err)
	}

	// Always set the recalculated fingerprint, overwriting any existing one
	if fingerprint != 0 {
		(*row)["_cardinalhq.fingerprint"] = fingerprint
	} else {
		// Remove existing fingerprint field if we calculated 0 (no valid message found)
		delete(*row, "_cardinalhq.fingerprint")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)["resource.bucket.name"] = t.bucket
	(*row)["resource.file.name"] = "./" + t.objectID
	(*row)["resource.file.type"] = ingestlogs.GetFileType(t.objectID)

	// Ensure required CardinalhQ fields are set
	(*row)["_cardinalhq.telemetry_type"] = "logs"
	(*row)["_cardinalhq.name"] = "log.events"
	(*row)["_cardinalhq.value"] = float64(1)

	return nil
}

// calculateFingerprint computes a fingerprint for the log record
func (t *LogTranslator) calculateFingerprint(row filereader.Row) (int64, error) {
	// Extract the log message/body using the same logic as the original system
	var message string

	// Only look at _cardinalhq.message field
	if val, ok := row["_cardinalhq.message"]; ok {
		if str, ok := val.(string); ok && str != "" {
			message = str
		}
	}

	// If no message found, return 0 (no fingerprint)
	if message == "" {
		return 0, nil
	}

	// Use the same fingerprinter as the original system
	fingerprint, _, _, err := fingerprinter.Fingerprint(message, t.trieClusterManager)
	if err != nil {
		return 0, err
	}

	return fingerprint, nil
}
