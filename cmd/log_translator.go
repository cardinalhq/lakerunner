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

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// LogTranslator adds resource metadata to log rows
type LogTranslator struct {
	orgID    string
	bucket   string
	objectID string
}

// TranslateRow adds resource fields to each row
// Assumes all other log fields are properly set when the record comes in
func (t *LogTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
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
