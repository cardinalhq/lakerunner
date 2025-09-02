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
	"fmt"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// MetricTranslator adds resource metadata to metric rows
type MetricTranslator struct {
	OrgID    string
	Bucket   string
	ObjectID string
}

// TranslateRow adds resource fields to each row
// Assumes all other metric fields (including sketches) are properly set by the proto reader
func (t *MetricTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)[wkk.NewRowKey("resource.bucket.name")] = t.Bucket
	(*row)[wkk.NewRowKey("resource.file.name")] = "./" + t.ObjectID
	(*row)[wkk.RowKeyCCustomerID] = t.OrgID
	(*row)[wkk.RowKeyCTelemetryType] = "metrics"

	// Validate required timestamp field - drop row if missing or invalid
	timestamp, ok := (*row)[wkk.RowKeyCTimestamp].(int64)
	if !ok {
		return fmt.Errorf("_cardinalhq.timestamp field is missing or not int64")
	}

	// Truncate timestamp to nearest 10-second interval
	const tenSecondsMs = int64(10000)
	truncatedTimestamp := (timestamp / tenSecondsMs) * tenSecondsMs
	(*row)[wkk.RowKeyCTimestamp] = truncatedTimestamp

	// Compute and add TID field
	if _, nameOk := (*row)[wkk.RowKeyCName].(string); !nameOk {
		return fmt.Errorf("missing or invalid _cardinalhq.name field for TID computation")
	}

	rowMap := pipeline.ToStringMap(*row)
	tid := helpers.ComputeTID(rowMap)
	(*row)[wkk.RowKeyCTID] = tid

	return nil
}
