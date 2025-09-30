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
	"context"
	"fmt"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// LogTranslator adds resource metadata to log rows
type LogTranslator struct {
	orgID             string
	bucket            string
	objectID          string
	exemplarProcessor *exemplars.Processor
}

// NewLogTranslator creates a new LogTranslator with the specified metadata
func NewLogTranslator(orgID, bucket, objectID string, exemplarProcessor *exemplars.Processor) *LogTranslator {
	return &LogTranslator{
		orgID:             orgID,
		bucket:            bucket,
		objectID:          objectID,
		exemplarProcessor: exemplarProcessor,
	}
}

// TranslateRow adds resource fields to each row
func (t *LogTranslator) TranslateRow(ctx context.Context, row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)[wkk.RowKeyResourceBucketName] = t.bucket
	(*row)[wkk.RowKeyResourceFileName] = "./" + t.objectID
	(*row)[wkk.RowKeyResourceFile] = getResourceFile(t.objectID)
	(*row)[wkk.RowKeyResourceFileType] = helpers.GetFileType(t.objectID)

	// Ensure required CardinalhQ fields are set
	(*row)[wkk.RowKeyCTelemetryType] = "logs"
	(*row)[wkk.RowKeyCName] = "log_events"
	(*row)[wkk.RowKeyCValue] = float64(1.0)

	t.setFingerprint(ctx, row)

	return nil
}

func getResourceFile(objectid string) string {
	items := strings.Split(objectid, "/")
	for i, item := range items {
		if strings.EqualFold(item, "Support") && i < len(items)-1 {
			return items[i+1]
		}
	}
	return "unknown"
}

func (t *LogTranslator) setFingerprint(ctx context.Context, row *filereader.Row) {
	if t.exemplarProcessor == nil {
		return
	}

	if _, ok := (*row)[wkk.RowKeyCFingerprint]; ok {
		return // Fingerprint already set
	}

	message, ok := (*row)[wkk.RowKeyCMessage].(string)
	if !ok || message == "" {
		return // No message to fingerprint
	}

	tenant := t.exemplarProcessor.GetTenant(ctx, t.orgID)
	trieClusterManager := tenant.GetTrieClusterManager()
	fingerprint, _, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
	if err == nil {
		(*row)[wkk.RowKeyCFingerprint] = fingerprint
	}
}
