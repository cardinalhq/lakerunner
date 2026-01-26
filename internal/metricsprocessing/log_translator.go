// Copyright (C) 2025-2026 CardinalHQ, Inc
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

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// LogTranslator adds resource metadata to log rows
type LogTranslator struct {
	orgID                    string
	bucket                   string
	objectID                 string
	fingerprintTenantManager *fingerprint.TenantManager
}

// NewLogTranslator creates a new LogTranslator with the specified metadata
func NewLogTranslator(orgID, bucket, objectID string, fingerprintTenantManager *fingerprint.TenantManager) *LogTranslator {
	return &LogTranslator{
		orgID:                    orgID,
		bucket:                   bucket,
		objectID:                 objectID,
		fingerprintTenantManager: fingerprintTenantManager,
	}
}

// TranslateRow adds resource fields to each row
func (t *LogTranslator) TranslateRow(ctx context.Context, row *pipeline.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	resourceFile := getResourceFile(t.objectID)
	(*row)[wkk.RowKeyResourceBucketName] = t.bucket
	(*row)[wkk.RowKeyResourceFileName] = "./" + t.objectID
	(*row)[wkk.RowKeyResourceFile] = resourceFile
	(*row)[wkk.RowKeyResourceFileType] = helpers.GetFileType(t.objectID)

	// Extract customer domain if pattern matches
	if customerDomain := helpers.ExtractCustomerDomain(resourceFile); customerDomain != "" {
		(*row)[wkk.RowKeyResourceCustomerDomain] = customerDomain
	}

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

func (t *LogTranslator) setFingerprint(ctx context.Context, row *pipeline.Row) {
	if t.fingerprintTenantManager == nil {
		return
	}

	if _, ok := (*row)[wkk.RowKeyCFingerprint]; ok {
		return // Fingerprint already set
	}

	message, ok := (*row)[wkk.RowKeyCMessage].(string)
	if !ok || message == "" {
		return // No message to fingerprint
	}

	tenant := t.fingerprintTenantManager.GetTenant(t.orgID)
	trieClusterManager := tenant.GetTrieClusterManager()
	fp, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
	if err == nil {
		(*row)[wkk.RowKeyCFingerprint] = fp
	}
}
