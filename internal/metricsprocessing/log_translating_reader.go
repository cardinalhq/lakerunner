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
	"io"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// LogTranslatingReader wraps a reader and applies log-specific transformations.
// Unlike ParquetLogTranslatingReader, this is for non-parquet formats (proto, json, csv).
type LogTranslatingReader struct {
	wrapped                  filereader.Reader
	orgID                    string
	bucket                   string
	objectID                 string
	fingerprintTenantManager *fingerprint.TenantManager
	schema                   *filereader.ReaderSchema
	closed                   bool
	rowCount                 int64
	schemaBuilt              bool
}

var _ filereader.Reader = (*LogTranslatingReader)(nil)

// NewLogTranslatingReader creates a new reader that applies log-specific transformations.
func NewLogTranslatingReader(wrapped filereader.Reader, orgID, bucket, objectID string, fingerprintTenantManager *fingerprint.TenantManager) *LogTranslatingReader {
	// Get source schema from wrapped reader (which should be fully built in its constructor)
	sourceSchema := wrapped.GetSchema()
	if sourceSchema == nil {
		sourceSchema = filereader.NewReaderSchema()
	}

	// Start with a copy of the source schema
	schema := sourceSchema.Copy()

	// Add columns that translateRow adds
	schema.AddColumn(wkk.RowKeyResourceBucketName, wkk.RowKeyResourceBucketName, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyResourceFileName, wkk.RowKeyResourceFileName, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyResourceFile, wkk.RowKeyResourceFile, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyResourceFileType, wkk.RowKeyResourceFileType, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyResourceCustomerDomain, wkk.RowKeyResourceCustomerDomain, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTelemetryType, wkk.RowKeyCTelemetryType, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCName, wkk.RowKeyCName, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCValue, wkk.RowKeyCValue, filereader.DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyCFingerprint, wkk.RowKeyCFingerprint, filereader.DataTypeInt64, true)

	return &LogTranslatingReader{
		wrapped:                  wrapped,
		orgID:                    orgID,
		bucket:                   bucket,
		objectID:                 objectID,
		fingerprintTenantManager: fingerprintTenantManager,
		schema:                   schema,
		schemaBuilt:              true,
	}
}

// Next returns the next batch of transformed log data.
func (r *LogTranslatingReader) Next(ctx context.Context) (*filereader.Batch, error) {
	if r.closed {
		return nil, io.EOF
	}

	// Get batch from wrapped reader
	batch, err := r.wrapped.Next(ctx)
	if err != nil {
		return nil, err
	}

	// Transform each row
	for i := 0; i < batch.Len(); i++ {
		row := batch.Get(i)
		r.translateRow(ctx, &row)
	}

	r.rowCount += int64(batch.Len())
	return batch, nil
}

// translateRow applies log-specific transformations to a row.
func (r *LogTranslatingReader) translateRow(ctx context.Context, row *pipeline.Row) {
	// Set resource fields
	resourceFile := r.getResourceFile()
	(*row)[wkk.RowKeyResourceBucketName] = r.bucket
	(*row)[wkk.RowKeyResourceFileName] = "./" + r.objectID
	(*row)[wkk.RowKeyResourceFile] = resourceFile
	(*row)[wkk.RowKeyResourceFileType] = helpers.GetFileType(r.objectID)

	// Extract customer domain if pattern matches
	var customerDomain string
	if customerDomain = helpers.ExtractCustomerDomain(resourceFile); customerDomain != "" {
		(*row)[wkk.RowKeyResourceCustomerDomain] = customerDomain
	}

	// Ensure required CardinalhQ fields are set
	(*row)[wkk.RowKeyCTelemetryType] = "logs"
	(*row)[wkk.RowKeyCName] = "log_events"
	(*row)[wkk.RowKeyCValue] = float64(1.0)

	// Add fingerprint
	r.setFingerprint(ctx, row)
}

// getResourceFile extracts the resource file name from the object ID.
func (r *LogTranslatingReader) getResourceFile() string {
	items := strings.Split(r.objectID, "/")
	for i, item := range items {
		if strings.EqualFold(item, "Support") && i < len(items)-1 {
			return items[i+1]
		}
	}
	return "unknown"
}

// setFingerprint adds a fingerprint to the row if message is present.
func (r *LogTranslatingReader) setFingerprint(ctx context.Context, row *pipeline.Row) {
	if r.fingerprintTenantManager == nil {
		return
	}

	if _, ok := (*row)[wkk.RowKeyCFingerprint]; ok {
		return // Fingerprint already set
	}

	message, ok := (*row)[wkk.RowKeyCMessage].(string)
	if !ok || message == "" {
		return // No message to fingerprint
	}

	tenant := r.fingerprintTenantManager.GetTenant(r.orgID)
	trieClusterManager := tenant.GetTrieClusterManager()
	fp, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
	if err == nil {
		(*row)[wkk.RowKeyCFingerprint] = fp
	}
}

// GetSchema returns the schema with log-specific columns added.
func (r *LogTranslatingReader) GetSchema() *filereader.ReaderSchema {
	// Schema is built in constructor, just return a copy
	return r.schema.Copy()
}

// Close closes the wrapped reader
func (r *LogTranslatingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.wrapped != nil {
		return r.wrapped.Close()
	}
	return nil
}

// TotalRowsReturned returns the total number of rows successfully returned
func (r *LogTranslatingReader) TotalRowsReturned() int64 {
	return r.rowCount
}
