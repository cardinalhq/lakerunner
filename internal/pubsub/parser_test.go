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

package pubsub

import (
	"testing"

	"github.com/google/uuid"
)

func TestParseS3LikeEvents_FileSize(t *testing.T) {
	orgID := uuid.New()
	s3Event := `{
		"Records": [
			{
				"s3": {
					"bucket": {
						"name": "test-bucket"
					},
					"object": {
						"key": "otel-raw/` + orgID.String() + `/test-collector/logs_data.json.gz",
						"size": 2048
					}
				}
			}
		]
	}`

	items, err := parseS3LikeEvents([]byte(s3Event))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(items))
	}

	item := items[0]
	if item.FileSize != 2048 {
		t.Errorf("Expected file size 2048, got %d", item.FileSize)
	}
	if item.Signal != "logs" {
		t.Errorf("Expected telemetry type 'logs', got %s", item.Signal)
	}
	if item.OrganizationID != orgID {
		t.Errorf("Expected org ID %s, got %s", orgID.String(), item.OrganizationID.String())
	}
	if item.CollectorName != "test-collector" {
		t.Errorf("Expected collector name 'test-collector', got %s", item.CollectorName)
	}
}

func TestParseS3LikeEvents_NonOtelRaw(t *testing.T) {
	s3Event := `{
		"Records": [
			{
				"s3": {
					"bucket": {
						"name": "test-bucket"
					},
					"object": {
						"key": "logs-raw/test-file.json.gz",
						"size": 1024
					}
				}
			}
		]
	}`

	items, err := parseS3LikeEvents([]byte(s3Event))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("Expected 1 item, got %d", len(items))
	}

	item := items[0]
	if item.FileSize != 1024 {
		t.Errorf("Expected file size 1024, got %d", item.FileSize)
	}
	if item.Signal != "logs" {
		t.Errorf("Expected telemetry type 'logs', got %s", item.Signal)
	}
	if item.CollectorName != "" {
		t.Errorf("Expected empty collector name, got %s", item.CollectorName)
	}
}

func TestParseS3LikeEvents_SkipsDirectories(t *testing.T) {
	s3Event := `{
		"Records": [
			{
				"s3": {
					"bucket": {
						"name": "test-bucket"
					},
					"object": {
						"key": "logs-raw/directory/",
						"size": 0
					}
				}
			}
		]
	}`

	items, err := parseS3LikeEvents([]byte(s3Event))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(items) != 0 {
		t.Errorf("Expected 0 items (directory should be skipped), got %d", len(items))
	}
}

func TestParseS3LikeEvents_SkipsDatabaseFiles(t *testing.T) {
	s3Event := `{
		"Records": [
			{
				"s3": {
					"bucket": {
						"name": "test-bucket"
					},
					"object": {
						"key": "db/some-database-file.db",
						"size": 5120
					}
				}
			}
		]
	}`

	items, err := parseS3LikeEvents([]byte(s3Event))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(items) != 0 {
		t.Errorf("Expected 0 items (db files should be skipped), got %d", len(items))
	}
}

func TestParseS3LikeEvents_MultipleTelemetryTypes(t *testing.T) {
	s3Event := `{
		"Records": [
			{
				"s3": {
					"bucket": {
						"name": "test-bucket"
					},
					"object": {
						"key": "metrics-raw/test-file.json.gz",
						"size": 512
					}
				}
			},
			{
				"s3": {
					"bucket": {
						"name": "test-bucket"
					},
					"object": {
						"key": "traces-raw/test-file.json.gz",
						"size": 1536
					}
				}
			}
		]
	}`

	items, err := parseS3LikeEvents([]byte(s3Event))
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(items) != 2 {
		t.Fatalf("Expected 2 items, got %d", len(items))
	}

	// First item should be metrics
	if items[0].Signal != "metrics" {
		t.Errorf("Expected first item telemetry type 'metrics', got %s", items[0].Signal)
	}
	if items[0].FileSize != 512 {
		t.Errorf("Expected first item file size 512, got %d", items[0].FileSize)
	}

	// Second item should be traces
	if items[1].Signal != "traces" {
		t.Errorf("Expected second item telemetry type 'traces', got %s", items[1].Signal)
	}
	if items[1].FileSize != 1536 {
		t.Errorf("Expected second item file size 1536, got %d", items[1].FileSize)
	}
}
