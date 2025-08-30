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

package pubsub

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// EventParser defines the interface for parsing different types of storage events
type EventParser interface {
	Parse(raw []byte) ([]lrdb.Inqueue, error)
	GetEventType() string
}

// S3EventParser handles AWS S3 events
type S3EventParser struct{}

func (p *S3EventParser) GetEventType() string {
	return "S3"
}

func (p *S3EventParser) Parse(raw []byte) ([]lrdb.Inqueue, error) {
	var evt struct {
		Records []struct {
			S3 struct {
				Bucket struct {
					Name string `json:"name"`
				} `json:"bucket"`
				Object struct {
					Key  string `json:"key"`
					Size int64  `json:"size"`
				} `json:"object"`
			} `json:"s3"`
		} `json:"Records"`
	}

	if err := json.Unmarshal(raw, &evt); err != nil {
		return nil, fmt.Errorf("failed to parse S3 event: %w", err)
	}

	out := make([]lrdb.Inqueue, 0, len(evt.Records))
	for _, rec := range evt.Records {
		item, err := p.parseS3Record(rec.S3.Bucket.Name, rec.S3.Object.Key, rec.S3.Object.Size)
		if err != nil {
			slog.Error("Failed to parse S3 record", slog.Any("error", err))
			continue
		}
		if item != nil {
			out = append(out, *item)
		}
	}
	return out, nil
}

func (p *S3EventParser) parseS3Record(bucketName, key string, size int64) (*lrdb.Inqueue, error) {
	key, err := url.QueryUnescape(key)
	if err != nil {
		return nil, fmt.Errorf("failed to unescape key: %w", err)
	}

	item, err := parseObjectKey(bucketName, key)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return nil, nil
	}

	// Set the file size from S3 event
	item.FileSize = size
	return item, nil
}

// GCPStorageEventParser handles GCP Cloud Storage events
type GCPStorageEventParser struct{}

func (p *GCPStorageEventParser) GetEventType() string {
	return "GCP"
}

func (p *GCPStorageEventParser) Parse(raw []byte) ([]lrdb.Inqueue, error) {
	var evt struct {
		Kind string `json:"kind"`
		Name string `json:"name"`
		ID   string `json:"id"`
	}

	if err := json.Unmarshal(raw, &evt); err != nil {
		return nil, fmt.Errorf("failed to parse GCP storage event: %w", err)
	}

	if evt.Kind != "storage#object" {
		return nil, fmt.Errorf("unexpected GCP event kind: %s", evt.Kind)
	}

	idParts := strings.Split(evt.ID, "/")
	if len(idParts) < 2 {
		return nil, fmt.Errorf("invalid GCP storage event ID format: %s", evt.ID)
	}
	bucketName := idParts[0]

	item, err := parseObjectKey(bucketName, evt.Name)
	if err != nil {
		return nil, err
	}
	if item == nil {
		return []lrdb.Inqueue{}, nil
	}

	slog.Info("Parsed GCP storage event",
		slog.String("bucket", bucketName),
		slog.String("object_key", evt.Name),
		slog.String("telemetry_type", item.Signal),
		slog.String("collector", item.CollectorName),
		slog.String("organization_id", item.OrganizationID.String()))

	return []lrdb.Inqueue{*item}, nil
}

// parseObjectKey is a shared method for parsing object keys regardless of event source
func parseObjectKey(bucketName, key string) (*lrdb.Inqueue, error) {
	if strings.HasSuffix(key, "/") {
		return nil, fmt.Errorf("skipping directory key: %s", key)
	}

	parts := strings.Split(key, "/")
	if parts[0] == "db" {
		return nil, nil
	}

	var orgID uuid.UUID
	var telem, collector string
	var err error

	if parts[0] == "otel-raw" {
		if len(parts) < 4 {
			return nil, fmt.Errorf("unexpected otel-raw key format: %s", key)
		}

		orgID, err = uuid.Parse(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid organization_id %q (key=%s): %w", parts[1], key, err)
		}
		collector = parts[2]

		fname := parts[len(parts)-1]
		telem = fname
		if idx := strings.Index(fname, "_"); idx != -1 {
			telem = fname[:idx]
		}
	} else if parts[0] == "logs-raw" {
		telem = "logs"
	} else if parts[0] == "metrics-raw" {
		telem = "metrics"
	} else if parts[0] == "traces-raw" {
		telem = "traces"
	} else {
		telem = "logs" // Default to logs for unknown prefixes
	}

	return &lrdb.Inqueue{
		OrganizationID: orgID,
		InstanceNum:    -1,
		Bucket:         bucketName,
		ObjectID:       key,
		Signal:         telem,
		CollectorName:  collector,
		FileSize:       0,
	}, nil
}

type EventParserFactory struct{}

func (f *EventParserFactory) NewParser(raw []byte) (EventParser, error) {
	// Try to detect GCP event first
	var gcpEvent struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &gcpEvent); err == nil && gcpEvent.Kind == "storage#object" {
		return &GCPStorageEventParser{}, nil
	}

	// Try to detect S3 event
	var s3Event struct {
		Records []interface{} `json:"Records"`
	}
	if err := json.Unmarshal(raw, &s3Event); err == nil && len(s3Event.Records) > 0 {
		return &S3EventParser{}, nil
	}

	return nil, fmt.Errorf("unable to determine event type from content")
}

func parseS3LikeEvents(raw []byte) ([]lrdb.Inqueue, error) {
	factory := &EventParserFactory{}
	parser, err := factory.NewParser(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}

	items, err := parser.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s event: %w", parser.GetEventType(), err)
	}

	return items, nil
}
