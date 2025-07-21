// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

// s3Event is a minimal model of the incoming JSON
type s3Event struct {
	Records []struct {
		S3 struct {
			Bucket struct {
				Name string `json:"name"`
			} `json:"bucket"`
			Object struct {
				Key string `json:"key"`
			} `json:"object"`
		} `json:"s3"`
	} `json:"Records"`
}

func parseS3LikeEvents(raw []byte) ([]lrdb.Inqueue, error) {
	var evt s3Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		return nil, fmt.Errorf("unmarshal event: %w", err)
	}

	out := make([]lrdb.Inqueue, 0, len(evt.Records))
	for _, rec := range evt.Records {
		key := rec.S3.Object.Key
		key, err := url.QueryUnescape(key)
		if err != nil {
			slog.Error("Failed to unescape key", slog.Any("error", err))
			continue
		}
		parts := strings.Split(key, "/")
		if parts[0] == "db" {
			continue // Skip database files
		}
		var orgID uuid.UUID
		var telem, collector string
		if parts[0] == "otel-raw" {
			if len(parts) < 4 {
				slog.Error("Unexpected key format", slog.String("key", key))
				continue
			}

			orgID, err = uuid.Parse(parts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid organization_id %q: %w", parts[1], err)
			}
			collector = parts[2]

			fname := parts[len(parts)-1]
			telem = fname
			if idx := strings.Index(fname, "_"); idx != -1 {
				telem = fname[:idx]
			}
		}

		iq := lrdb.Inqueue{
			OrganizationID: orgID,
			InstanceNum:    -1,
			Bucket:         rec.S3.Bucket.Name,
			ObjectID:       key,
			TelemetryType:  telem,
			CollectorName:  collector,
		}
		out = append(out, iq)
	}
	return out, nil
}
