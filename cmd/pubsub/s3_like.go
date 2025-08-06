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

		if strings.HasSuffix(key, "/") {
			continue // Skip "directory" keys
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
		} else if parts[0] == "logs-raw" {
			telem = "logs"
		} else if parts[0] == "metrics-raw" {
			telem = "metrics"
		} else if parts[0] == "traces-raw" {
			telem = "traces"
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
