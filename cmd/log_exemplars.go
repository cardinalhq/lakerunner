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
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func processLogsExemplarsDirect(ctx context.Context, organizationID string, exemplars []*exemplar.ExemplarData, mdb lrdb.StoreFull) error {
	orgID, err := uuid.Parse(organizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	slog.Info("Processing logs exemplars",
		"num_exemplars", len(exemplars),
		"organization_id", organizationID)

	records := make([]lrdb.BatchUpsertExemplarLogsParams, 0, len(exemplars))

	for _, exemplar := range exemplars {
		serviceName := exemplar.Attributes["service.name"]
		clusterName := exemplar.Attributes["k8s.cluster.name"]
		namespaceName := exemplar.Attributes["k8s.namespace.name"]
		fingerprintStr := exemplar.Attributes["fingerprint"]
		oldFingerprintStr := exemplar.Attributes["old_fingerprint"]

		if fingerprintStr == "" {
			slog.Warn("Missing fingerprint", "fingerprint", fingerprintStr)
			continue
		}

		fingerprint, err := strconv.ParseInt(fingerprintStr, 10, 64)
		if err != nil {
			slog.Error("Failed to parse fingerprint", "error", err, "fingerprint", fingerprintStr)
			continue
		}
		
		// Parse old fingerprint (0 if not present or invalid)
		var oldFingerprint int64
		if oldFingerprintStr != "" && oldFingerprintStr != "0" {
			oldFingerprint, err = strconv.ParseInt(oldFingerprintStr, 10, 64)
			if err != nil {
				slog.Debug("Failed to parse old fingerprint, using 0", "error", err, "old_fingerprint", oldFingerprintStr)
				oldFingerprint = 0
			}
		}

		var exemplarData any
		if err := json.Unmarshal([]byte(exemplar.Payload), &exemplarData); err != nil {
			slog.Error("Failed to parse exemplar payload", "error", err)
			continue
		}

		serviceIdentifierID, err := upsertServiceIdentifierDirect(ctx, mdb, orgID, serviceName, clusterName, namespaceName)
		if err != nil {
			slog.Error("Failed to upsert service identifier", "error", err)
			continue
		}

		attributesAny := make(map[string]any)
		for k, v := range exemplar.Attributes {
			attributesAny[k] = v
		}

		var exemplarMap map[string]any
		if exemplarDataMap, ok := exemplarData.(map[string]any); ok {
			exemplarMap = exemplarDataMap
		} else {
			exemplarBytes, err := json.Marshal(exemplarData)
			if err != nil {
				slog.Error("Failed to marshal exemplar data", "error", err)
				continue
			}
			if err := json.Unmarshal(exemplarBytes, &exemplarMap); err != nil {
				slog.Error("Failed to convert exemplar data to map", "error", err)
				continue
			}
		}

		record := lrdb.BatchUpsertExemplarLogsParams{
			OrganizationID:      orgID,
			ServiceIdentifierID: serviceIdentifierID,
			Fingerprint:         fingerprint,
			OldFingerprint:      oldFingerprint,
			Attributes:          attributesAny,
			Exemplar:            exemplarMap,
		}
		records = append(records, record)
	}

	if len(records) == 0 {
		return nil
	}

	batchResults := mdb.BatchUpsertExemplarLogs(ctx, records)
	batchResults.QueryRow(func(i int, isNew bool, err error) {
		if err != nil {
			slog.Error("Failed to upsert exemplar log", "error", err, "index", i)
		}
	})

	slog.Info("Processed logs exemplars", "count", len(records))
	return nil
}
