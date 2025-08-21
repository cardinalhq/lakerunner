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

package helpers

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

const DBPrefix = "db"

func MakeDBObjectID(
	orgID uuid.UUID,
	collectorName string,
	dateint int32,
	hour int16,
	segmentID int64,
	ttype string,
) string {
	return path.Join(
		DBPrefix,
		orgID.String(),
		collectorName,
		strconv.Itoa(int(dateint)),
		ttype,
		fmt.Sprintf("%02d", hour),
		fmt.Sprintf("tbl_%d.parquet", segmentID),
	)
}

func MakeDBObjectIDbad(
	orgID uuid.UUID,
	dateint int32,
	hour int16,
	segmentID int64,
	ttype string,
) string {
	return path.Join(
		DBPrefix,
		orgID.String(),
		"default",
		strconv.Itoa(int(dateint)),
		ttype,
		fmt.Sprintf("%d", hour),
		fmt.Sprintf("tbl_%d.parquet", segmentID),
	)
}

// ExtractCollectorName extracts the collector name from the object path
// Expected format: otel-raw/{organization_id}/{collector_name}/...
func ExtractCollectorName(objectID string) string {
	if !strings.HasPrefix(objectID, "otel-raw/") {
		return ""
	}

	parts := strings.Split(objectID, "/")
	if len(parts) < 3 {
		return ""
	}

	return parts[2]
}
