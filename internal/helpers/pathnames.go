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


package helpers

import (
	"fmt"
	"path"
	"strconv"

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
		fmt.Sprintf("%d", hour),
		fmt.Sprintf("tbl_%d.parquet", segmentID),
	)
}
