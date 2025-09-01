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
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type qmc struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
	FrequencyMs    int32
	TsRange        pgtype.Range[pgtype.Timestamptz]
}

func qmcFromInqueue(inf lrdb.Inqueue, frequency int32, startTS int64) qmc {
	startTime := time.UnixMilli(startTS).UTC()
	endTime := startTime.Add(time.Duration(frequency) * time.Millisecond)

	return qmc{
		OrganizationID: inf.OrganizationID,
		InstanceNum:    inf.InstanceNum,
		FrequencyMs:    frequency,
		TsRange:        helpers.TimeRange{Start: startTime, End: endTime}.ToPgRange(),
	}
}
