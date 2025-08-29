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

package queryapi

import (
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
	"time"

	"github.com/google/uuid"
)

// PushDownRequest is sent to a worker.
type PushDownRequest struct {
	OrganizationID uuid.UUID       `json:"orgId"`
	BaseExpr       promql.BaseExpr `json:"baseExpr"`
	StartTs        int64           `json:"startTs"`
	EndTs          int64           `json:"endTs"`
	Step           time.Duration   `json:"step"`
	Segments       []SegmentInfo   `json:"segments"`
}

type QuerierService struct {
	mdb             lrdb.StoreFull
	workerDiscovery WorkerDiscovery
}

func NewQuerierService(mdb lrdb.StoreFull, workerDiscovery WorkerDiscovery) (*QuerierService, error) {
	return &QuerierService{mdb: mdb, workerDiscovery: workerDiscovery}, nil
}
