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
	"time"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"

	"github.com/google/uuid"
)

// PushDownRequest is sent to a worker.
type PushDownRequest struct {
	OrganizationID uuid.UUID     `json:"orgId"`
	StartTs        int64         `json:"startTs"`
	EndTs          int64         `json:"endTs"`
	Step           time.Duration `json:"step"`
	Segments       []SegmentInfo `json:"segments"`

	// dataset specific fields
	BaseExpr *promql.BaseExpr `json:"baseExpr"`
	LogLeaf  *logql.LogLeaf   `json:"logLeaf"`
	Limit    int              `json:"limit"`
	Reverse  bool             `json:"reverse"`

	TagName string `json:"tagName"` // Set this to a tag name to get distinct values for that tag
}

func (p *PushDownRequest) ToOrderString() string {
	if p.Reverse {
		return "DESC"
	}
	return "ASC"
}

type QuerierService struct {
	mdb             lrdb.StoreFull
	workerDiscovery WorkerDiscovery
}

func NewQuerierService(mdb lrdb.StoreFull, workerDiscovery WorkerDiscovery) (*QuerierService, error) {
	return &QuerierService{mdb: mdb, workerDiscovery: workerDiscovery}, nil
}
