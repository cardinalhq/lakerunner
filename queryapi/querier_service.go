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

package queryapi

import (
	"os"
	"time"

	"github.com/cardinalhq/lakerunner/internal/orgapikey"
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
	BaseExpr   *promql.BaseExpr `json:"baseExpr"`
	LogLeaf    *logql.LogLeaf   `json:"logLeaf"`
	LegacyLeaf *LegacyLeaf      `json:"legacyLeaf,omitempty"` // Direct legacy AST query (bypasses LogQL)
	Limit      int              `json:"limit"`
	Reverse    bool             `json:"reverse"`
	Fields     []string         `json:"fields,omitempty"`

	TagName  string `json:"tagName"`  // Set this to a tag name to get distinct values for that tag
	TagNames bool   `json:"tagNames"` // Set this to true to get distinct tag names (column names) instead of values
	IsSpans  bool   `json:"isSpans"`  // Set this to true for spans queries
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
	apiKeyProvider  orgapikey.OrganizationAPIKeyProvider
	jwtSecretKey    string // Cached JWT secret key from TOKEN_HMAC256_KEY environment variable
}

func NewQuerierService(mdb lrdb.StoreFull, workerDiscovery WorkerDiscovery, apiKeyProvider orgapikey.OrganizationAPIKeyProvider) (*QuerierService, error) {
	// Cache JWT secret key at startup (empty string if not configured)
	jwtSecretKey := os.Getenv("TOKEN_HMAC256_KEY")
	return &QuerierService{
		mdb:             mdb,
		workerDiscovery: workerDiscovery,
		apiKeyProvider:  apiKeyProvider,
		jwtSecretKey:    jwtSecretKey,
	}, nil
}
