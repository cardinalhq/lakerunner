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

package bigquery

import (
	bq "cloud.google.com/go/bigquery"
	"context"
	"log/slog"
	"os"
)

type AnalystServer struct {
	ProjectID  string
	BQ         *bq.Client
	Graph      *BQGraph
	Ontology   *Ontology
	Candidates []Candidate

	// Embeddings (optional, for GetRelevantQuestions)
	OpenAIKey        string
	OpenAIEmbedModel string // default "text-embedding-3-small"
}

// NewAnalystServer bootstraps and caches RunAll() outputs.
func NewAnalystServer(ctx context.Context, projectID string, datasets []string, topN int) (*AnalystServer, error) {
	// BQ client
	bqc, err := bq.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	// We call your existing RunAll (uses global projectID in your code; refactor if needed)
	cands, onto, g, err := RunAll(ctx, datasets, topN)
	if err != nil {
		_ = bqc.Close()
		return nil, err
	}

	slog.Info("Starting Analyst server...", "project", projectID, "datasets", datasets, "candidates", len(cands), "facts", len(onto.Facts), "dims", len(onto.Dimensions), "nodes", len(g.Nodes), "edges", len(g.Edges))

	s := &AnalystServer{
		ProjectID:  projectID,
		BQ:         bqc,
		Graph:      g,
		Ontology:   onto,
		Candidates: cands,
		OpenAIKey:  os.Getenv("OPENAI_API_KEY"),
		OpenAIEmbedModel: func() string {
			if v := os.Getenv("OPENAI_EMBED_MODEL"); v != "" {
				return v
			}
			return "text-embedding-3-small"
		}(),
	}
	return s, nil
}

func (s *AnalystServer) Close() {
	if s.BQ != nil {
		if err := s.BQ.Close(); err != nil {
			slog.Warn("bq close", "err", err)
		}
	}
}
