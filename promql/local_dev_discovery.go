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

package promql

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
)

type LocalDevDiscovery struct {
}

func NewLocalDevDiscovery() *LocalDevDiscovery {
	return &LocalDevDiscovery{}
}

func (d *LocalDevDiscovery) Start(ctx context.Context) error {
	slog.Info("LocalDevDiscovery started")
	return nil
}

func (d *LocalDevDiscovery) Stop() error {
	slog.Info("LocalDevDiscovery stopped")
	return nil
}

func (d *LocalDevDiscovery) GetWorkersForSegments(organizationID uuid.UUID, segmentIDs []int64) ([]SegmentWorkerMapping, error) {
	mappings := make([]SegmentWorkerMapping, len(segmentIDs))
	for i, segmentID := range segmentIDs {
		mappings[i] = SegmentWorkerMapping{
			SegmentID: segmentID,
			Worker: Worker{
				IP:   "127.0.0.1",
				Port: 8080,
			},
		}
	}
	return mappings, nil
}

func (d *LocalDevDiscovery) GetAllWorkers() ([]Worker, error) {

	return []Worker{
		{
			IP:   "127.0.0.1",
			Port: 8080,
		},
	}, nil
}
