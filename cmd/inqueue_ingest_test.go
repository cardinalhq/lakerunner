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
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func TestBatchOrgSafetyCheck(t *testing.T) {
	tests := []struct {
		name    string
		items   []lrdb.ClaimInqueueWorkBatchRow
		wantErr bool
		errMsg  string
	}{
		{
			name:    "EmptyBatch",
			items:   []lrdb.ClaimInqueueWorkBatchRow{},
			wantErr: false,
		},
		{
			name: "SingleItem",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
			},
			wantErr: false,
		},
		{
			name: "SameOrgBatch",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
			},
			wantErr: false,
		},
		{
			name: "DifferentOrgBatch",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440001")}, // Different org
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
			},
			wantErr: true,
			errMsg:  "batch safety check failed: item 1 has organization ID 550e8400-e29b-41d4-a716-446655440001, expected 550e8400-e29b-41d4-a716-446655440000",
		},
		{
			name: "DifferentOrgAtEnd",
			items: []lrdb.ClaimInqueueWorkBatchRow{
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")},
				{OrganizationID: uuid.MustParse("550e8400-e29b-41d4-a716-446655440002")}, // Different org at end
			},
			wantErr: true,
			errMsg:  "batch safety check failed: item 2 has organization ID 550e8400-e29b-41d4-a716-446655440002, expected 550e8400-e29b-41d4-a716-446655440000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBatchOrganizationConsistency(tt.items)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), "batch safety check failed")
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// concurrentTestStore implements the minimal inqueue operations needed for testing
// and embeds the generated MockStoreFull to satisfy the lrdb.StoreFull interface.
type concurrentTestStore struct {
	*MockStoreFull
	mu    sync.Mutex
	items map[uuid.UUID]*testItem
}

type testItem struct {
	lrdb.Inqueue
	processed bool
}

type nopMetricEstimator struct{}

func (nopMetricEstimator) Get(uuid.UUID, int16, int32) int64 { return 40_000 }

type nopLogEstimator struct{}

func (nopLogEstimator) Get(uuid.UUID, int16) int64 { return 40_000 }

func newConcurrentTestStore(t *testing.T, total int) *concurrentTestStore {
	ctrl := gomock.NewController(t)
	store := &concurrentTestStore{
		MockStoreFull: NewMockStoreFull(ctrl),
		items:         make(map[uuid.UUID]*testItem, total),
	}
	orgID := uuid.New()
	for i := 0; i < total; i++ {
		id := uuid.New()
		store.items[id] = &testItem{
			Inqueue: lrdb.Inqueue{
				ID:             id,
				QueueTs:        time.Now(),
				OrganizationID: orgID,
				InstanceNum:    1,
				Bucket:         "b",
				ObjectID:       fmt.Sprintf("obj-%d", i),
				TelemetryType:  "metrics",
			},
		}
	}
	return store
}

func (s *concurrentTestStore) ClaimInqueueWork(ctx context.Context, params lrdb.ClaimInqueueWorkParams) (lrdb.Inqueue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, it := range s.items {
		if it.TelemetryType == params.TelemetryType && it.ClaimedBy == 0 && !it.processed {
			it.ClaimedBy = params.ClaimedBy
			it.Tries++
			return it.Inqueue, nil
		}
	}
	return lrdb.Inqueue{}, pgx.ErrNoRows
}

func (s *concurrentTestStore) ClaimInqueueWorkBatch(ctx context.Context, params lrdb.ClaimInqueueWorkBatchParams) ([]lrdb.ClaimInqueueWorkBatchRow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		orgID   uuid.UUID
		instNum int16
		found   bool
	)
	// Identify the first available item to determine organization and instance
	for _, it := range s.items {
		if it.TelemetryType == params.TelemetryType && it.ClaimedBy == 0 && !it.processed {
			orgID = it.OrganizationID
			instNum = it.InstanceNum
			found = true
			break
		}
	}
	if !found {
		return nil, nil
	}

	items := make([]lrdb.ClaimInqueueWorkBatchRow, 0, params.BatchCount)
	for _, it := range s.items {
		if it.OrganizationID == orgID && it.InstanceNum == instNum &&
			it.TelemetryType == params.TelemetryType && it.ClaimedBy == 0 && !it.processed {
			it.ClaimedBy = params.WorkerID
			it.Tries++
			items = append(items, lrdb.ClaimInqueueWorkBatchRow{
				ID:             it.ID,
				QueueTs:        it.QueueTs,
				Priority:       it.Priority,
				OrganizationID: it.OrganizationID,
				CollectorName:  it.CollectorName,
				InstanceNum:    it.InstanceNum,
				Bucket:         it.Bucket,
				ObjectID:       it.ObjectID,
				TelemetryType:  it.TelemetryType,
				Tries:          it.Tries,
				ClaimedBy:      it.ClaimedBy,
				ClaimedAt:      it.ClaimedAt,
				FileSize:       it.FileSize,
			})
			if len(items) >= int(params.BatchCount) {
				break
			}
		}
	}
	return items, nil
}

func (s *concurrentTestStore) ReleaseInqueueWork(ctx context.Context, params lrdb.ReleaseInqueueWorkParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[params.ID]; ok && it.ClaimedBy == params.ClaimedBy {
		it.ClaimedBy = 0
	}
	return nil
}

func (s *concurrentTestStore) DeleteInqueueWork(ctx context.Context, params lrdb.DeleteInqueueWorkParams) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if it, ok := s.items[params.ID]; ok && it.ClaimedBy == params.ClaimedBy {
		it.processed = true
	}
	return nil
}

func (s *concurrentTestStore) InqueueJournalDelete(ctx context.Context, params lrdb.InqueueJournalDeleteParams) error {
	return nil
}

func (s *concurrentTestStore) InqueueJournalUpsert(ctx context.Context, params lrdb.InqueueJournalUpsertParams) (bool, error) {
	return true, nil
}

func TestIngestFilesBatchConcurrentWorkers(t *testing.T) {
	os.Setenv("LAKERUNNER_METRICS_BATCH_SIZE", "3")
	defer os.Unsetenv("LAKERUNNER_METRICS_BATCH_SIZE")
	myInstanceID = 1

	setupGlobalMetrics()

	total := 9
	store := newConcurrentTestStore(t, total)

	var mu sync.Mutex
	processed := make(map[uuid.UUID]int)

	singleFx := func(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider,
		mdb lrdb.StoreFull, awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingestDateint int32,
		rpfEstimate int64, loop *IngestLoopContext) error {
		mu.Lock()
		processed[inf.ID]++
		mu.Unlock()
		return nil
	}

	batchFx := func(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider,
		mdb lrdb.StoreFull, awsmanager *awsclient.Manager, items []lrdb.Inqueue, ingestDateint int32,
		rpfEstimate int64, loop *IngestLoopContext) error {
		mu.Lock()
		for _, it := range items {
			processed[it.ID]++
		}
		mu.Unlock()
		return nil
	}

	worker := func() {
		loop := &IngestLoopContext{
			ctx:             context.Background(),
			mdb:             store,
			signal:          "metrics",
			ll:              slog.Default(),
			metricEstimator: nopMetricEstimator{},
			logEstimator:    nopLogEstimator{},
			processedItems:  new(int64),
			lastLogTime:     new(time.Time),
		}
		for {
			backoff, _, err := ingestFilesBatch(context.Background(), loop, singleFx, batchFx)
			if err != nil {
				t.Errorf("ingestFilesBatch error: %v", err)
				return
			}
			if backoff {
				return
			}
		}
	}

	var wg sync.WaitGroup
	workers := 3
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker()
		}()
	}
	wg.Wait()

	if len(processed) != total {
		t.Fatalf("processed %d items, want %d", len(processed), total)
	}
	for id, count := range processed {
		if count != 1 {
			t.Errorf("item %s processed %d times", id, count)
		}
	}
}
