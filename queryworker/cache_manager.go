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

package queryworker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
	"github.com/google/uuid"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DownloadBatchFunc downloads ALL given paths to their target local paths.
type DownloadBatchFunc func(ctx context.Context, storageProfile storageprofile.StorageProfile, keys []string) error

// RowMapper turns the current row into a T.
type RowMapper[T promql.Timestamped] func(queryapi.PushDownRequest, []string, *sql.Rows) (T, error)

type ingestJob struct {
	profile storageprofile.StorageProfile
	paths   []string
	ids     []int64
}

// CacheManager coordinates downloads, batch-ingest, queries, and LRU evictions.
type CacheManager struct {
	sink                   *DDBSink
	maxRows                int64
	downloader             DownloadBatchFunc
	storageProfileProvider storageprofile.StorageProfileProvider

	// in-memory presence + LRU tracking
	mu         sync.RWMutex
	present    map[int64]struct{}  // segmentID -> exists in cache
	lastAccess map[int64]time.Time // segmentID -> last access
	inflight   map[int64]*struct{} // singleflight per segmentID

	ingestQ    chan ingestJob
	stopIngest context.CancelFunc
	ingestWG   sync.WaitGroup
}

const (
	MaxRowsDefault = 1000000
)

func NewCacheManager(dl DownloadBatchFunc, storageProfileProvider storageprofile.StorageProfileProvider) *CacheManager {
	ddb, err := NewDDBSink(context.Background())
	if err != nil {
		slog.Error("Failed to create DuckDB sink", slog.Any("error", err))
		return nil
	}
	w := &CacheManager{
		sink:                   ddb,
		storageProfileProvider: storageProfileProvider,
		maxRows:                MaxRowsDefault,
		downloader:             dl,
		present:                make(map[int64]struct{}, 1024),
		lastAccess:             make(map[int64]time.Time, 1024),
		inflight:               make(map[int64]*struct{}, 1024),
		ingestQ:                make(chan ingestJob, 64),
	}

	ingCtx, ingCancel := context.WithCancel(context.Background())
	w.stopIngest = ingCancel
	w.ingestWG.Add(1)
	go w.ingestLoop(ingCtx)
	return w
}

func (w *CacheManager) Close() {
	if w.stopIngest != nil {
		w.stopIngest()
	}
	w.ingestWG.Wait()
}

func EvaluatePushDown[T promql.Timestamped](
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	userSQL string,
	s3GlobSize int,
	mapper RowMapper[T],
) (<-chan T, error) {
	if len(request.Segments) == 0 {
		return nil, errors.New("no segment paths")
	}
	if mapper == nil {
		return nil, errors.New("nil RowMapper")
	}
	if !strings.Contains(userSQL, "{table}") {
		return nil, errors.New(`userSQL must contain "{table}" placeholder`)
	}

	// Group segments by orgId/instanceNum
	segmentsByOrg := make(map[uuid.UUID]map[int16][]promql.SegmentInfo)
	for _, seg := range request.Segments {
		if _, ok := segmentsByOrg[seg.OrganizationID]; !ok {
			segmentsByOrg[seg.OrganizationID] = make(map[int16][]promql.SegmentInfo)
		}
		if _, ok := segmentsByOrg[seg.OrganizationID][seg.InstanceNum]; !ok {
			segmentsByOrg[seg.OrganizationID][seg.InstanceNum] = []promql.SegmentInfo{}
		}
		segmentsByOrg[seg.OrganizationID][seg.InstanceNum] = append(segmentsByOrg[seg.OrganizationID][seg.InstanceNum], seg)
	}

	outs := make([]<-chan T, 0)

	// Now start putting channels together for every orgId/instanceNum.
	for orgId, instances := range segmentsByOrg {
		for instanceNum, segments := range instances {
			profile, err := w.storageProfileProvider.GetStorageProfileForOrganization(ctx, orgId)
			if err != nil {
				slog.Error("Failed to get storage profile for organization",
					slog.String("orgId", orgId.String()),
					slog.Int("instanceNum", int(instanceNum)),
					slog.Any("error", err))

				return nil, fmt.Errorf("get storage profile for org %s instance %d: %w", orgId.String(), instanceNum, err)
			}

			// Split into cached vs S3
			var s3URIs []string
			var s3LocalPaths []string
			var s3IDs []int64
			var cachedIDs []int64

			for _, seg := range segments {
				objectId := fmt.Sprintf("db/%s/%s/%d/metrics/%s/tbl_%d.parquet", orgId.String(),
					"default",
					seg.DateInt,
					seg.Hour,
					seg.SegmentID)

				w.mu.RLock()
				_, inCache := w.present[seg.SegmentID]
				w.mu.RUnlock()

				if inCache {
					cachedIDs = append(cachedIDs, seg.SegmentID)
				} else {
					bucket := profile.Bucket
					prefix := "s3://" + bucket + "/"
					s3URIs = append(s3URIs, prefix+objectId)
					s3LocalPaths = append(s3LocalPaths, objectId)
					s3IDs = append(s3IDs, seg.SegmentID)
				}
			}

			// Stream uncached segments directly from S3 (one channel per glob).
			s3Channels, err := streamFromS3(ctx, w, request, s3URIs, s3GlobSize, userSQL, mapper)
			if err != nil {
				return nil, fmt.Errorf("stream from S3: %w", err)
			}
			outs = append(outs, s3Channels...)

			// If we have uncached segments, enqueue them for download + batch-ingest.
			if len(s3LocalPaths) > 0 {
				w.enqueueIngest(profile, s3LocalPaths, s3IDs)
			}

			// If we have cached segments, stream them from the cache.
			cachedChannels := streamCached(ctx, w, request, cachedIDs, userSQL, mapper)
			outs = append(outs, cachedChannels...)
		}
	}

	return promql.MergeSorted(ctx, 1024, outs...), nil
}

func streamCached[T promql.Timestamped](ctx context.Context, w *CacheManager,
	request queryapi.PushDownRequest,
	cachedIDs []int64,
	userSQL string, mapper RowMapper[T]) []<-chan T {
	outs := make([]<-chan T, 0)

	if len(cachedIDs) > 0 {
		out := make(chan T, 256)
		outs = append(outs, out)

		// Touch lastAccess for cached segments
		w.mu.Lock()
		now := time.Now()
		for _, id := range cachedIDs {
			w.lastAccess[id] = now
		}
		w.mu.Unlock()

		go func(ids []int64, out chan<- T) {
			defer close(out)

			// Build IN-list: 123,456,...
			idLits := make([]string, len(ids))
			for i, id := range ids {
				idLits[i] = strconv.FormatInt(id, 10)
			}
			inList := strings.Join(idLits, ",")

			// Replace {table} with cached table; replace sentinel "AND true" with segment filter.
			cacheSQL := strings.Replace(userSQL, "{table}", w.sink.table, 1)
			cacheSQL = strings.Replace(cacheSQL, "AND true", "AND segment_id IN ("+inList+")", 1)

			rows, err := w.sink.db.QueryContext(ctx, cacheSQL)
			if err != nil {
				return
			}
			defer func(rows *sql.Rows) {
				err := rows.Close()
				if err != nil {
					slog.Error("Error closing rows", slog.Any("error", err))
				}
			}(rows)

			cols, err := rows.Columns()
			if err != nil {
				slog.Error("failed to get columns", "err", err)
				return
			}

			for rows.Next() {
				select {
				case <-ctx.Done():
					return
				default:
				}
				v, mErr := mapper(request, cols, rows)
				if mErr != nil {
					return
				}
				out <- v
			}
			_ = rows.Err()
		}(cachedIDs, out)
	}
	return outs
}

func streamFromS3[T promql.Timestamped](
	ctx context.Context,
	w *CacheManager,
	request queryapi.PushDownRequest,
	s3URIs []string,
	s3GlobSize int,
	userSQL string,
	mapper RowMapper[T],
) ([]<-chan T, error) {
	if len(s3URIs) == 0 {
		return []<-chan T{}, nil
	}

	batches := chunkStrings(s3URIs, s3GlobSize)
	outs := make([]<-chan T, 0, len(batches))

	for _, uris := range batches {
		out := make(chan T, 256)
		outs = append(outs, out)

		go func(uris []string, out chan<- T) {
			defer close(out)

			quoted := make([]string, len(uris))
			for i := range uris {
				quoted[i] = "'" + escapeSQL(uris[i]) + "'"
			}
			array := "[" + strings.Join(quoted, ", ") + "]"
			src := fmt.Sprintf(`read_parquet(%s, union_by_name=true)`, array)
			sqlReplaced := strings.Replace(userSQL, "{table}", src, 1)

			rows, err := w.sink.db.QueryContext(ctx, sqlReplaced)

			if err != nil {
				return
			}
			defer func(rows *sql.Rows) {
				err := rows.Close()
				if err != nil {
					slog.Error("Error closing rows", slog.Any("error", err))
				}
			}(rows)

			cols, err := rows.Columns()
			if err != nil {
				slog.Error("failed to get columns", "err", err)
				return
			}

			for rows.Next() {
				select {
				case <-ctx.Done():
					return
				default:
				}
				v, mErr := mapper(request, cols, rows)
				if mErr != nil {
					return
				}
				out <- v
			}
			_ = rows.Err()
		}(uris, out)
	}

	// enqueue is done in EvaluatePushDown(...) after ids/paths are known
	return outs, nil
}

// enqueueIngest filters out present/in-flight, marks new IDs in-flight, and queues one job.
func (w *CacheManager) enqueueIngest(storageProfile storageprofile.StorageProfile, paths []string, ids []int64) {
	if len(paths) == 0 || len(paths) != len(ids) {
		return
	}

	// Filter + mark in-flight under lock
	var todoPaths []string
	var todoIDs []int64
	w.mu.Lock()
	for i, id := range ids {
		if _, ok := w.present[id]; ok {
			continue // already cached
		}
		if _, in := w.inflight[id]; in {
			continue // someone else is already ingesting
		}
		w.inflight[id] = &struct{}{}
		todoPaths = append(todoPaths, paths[i])
		todoIDs = append(todoIDs, id)
		w.lastAccess[id] = time.Now()
	}
	w.mu.Unlock()

	if len(todoPaths) == 0 {
		return
	}

	// Non-blocking enqueue; fallback to blocking if buffer full
	select {
	default:
		w.ingestQ <- ingestJob{profile: storageProfile, paths: todoPaths, ids: todoIDs}
	}
}

func (w *CacheManager) ingestLoop(ctx context.Context) {
	defer w.ingestWG.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case job := <-w.ingestQ:
			if len(job.paths) == 0 {
				continue
			}

			// Download (if provided), then ingest; best-effort and per-batch.
			if w.downloader != nil {
				if err := w.downloader(ctx, job.profile, job.paths); err != nil {
					// release inflight on failure
					w.mu.Lock()
					for _, id := range job.ids {
						if wg := w.inflight[id]; wg != nil {
							delete(w.inflight, id)
						}
					}
					w.mu.Unlock()
					continue
				}
			}

			if err := w.sink.IngestParquetBatch(ctx, job.paths, job.ids); err != nil {
				// release inflight on failure
				w.mu.Lock()
				for _, id := range job.ids {
					if wg := w.inflight[id]; wg != nil {
						delete(w.inflight, id)
					}
				}
				w.mu.Unlock()
				continue
			}

			// Mark present, update lastAccess, release inflight, and delete local files
			now := time.Now()
			w.mu.Lock()
			for _, id := range job.ids {
				w.present[id] = struct{}{}
				w.lastAccess[id] = now
				if wg := w.inflight[id]; wg != nil {
					delete(w.inflight, id)
				}
			}
			w.mu.Unlock()

			for _, p := range job.paths {
				_ = os.Remove(p) // best-effort cleanup
			}

			w.maybeEvictOnce(ctx)
		}
	}
}

func chunkStrings(xs []string, size int) [][]string {
	if size <= 0 || len(xs) == 0 {
		return nil
	}
	var out [][]string
	for i := 0; i < len(xs); i += size {
		j := i + size
		if j > len(xs) {
			j = len(xs)
		}
		out = append(out, xs[i:j])
	}
	return out
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
}

func (w *CacheManager) maybeEvictOnce(ctx context.Context) {
	if w.maxRows <= 0 {
		return
	}
	over := w.sink.RowCount() - w.maxRows
	if over <= 0 {
		return
	}

	type ent struct {
		id int64
		at time.Time
	}
	w.mu.Lock()
	lru := make([]ent, 0, len(w.present))
	for id := range w.present {
		lru = append(lru, ent{id: id, at: w.lastAccess[id]})
	}
	w.mu.Unlock()
	if len(lru) == 0 {
		return
	}

	sort.Slice(lru, func(i, j int) bool { return lru[i].at.Before(lru[j].at) })

	const batchSize = 64
	batch := make([]int64, 0, batchSize)

	for _, e := range lru {
		if w.sink.RowCount() <= w.maxRows {
			break
		}
		batch = append(batch, e.id)
		if len(batch) == batchSize {
			w.dropSegments(ctx, batch)
			batch = batch[:0]
		}
	}
	if len(batch) > 0 && w.sink.RowCount() > w.maxRows {
		w.dropSegments(ctx, batch)
	}
}

func (w *CacheManager) dropSegments(ctx context.Context, segIDs []int64) {
	_, _ = w.sink.DeleteSegments(ctx, segIDs)

	w.mu.Lock()
	for _, id := range segIDs {
		delete(w.present, id)
		delete(w.lastAccess, id)
	}
	w.mu.Unlock()
}
