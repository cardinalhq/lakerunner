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
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/cardinalhq/lakerunner/queryapi"
)

const (
	ChannelBufferSize = 4096
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
	s3Db                   *duckdbx.S3DB
	maxRows                int64
	downloader             DownloadBatchFunc
	storageProfileProvider storageprofile.StorageProfileProvider
	dataset                string

	// in-memory presence + LRU tracking
	mu         sync.RWMutex
	present    map[int64]struct{}  // segmentID -> exists in cache
	lastAccess map[int64]time.Time // segmentID -> last access
	inflight   map[int64]*struct{} // singleflight per segmentID

	profilesByOrgInstanceNum map[uuid.UUID]map[int16]storageprofile.StorageProfile
	profilesMu               sync.RWMutex // <--- add this

	ingestQ    chan ingestJob
	stopIngest context.CancelFunc
	ingestWG   sync.WaitGroup
}

const (
	MaxRowsDefault = 1000000000 // 1 billion rows (approx 10GB on disk)
)

func NewCacheManager(dl DownloadBatchFunc, dataset string, storageProfileProvider storageprofile.StorageProfileProvider) *CacheManager {
	ddb, err := NewDDBSink(dataset, context.Background())
	if err != nil {
		slog.Error("Failed to create DuckDB sink", slog.Any("error", err))
		return nil
	}
	// TODO: Pass config when available in queryworker
	s3DB, err := duckdbx.NewS3DB("s3", nil)
	if err != nil {
		slog.Error("Failed to create S3 DuckDB database", slog.Any("error", err))
		return nil
	}
	w := &CacheManager{
		sink:                     ddb,
		s3Db:                     s3DB,
		dataset:                  dataset,
		storageProfileProvider:   storageProfileProvider,
		profilesByOrgInstanceNum: make(map[uuid.UUID]map[int16]storageprofile.StorageProfile),
		maxRows:                  MaxRowsDefault,
		downloader:               dl,
		present:                  make(map[int64]struct{}, ChannelBufferSize),
		lastAccess:               make(map[int64]time.Time, ChannelBufferSize),
		inflight:                 make(map[int64]*struct{}, ChannelBufferSize),
		ingestQ:                  make(chan ingestJob, 64),
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

func (w *CacheManager) getProfile(ctx context.Context, orgID uuid.UUID, inst int16) (storageprofile.StorageProfile, error) {
	// Fast path: read under RLock
	w.profilesMu.RLock()
	if byInst, ok := w.profilesByOrgInstanceNum[orgID]; ok {
		if p, ok2 := byInst[inst]; ok2 {
			w.profilesMu.RUnlock()
			return p, nil
		}
	}
	w.profilesMu.RUnlock()

	// Fetch outside locks
	p, err := w.storageProfileProvider.GetStorageProfileForOrganizationAndInstance(ctx, orgID, inst)
	if err != nil {
		return storageprofile.StorageProfile{}, fmt.Errorf(
			"get storage profile for org %s instance %d: %w", orgID.String(), inst, err)
	}

	// Write path with double-check
	w.profilesMu.Lock()
	defer w.profilesMu.Unlock()

	if _, ok := w.profilesByOrgInstanceNum[orgID]; !ok {
		w.profilesByOrgInstanceNum[orgID] = make(map[int16]storageprofile.StorageProfile)
	}
	if existing, ok := w.profilesByOrgInstanceNum[orgID][inst]; ok {
		return existing, nil
	}
	w.profilesByOrgInstanceNum[orgID][inst] = p
	return p, nil
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

	// Group segments by orgId/instanceNum (no profile map mutation here)
	segmentsByOrg := make(map[uuid.UUID]map[int16][]queryapi.SegmentInfo)
	start := time.Now()
	for _, seg := range request.Segments {
		if _, ok := segmentsByOrg[seg.OrganizationID]; !ok {
			segmentsByOrg[seg.OrganizationID] = make(map[int16][]queryapi.SegmentInfo)
		}
		segmentsByOrg[seg.OrganizationID][seg.InstanceNum] =
			append(segmentsByOrg[seg.OrganizationID][seg.InstanceNum], seg)
	}
	metadataCreationTime := time.Since(start)
	slog.Info("Metadata Creation Time", "duration", metadataCreationTime.String())
	start = time.Now()

	outs := make([]<-chan T, 0)

	// Build channels per (org, instance)
	for orgId, instances := range segmentsByOrg {
		for instanceNum, segments := range instances {
			profile, err := w.getProfile(ctx, orgId, instanceNum)
			if err != nil {
				return nil, err
			}

			// Split into cached vs S3
			var s3URIs []string
			var s3LocalPaths []string
			var s3IDs []int64
			var cachedIDs []int64

			//sortSegments(segments, request)

			for _, seg := range segments {
				objectId := fmt.Sprintf("db/%s/%s/%d/%s/%s/tbl_%d.parquet",
					orgId.String(),
					profile.CollectorName,
					seg.DateInt,
					w.dataset,
					seg.Hour,
					seg.SegmentID)

				w.mu.RLock()
				_, inCache := w.present[seg.SegmentID]
				w.mu.RUnlock()

				if inCache {
					cachedIDs = append(cachedIDs, seg.SegmentID)
				} else {
					bucket := profile.Bucket
					var prefix string
					if profile.CloudProvider == "azure" {
						prefix = "azure://" + bucket + "/"
					} else {
						prefix = "s3://" + bucket + "/"
					}
					s3URIs = append(s3URIs, prefix+objectId)
					s3LocalPaths = append(s3LocalPaths, objectId)
					s3IDs = append(s3IDs, seg.SegmentID)
				}
			}

			// Safe logging (len(w.present) under RLock)
			w.mu.RLock()
			numPresent := len(w.present)
			w.mu.RUnlock()
			slog.Info("Segment Stats",
				"numS3", len(s3URIs),
				"numCached", len(cachedIDs),
				"numPresent", numPresent)

			// Stream uncached segments directly from S3 (one channel per glob).
			s3Channels, err := streamFromS3(ctx, w, request,
				profile.Bucket,
				profile.Region,
				profile.Endpoint,
				s3URIs,
				s3GlobSize,
				userSQL,
				mapper)
			if err != nil {
				return nil, fmt.Errorf("stream from S3: %w", err)
			}
			outs = append(outs, s3Channels...)

			// Enqueue uncached segments for background ingest.
			if len(s3LocalPaths) > 0 {
				w.enqueueIngest(profile, s3LocalPaths, s3IDs)
			}

			// Stream cached segments from the cache.
			cachedChannels := streamCached(ctx, w, request, cachedIDs, userSQL, mapper)
			outs = append(outs, cachedChannels...)
		}
	}
	channelCreationTime := time.Since(start)
	slog.Info("Channel Creation Time", "duration", channelCreationTime.String())

	// Merge all sources (cached + S3 batches) in timestamp order.
	return promql.MergeSorted(ctx, ChannelBufferSize, request.Reverse, request.Limit, outs...), nil
}

func streamCached[T promql.Timestamped](ctx context.Context, w *CacheManager,
	request queryapi.PushDownRequest,
	cachedIDs []int64,
	userSQL string, mapper RowMapper[T]) []<-chan T {
	outs := make([]<-chan T, 0)

	if len(cachedIDs) > 0 {
		out := make(chan T, ChannelBufferSize)
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

			idLits := make([]string, len(ids))
			for i, id := range ids {
				idLits[i] = strconv.FormatInt(id, 10)
			}
			inList := strings.Join(idLits, ",")

			// Replace {table} with cached table; replace sentinel "AND true" with segment filter.
			cacheSQL := ""
			if request.LogLeaf != nil || (request.BaseExpr != nil && request.BaseExpr.LogLeaf != nil) {
				cacheBase := fmt.Sprintf("(SELECT * FROM %s WHERE segment_id IN (%s))", w.sink.table, inList)
				cacheSQL = strings.Replace(userSQL, "{table}", cacheBase, 1)
			} else {
				cacheSQL = strings.Replace(userSQL, "{table}", w.sink.table, 1)
				cacheSQL = strings.Replace(cacheSQL, "AND true", "AND segment_id IN ("+inList+")", 1)
			}

			//slog.Info("Querying cached segments", slog.Int("numSegments", len(ids)), slog.String("sql", cacheSQL))
			rows, conn, err := w.sink.db.QueryContext(ctx, cacheSQL)
			if err != nil {
				return
			}
			defer func(rows *sql.Rows) {
				err := rows.Close()
				if err != nil {
					slog.Error("Error closing rows", slog.Any("error", err))
				}
			}(rows)

			defer func(conn *sql.Conn) {
				err := conn.Close()
				if err != nil {
					slog.Error("Error closing connection", slog.Any("error", err))
				}
			}(conn)

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
	bucket string,
	region string,
	endpoint string,
	s3URIs []string,
	s3GlobSize int,
	userSQL string,
	mapper RowMapper[T],
) ([]<-chan T, error) {
	if len(s3URIs) == 0 {
		return []<-chan T{}, nil
	}

	batches := chunkStrings(s3URIs, s3GlobSize)
	//slog.Info("Chunked S3 URIs into batches", slog.Int("batches", len(batches)), slog.Int("incoming", len(s3URIs)))
	outs := make([]<-chan T, 0, len(batches))

	for _, uris := range batches {
		slog.Info("Streaming from S3", slog.Int("uris", len(uris)))
		out := make(chan T, ChannelBufferSize)
		outs = append(outs, out)

		urisCopy := append([]string(nil), uris...) // capture loop var

		go func(out chan<- T) {
			defer close(out)

			// Build read_parquet source
			quoted := make([]string, len(urisCopy))
			for i := range urisCopy {
				quoted[i] = "'" + escapeSQL(urisCopy[i]) + "'"
			}
			array := "[" + strings.Join(quoted, ", ") + "]"
			src := fmt.Sprintf(`read_parquet(%s, union_by_name=true)`, array)

			sqlReplaced := strings.Replace(userSQL, "{table}", src, 1)
			// Lease a per-bucket connection (creates/refreshes S3 secret under the hood)
			start := time.Now()
			conn, release, err := w.s3Db.GetConnection(ctx, bucket, region, endpoint)
			connectionAcquireTime := time.Since(start)
			slog.Info("S3 Connection Acquire Time", "duration", connectionAcquireTime.String(), "bucket", bucket)

			if err != nil {
				slog.Error("GetConnection failed", slog.String("bucket", bucket), slog.Any("error", err))
				return
			}
			// Ensure rows close before releasing the connection
			defer release()

			rows, err := conn.QueryContext(ctx, sqlReplaced)
			if err != nil {
				slog.Error("Query failed", slog.Any("error", err))
				return
			}
			defer func() {
				if err := rows.Close(); err != nil {
					slog.Error("Error closing rows", slog.Any("error", err))
				}
			}()

			cols, err := rows.Columns()
			if err != nil {
				slog.Error("failed to get columns", slog.Any("error", err))
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
					slog.Error("Row mapping failed", slog.Any("error", mErr))
					return
				}
				out <- v
			}
			if err := rows.Err(); err != nil {
				slog.Error("Rows iteration error", slog.Any("error", err))
			}
		}(out)
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
			continue
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

	// Non-blocking enqueue
	w.ingestQ <- ingestJob{profile: storageProfile, paths: todoPaths, ids: todoIDs}
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

			if w.downloader != nil {
				if err := w.downloader(ctx, job.profile, job.paths); err != nil {
					slog.Error("Failed to download S3 objects", "error", err.Error())
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
				slog.Error("Failed to ingest Parquet batch", "error", err.Error())
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
				//slog.Info("Marking segment as present", slog.Int64("segmentID", id))
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

const batchSize = 300

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, `'`, `''`)
}

func (w *CacheManager) maybeEvictOnce(ctx context.Context) {
	if w.maxRows <= 0 {
		return
	}

	over := w.sink.RowCount() - w.maxRows
	usedSizeGB, err := getUsedDiskSizeInGB()
	if err != nil {
		slog.Error("Failed to get used disk size", slog.Any("error", err))
		return
	}
	slog.Info("Cache Status", slog.Int64("rowCount", w.sink.RowCount()), slog.Int64("maxRows", w.maxRows), slog.Int64("overRows", over), slog.Float64("usedDiskGB", usedSizeGB))
	shouldEvict := over > 0 && usedSizeGB >= 8

	if shouldEvict {
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

		batch := make([]int64, 0, batchSize)

		for _, e := range lru {
			//if w.sink.RowCount() <= w.maxRows {
			//	break
			//}
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
}

func (w *CacheManager) dropSegments(ctx context.Context, segIDs []int64) {
	slog.Info("Evicting segments from cache", slog.Int("count", len(segIDs)))
	_, _ = w.sink.DeleteSegments(ctx, segIDs)

	w.mu.Lock()
	for _, id := range segIDs {
		delete(w.present, id)
		delete(w.lastAccess, id)
	}
	w.mu.Unlock()
}

func getUsedDiskSizeInGB() (float64, error) {
	var stat syscall.Statfs_t

	err := syscall.Statfs(".", &stat)
	if err != nil {
		return 0, err
	}
	usedBytes := (stat.Blocks - stat.Bfree) * uint64(stat.Bsize)
	usedGB := float64(usedBytes) / (1024 * 1024 * 1024)
	return usedGB, nil
}
