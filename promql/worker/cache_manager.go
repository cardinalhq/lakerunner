package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DownloadBatchFunc downloads ALL given paths to their target local paths.
type DownloadBatchFunc func(ctx context.Context, paths []string) error

// RowMapper turns the current row into a T.
type RowMapper[T any] func(*sql.Rows) (T, error)

// CacheManager coordinates downloads, batch-ingest, queries, and LRU evictions.
// Eviction is periodic (cron-style), not triggered.
type CacheManager struct {
	sink       *Sink
	maxRows    int64
	downloader DownloadBatchFunc

	// in-memory presence + LRU tracking
	mu         sync.RWMutex
	present    map[int64]struct{}        // segmentID -> exists in cache
	lastAccess map[int64]time.Time       // segmentID -> last access
	inflight   map[int64]*sync.WaitGroup // singleflight per segmentID

	// eviction cron
	interval  time.Duration
	stopEvict context.CancelFunc
	evictWG   sync.WaitGroup

	ingestQ    chan ingestJob
	stopIngest context.CancelFunc
	ingestWG   sync.WaitGroup
}

type ingestJob struct {
	paths []string
	ids   []int64
}

// NewCacheManager constructs a wrapper and starts the periodic evictor.
// If interval <= 0, a default of 30s is used. maxRows <= 0 disables eviction.
func NewCacheManager(sink *Sink, maxRows int64, dl DownloadBatchFunc, interval time.Duration) *CacheManager {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	w := &CacheManager{
		sink:       sink,
		maxRows:    maxRows,
		downloader: dl,
		present:    make(map[int64]struct{}, 1024),
		lastAccess: make(map[int64]time.Time, 1024),
		inflight:   make(map[int64]*sync.WaitGroup, 1024),
		interval:   interval,
		ingestQ:    make(chan ingestJob, 64),
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.stopEvict = cancel
	w.evictWG.Add(1)
	go w.evictorCron(ctx)

	ingCtx, ingCancel := context.WithCancel(context.Background())
	w.stopIngest = ingCancel
	w.ingestWG.Add(1)
	go w.ingestLoop(ingCtx)
	return w
}

func (w *CacheManager) Close() {
	if w.stopEvict != nil {
		w.stopEvict()
	}
	if w.stopIngest != nil {
		w.stopIngest()
	}
	w.evictWG.Wait()
	w.ingestWG.Wait()
}

func Query[T any](
	ctx context.Context,
	w *CacheManager,
	segmentPaths []string,
	userSQL string,
	bucket string,
	s3GlobSize int,
	mapper RowMapper[T],
) ([]<-chan T, error) {
	if len(segmentPaths) == 0 {
		return nil, errors.New("no segment paths")
	}
	if mapper == nil {
		return nil, errors.New("nil RowMapper")
	}
	if !strings.Contains(userSQL, "{table}") {
		return nil, errors.New(`userSQL must contain "{table}" placeholder`)
	}
	if s3GlobSize <= 0 {
		s3GlobSize = 64
	}

	// Split into cached vs S3 (derive IDs outside of locks; only presence check needs the lock).
	var s3URIs []string
	var s3LocalPaths []string
	var s3IDs []int64
	var cachedIDs []int64

	for _, p := range segmentPaths {
		id, derr := deriveSegmentIDFromPath(p)
		if derr != nil {
			return nil, fmt.Errorf("derive segment id for %q: %w", p, derr)
		}

		w.mu.RLock()
		_, inCache := w.present[id]
		w.mu.RUnlock()

		if inCache {
			cachedIDs = append(cachedIDs, id)
		} else {
			key := strings.TrimLeft(p, "/")
			s3URIs = append(s3URIs, "s3://"+bucket+"/"+key)
			s3LocalPaths = append(s3LocalPaths, p)
			s3IDs = append(s3IDs, id)
		}
	}

	outs := make([]<-chan T, 0)

	// Stream uncached segments directly from S3 (one channel per glob).
	s3Channels, err := streamFromS3(ctx, w, s3URIs, s3GlobSize, userSQL, mapper)
	if err != nil {
		return nil, fmt.Errorf("stream from S3: %w", err)
	}
	outs = append(outs, s3Channels...)

	// If we have uncached segments, enqueue them for download + batch-ingest.
	if len(s3LocalPaths) > 0 {
		w.enqueueIngest(s3LocalPaths, s3IDs)
	}

	// If we have cached segments, stream them from the cache.
	cachedChannels := streamCached(ctx, w, cachedIDs, userSQL, mapper)
	outs = append(outs, cachedChannels...)

	return outs, nil
}

func streamCached[T any](ctx context.Context, w *CacheManager,
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

			for rows.Next() {
				select {
				case <-ctx.Done():
					return
				default:
				}
				v, mErr := mapper(rows)
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

func streamFromS3[T any](
	ctx context.Context,
	w *CacheManager,
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

			for rows.Next() {
				select {
				case <-ctx.Done():
					return
				default:
				}
				v, mErr := mapper(rows)
				if mErr != nil {
					return
				}
				out <- v
			}
			_ = rows.Err()
		}(uris, out)
	}

	// enqueue is done in Query(...) after ids/paths are known
	return outs, nil
}

// enqueueIngest filters out present/in-flight, marks new IDs in-flight, and queues one job.
func (w *CacheManager) enqueueIngest(paths []string, ids []int64) {
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
		wg := &sync.WaitGroup{}
		wg.Add(1)
		w.inflight[id] = wg
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
		w.ingestQ <- ingestJob{paths: todoPaths, ids: todoIDs}
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
				if err := w.downloader(ctx, job.paths); err != nil {
					// release inflight on failure
					w.mu.Lock()
					for _, id := range job.ids {
						if wg := w.inflight[id]; wg != nil {
							wg.Done()
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
						wg.Done()
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
					wg.Done()
					delete(w.inflight, id)
				}
			}
			w.mu.Unlock()

			for _, p := range job.paths {
				_ = os.Remove(p) // best-effort cleanup
			}
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

// -------------------- Eviction (periodic cron) --------------------

func (w *CacheManager) evictorCron(ctx context.Context) {
	defer w.evictWG.Done()
	t := time.NewTicker(w.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.maybeEvictOnce(ctx)
		}
	}
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

// deriveSegmentIDFromPath extracts the numeric {num} from a basename like
// "tbl_{num}.parquet". Returns an error if the pattern doesn't match or if {num}
// isn't a valid int64.
func deriveSegmentIDFromPath(p string) (int64, error) {
	base := filepath.Base(p)
	if !strings.HasSuffix(base, ".parquet") {
		return 0, fmt.Errorf("segment path %q: not a parquet file", p)
	}
	name := strings.TrimSuffix(base, ".parquet")
	// Expect prefix "tbl_" and numeric suffix
	const pref = "tbl_"
	if !strings.HasPrefix(name, pref) {
		return 0, fmt.Errorf("segment basename %q: expected prefix %q", base, pref)
	}
	numStr := strings.TrimPrefix(name, pref)
	if numStr == "" {
		return 0, fmt.Errorf("segment basename %q: missing numeric id", base)
	}
	n, err := strconv.ParseInt(numStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("segment basename %q: invalid numeric id %q: %w", base, numStr, err)
	}
	return n, nil
}
