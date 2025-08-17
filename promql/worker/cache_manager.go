package worker

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sort"
	"sync"
	"time"
)

// DownloadBatchFunc downloads ALL given paths to their target local paths.
type DownloadBatchFunc func(ctx context.Context, paths []string) error

// RowMapper turns the current row into a T.
type RowMapper[T any] func(*sql.Rows) (T, error)

// CacheWrapper coordinates downloads, batch-ingest, queries, and LRU evictions.
// Eviction is periodic (cron-style), not triggered.
type CacheWrapper struct {
	sink       *Sink
	maxRows    int64
	downloader DownloadBatchFunc

	// in-memory presence + LRU tracking
	mu            sync.Mutex
	present       map[string]struct{}        // segmentID -> exists in cache
	lastAccess    map[string]time.Time       // segmentID -> last access
	localPathByID map[string]string          // segmentID -> local file path
	inflight      map[string]*sync.WaitGroup // singleflight per segmentID

	// eviction cron
	interval  time.Duration
	stopEvict context.CancelFunc
	evictWG   sync.WaitGroup
}

// NewCacheWrapper constructs a wrapper and starts the periodic evictor.
// If interval <= 0, a default of 30s is used. maxRows <= 0 disables eviction.
func NewCacheWrapper(sink *Sink, maxRows int64, dl DownloadBatchFunc, interval time.Duration) *CacheWrapper {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	w := &CacheWrapper{
		sink:          sink,
		maxRows:       maxRows,
		downloader:    dl,
		present:       make(map[string]struct{}, 1024),
		lastAccess:    make(map[string]time.Time, 1024),
		localPathByID: make(map[string]string, 1024),
		inflight:      make(map[string]*sync.WaitGroup, 1024),
		interval:      interval,
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.stopEvict = cancel
	w.evictWG.Add(1)
	go w.evictorCron(ctx)
	return w
}

// Close stops the evictor goroutine.
func (w *CacheWrapper) Close() {
	if w.stopEvict != nil {
		w.stopEvict()
	}
	w.evictWG.Wait()
}

// EnsureAndQuery ensures segments are present (download + batch-ingest),
// then executes the query and streams mapper(row) -> T over a typed channel.
// A separate error channel returns a single terminal error (if any).
func EnsureAndQuery[T any](
	ctx context.Context,
	w *CacheWrapper,
	segmentPaths []string, // local file paths
	segmentIDs []string, // must match cache table segment_id
	sqlQuery string,
	args []any,
	mapper RowMapper[T],
) (<-chan T, <-chan error) {
	outCh := make(chan T, 256)
	errCh := make(chan error, 1)

	// validate
	if len(segmentPaths) != len(segmentIDs) {
		defer close(outCh)
		defer close(errCh)
		errCh <- errors.New("segmentPaths and segmentIDs length mismatch")
		return outCh, errCh
	}
	if mapper == nil {
		defer close(outCh)
		defer close(errCh)
		errCh <- errors.New("nil RowMapper")
		return outCh, errCh
	}

	go func() {
		defer close(outCh)
		defer close(errCh)

		now := time.Now()

		// Lists we "own" for this call (we'll download/ingest them).
		var ownedPaths []string
		var ownedIDs []string

		// Wait groups for segments already in-flight by other goroutines.
		waitSet := make(map[*sync.WaitGroup]struct{})

		// Split present vs missing; stamp access; record local paths.
		w.mu.Lock()
		for i, id := range segmentIDs {
			// Remember local path (used for eviction cleanup).
			w.localPathByID[id] = segmentPaths[i]

			if _, ok := w.present[id]; !ok {
				if wg, inflight := w.inflight[id]; inflight {
					waitSet[wg] = struct{}{}
				} else {
					wg := &sync.WaitGroup{}
					wg.Add(1)
					w.inflight[id] = wg
					ownedPaths = append(ownedPaths, segmentPaths[i])
					ownedIDs = append(ownedIDs, id)
				}
			}
			w.lastAccess[id] = now
		}
		w.mu.Unlock()

		// Wait for any segments being ingested by others.
		for wg := range waitSet {
			wg.Wait()
		}

		// Download and ingest the segments we own.
		if len(ownedPaths) > 0 {
			if w.downloader != nil {
				if err := w.downloader(ctx, ownedPaths); err != nil {
					// release in-flight to avoid deadlocks
					w.mu.Lock()
					for _, id := range ownedIDs {
						if wg := w.inflight[id]; wg != nil {
							wg.Done()
							delete(w.inflight, id)
						}
					}
					w.mu.Unlock()
					errCh <- err
					return
				}
			}
			if err := w.sink.IngestParquetBatch(ctx, ownedPaths, ownedIDs); err != nil {
				// release in-flight to avoid deadlocks
				w.mu.Lock()
				for _, id := range ownedIDs {
					if wg := w.inflight[id]; wg != nil {
						wg.Done()
						delete(w.inflight, id)
					}
				}
				w.mu.Unlock()
				errCh <- err
				return
			}
			// Mark present + release in-flight
			w.mu.Lock()
			for _, id := range ownedIDs {
				w.present[id] = struct{}{}
				if wg := w.inflight[id]; wg != nil {
					wg.Done()
					delete(w.inflight, id)
				}
			}
			w.mu.Unlock()
		}

		// Run the query and stream results.
		rows, err := w.sink.db.QueryContext(ctx, sqlQuery, args...)
		if err != nil {
			errCh <- err
			return
		}
		defer rows.Close()

		for rows.Next() {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}
			v, mErr := mapper(rows)
			if mErr != nil {
				errCh <- mErr
				return
			}
			outCh <- v
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			errCh <- rowsErr
			return
		}

		// Refresh access time for all segments used by this query.
		now2 := time.Now()
		w.mu.Lock()
		for _, id := range segmentIDs {
			w.lastAccess[id] = now2
		}
		w.mu.Unlock()
	}()

	return outCh, errCh
}

// -------------------- Eviction (periodic cron) --------------------

func (w *CacheWrapper) evictorCron(ctx context.Context) {
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

func (w *CacheWrapper) maybeEvictOnce(ctx context.Context) {
	if w.maxRows <= 0 {
		return
	}
	over := w.sink.RowCount() - w.maxRows
	if over <= 0 {
		return
	}

	type ent struct {
		id string
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
	batch := make([]string, 0, batchSize)

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

func (w *CacheWrapper) dropSegments(ctx context.Context, segIDs []string) {
	_, _ = w.sink.DeleteSegments(ctx, segIDs)

	w.mu.Lock()
	for _, id := range segIDs {
		// best-effort remove local file
		if p, ok := w.localPathByID[id]; ok {
			_ = os.Remove(p)
			delete(w.localPathByID, id)
		}
		delete(w.present, id)
		delete(w.lastAccess, id)
	}
	w.mu.Unlock()
}
