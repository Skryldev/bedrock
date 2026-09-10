package bedrock

import (
        "context"
        "errors"
        "time"

        "github.com/cockroachdb/pebble"
)

// writeBatch implements WriteBatch on top of a pooled pebble.Batch.
// Cache coherence is deferred: pending updates are buffered and applied
// only when Commit succeeds, so a failed Commit leaves the cache
// untouched and consistent with the engine.
type writeBatch struct {
        sb      *Store
        batch   *pebble.Batch
        pending []cacheInval
        owned   bool // true while the caller holds it (not returned to pool)
}

type cacheInval struct {
        op        uint8 // 0=set, 1=delete, 2=deleteRange, 3=setTTL
        key, val  []byte
        end       []byte
        ttlMillis int64
}

// Set appends a put operation.
func (w *writeBatch) Set(key, value []byte) error {
        if err := w.check(key); err != nil {
                return err
        }
        if err := w.batch.Set(key, value, nil); err != nil {
                return mapEngineError("batch.set", key, err)
        }
        w.pending = append(w.pending, cacheInval{op: 0, key: key, val: value})
        return nil
}

// SetWithTTL appends a put operation and pins the hot-cache entry.
func (w *writeBatch) SetWithTTL(key, value []byte, ttl time.Duration) error {
        if err := w.check(key); err != nil {
                return err
        }
        if err := w.batch.Set(key, value, nil); err != nil {
                return mapEngineError("batch.set", key, err)
        }
        w.pending = append(w.pending, cacheInval{op: 3, key: key, val: value, ttlMillis: ttl.Milliseconds()})
        return nil
}

// Delete appends a delete operation (idempotent).
func (w *writeBatch) Delete(key []byte) error {
        if err := w.check(key); err != nil {
                return err
        }
        if err := w.batch.Delete(key, nil); err != nil {
                return mapEngineError("batch.delete", key, err)
        }
        w.pending = append(w.pending, cacheInval{op: 1, key: key})
        return nil
}

// DeleteRange appends a range tombstone for [start, end).
func (w *writeBatch) DeleteRange(start, end []byte) error {
        if len(start) == 0 || len(end) == 0 {
                return ErrInvalidOperation
        }
        if err := w.batch.DeleteRange(start, end, nil); err != nil {
                return mapEngineError("batch.delete_range", start, err)
        }
        w.pending = append(w.pending, cacheInval{op: 2, key: start, end: end})
        return nil
}

// Merge appends a merge operation. The hot-cache entry is invalidated on
// commit: merged values are opaque and cannot be replayed locally.
func (w *writeBatch) Merge(key, value []byte) error {
        if err := w.check(key); err != nil {
                return err
        }
        if err := w.batch.Merge(key, value, nil); err != nil {
                return mapEngineError("batch.merge", key, err)
        }
        w.pending = append(w.pending, cacheInval{op: 1, key: key}) // merged value opaque: invalidate
        return nil
}

func (w *writeBatch) check(key []byte) error {
        if err := w.guard(); err != nil {
                return err
        }
        if len(key) == 0 {
                return ErrEmptyKey
        }
        return nil
}

// Count returns the number of enqueued operations.
func (w *writeBatch) Count() int { return int(w.batch.Count()) }

// Len returns the encoded size of the batch.
func (w *writeBatch) Len() int { return w.batch.Len() }

// Commit atomically applies all enqueued operations, then propagates
// cache invalidations and metrics. Commit leaves the batch reusable via
// Reset; Close must still be called to return it to the pool.
func (w *writeBatch) Commit(ctx context.Context) error {
        if err := w.guard(); err != nil {
                return err
        }
        start := w.sb.metrics.start()
        err := w.batch.Commit(w.sb.writeOpts)
        latency := time.Since(start)

        result := resultOK
        if err != nil {
                result = resultErr
        } else {
                w.applyCache()
                w.sb.metrics.batchOpsTotal.Add(uint64(w.batch.Count()))
        }
        w.sb.metrics.observe(OpKindBatch, result, latency)
        w.sb.afterOp(ctx, "batch.commit", nil, err, latency)
        w.sb.breakerRecord(err)
        w.pending = w.pending[:0] // committed or failed; either way drop
        return mapEngineError("batch.commit", nil, err)
}

// applyCache replays pending invalidations into the hot cache.
func (w *writeBatch) applyCache() {
        c := w.sb.cache
        if c == nil {
                return
        }
        for _, ci := range w.pending {
                switch ci.op {
                case 0:
                        c.set(ci.key, ci.val, 0)
                case 1:
                        c.delete(ci.key)
                case 2:
                        c.deleteRange(ci.key, ci.end)
                case 3:
                        c.set(ci.key, ci.val, time.Duration(ci.ttlMillis)*time.Millisecond)
                }
        }
}

// Reset clears the batch for reuse. Pending cache updates are discarded —
// call Reset only to abandon a batch.
func (w *writeBatch) Reset() {
        w.batch.Reset()
        w.pending = w.pending[:0]
}

// Close returns the batch to the pool. Close after Commit is the normal
// lifecycle; Close without Commit discards the mutations.
func (w *writeBatch) Close() error {
        if !w.owned {
                return nil
        }
        w.owned = false
        w.batch.Reset()
        b := w.batch
        w.batch = nil
        w.sb.batchPool.Put(b)
        return nil
}

var errDetachedBatch = errors.New("bedrock: batch is not owned (closed twice?)")

// guard ensures the batch is usable.
func (w *writeBatch) guard() error {
        if !w.owned || w.batch == nil {
                return errDetachedBatch
        }
        return nil
}
