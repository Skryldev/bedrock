package bedrock

import (
        "context"
        "io"
        "sync"
        "sync/atomic"
        "time"

        "github.com/cockroachdb/pebble"
        "github.com/cockroachdb/pebble/bloom"
        "github.com/cockroachdb/pebble/vfs"
        "go.uber.org/zap"
)

// Store is the concrete bedrock implementation. Create instances with
// Open or Repository.Open. Every exported method is safe for concurrent
// use; internal synchronization is atomic-flag based on the hot path.
type Store struct {
        name      string
        db        *pebble.DB
        cfg       *Config
        writeOpts *pebble.WriteOptions

        cache     *hotCache // nil when disabled
        metrics   *MetricsRegistry
        breaker   *CircuitBreaker // nil when disabled
        logger    *zap.Logger
        tracer    Tracer
        tracing   bool
        blockCach *pebble.Cache

        closing   atomic.Bool
        inFlight  sync.WaitGroup
        closeOnce sync.Once
        closeErr  error

        batchPool sync.Pool
        startedAt time.Time
}

// Compile-time interface conformance checks.
var (
        _ ExtendedStore = (*Store)(nil)
        _ Iterator      = (*scanIterator)(nil)
        _ WriteBatch    = (*writeBatch)(nil)
)

// Open creates a store from functional options. The engine directory is
// created if missing. Errors from Pebble are wrapped in *StoreError.
func Open(opts ...Option) (*Store, error) {
        cfg := DefaultConfig("default")
        for _, o := range opts {
                o(cfg)
        }
        return openWithConfig(cfg)
}

// openWithConfig builds the engine from a validated configuration.
func openWithConfig(cfg *Config) (*Store, error) {
        cfg = cfg.clone()
        if cfg.Logger == nil {
                cfg.Logger = defaultLogger()
        }
        if cfg.Tracer == nil {
                cfg.Tracer = NoopTracer
        }
        if cfg.SlowOpThreshold == 0 {
                cfg.SlowOpThreshold = 50 * time.Millisecond // guard against warn-spam
        }
        if err := cfg.Validate(); err != nil {
                return nil, err
        }

        s := &Store{
                name:      cfg.Name,
                cfg:       cfg,
                metrics:   newMetricsRegistry(cfg.Name),
                logger:    cfg.Logger,
                tracer:    cfg.Tracer,
                tracing:   cfg.Tracer != NoopTracer,
                startedAt: time.Now(),
        }
        s.writeOpts = pebble.NoSync
        if cfg.SyncWrites {
                s.writeOpts = pebble.Sync
        }
        if cfg.HotCache.Enabled {
                s.cache = newHotCache(cfg.HotCache)
        }
        if cfg.Circuit.Enabled {
                cb := NewCircuitBreaker(cfg.Circuit)
                cb.OnStateChange = func(from, to CircuitState) {
                        if to == CircuitOpen {
                                s.metrics.circuitOpens.Add(1)
                        }
                        s.logger.Warn("circuit breaker state change",
                                zap.String("from", from.String()), zap.String("to", to.String()))
                }
                s.breaker = cb
        }

        // Engine options.
        blockCach := pebble.NewCache(int64(cfg.CacheSizeMB) << 20)
        s.blockCach = blockCach
        pOpts := &pebble.Options{
                FS:                          vfs.Default,
                Cache:                       blockCach,
                MemTableSize:                uint64(cfg.MemTableSizeMB) << 20,
                MemTableStopWritesThreshold: cfg.MemTableStopWritesThreshold,
                L0CompactionThreshold:       cfg.L0CompactionThreshold,
                L0CompactionFileThreshold:   cfg.L0CompactionFileThreshold,
                LBaseMaxBytes:               cfg.LBaseMaxBytesMB << 20,
                MaxOpenFiles:                cfg.MaxOpenFiles,
                DisableWAL:                  cfg.DisableWAL,
                BytesPerSync:                cfg.BytesPerSync,
                WALBytesPerSync:             cfg.WALBytesPerSync,
                MaxConcurrentCompactions:    func() int { return cfg.MaxConcurrentCompactions },
                Logger:                      pebbleLoggerAdapter{l: cfg.Logger},
                // Filters: bloom filters on all levels except the last, where
                // they are rarely profitable (full scans dominate L6 reads).
                Levels: make([]pebble.LevelOptions, 7),
        }
        for i := range pOpts.Levels {
                lo := pebble.LevelOptions{
                        BlockSize:          32 << 10,
                        Compression:        pebble.SnappyCompression,
                        FilterPolicy:       bloom.FilterPolicy(cfg.BloomBitsPerKey),
                        IndexBlockSize:     256 << 10,
                        BlockSizeThreshold: 90,
                }
                pOpts.Levels[i] = lo
        }

        db, err := pebble.Open(cfg.DataDir, pOpts)
        if err != nil {
                blockCach.Unref()
                if s.cache != nil {
                        s.cache.close()
                }
                return nil, mapEngineError("open", []byte(cfg.DataDir), err)
        }
        s.db = db
        s.batchPool.New = func() any {
                return db.NewBatch()
        }

        s.logger.Info("bedrock opened",
                zap.String("name", s.name),
                zap.String("data_dir", cfg.DataDir),
                zap.Int("cache_mb", cfg.CacheSizeMB),
                zap.Int("memtable_mb", cfg.MemTableSizeMB),
                zap.Bool("sync_writes", cfg.SyncWrites),
                zap.Bool("hot_cache", cfg.HotCache.Enabled),
                zap.Bool("circuit_breaker", cfg.Circuit.Enabled),
        )
        return s, nil
}

// Name returns the logical instance name.
func (s *Store) Name() string { return s.name }

// DataDir returns the engine directory.
func (s *Store) DataDir() string { return s.cfg.DataDir }

// begin registers an in-flight operation or rejects it during shutdown.
func (s *Store) begin() error {
        if s.closing.Load() {
                return ErrShuttingDown
        }
        s.inFlight.Add(1)
        // Re-check after registering to close the race with Close().
        if s.closing.Load() {
                s.inFlight.Done()
                return ErrShuttingDown
        }
        return nil
}

// afterOp centralizes slow-op logging and span completion for point ops.
func (s *Store) afterOp(ctx context.Context, op string, key []byte, err error, latency time.Duration) {
        if s.cfg.SlowOpThreshold >= 0 && latency > s.cfg.SlowOpThreshold {
                s.metrics.observeSlow()
                withCtx(ctx, s.logger, s.cfg.CtxFields).Warn("slow operation",
                        zapOp(op), zapKey(key), zapDur(latency))
        }
        if s.tracing {
                SpanFromContext(ctx).End(err)
        }
}

// startSpan installs a tracing span into ctx when tracing is enabled. The
// span is ended by afterOp (point ops) or iterator Close (scans).
func (s *Store) startSpan(ctx context.Context, op string, key []byte) context.Context {
        if !s.tracing {
                return ctx
        }
        c, sp := s.tracer.StartSpan(ctx, op, key)
        sp.SetAttr("op", op)
        return ctxWithSpan(c, sp)
}

// pebbleLoggerAdapter routes Pebble's internal log stream into the
// configured zap logger so engine events appear in one structured trail.
type pebbleLoggerAdapter struct{ l *zap.Logger }

func (p pebbleLoggerAdapter) Infof(format string, args ...interface{}) {
        p.l.Sugar().Infof(format, args...)
}

func (p pebbleLoggerAdapter) Fatalf(format string, args ...interface{}) {
        p.l.Sugar().Panicf(format, args...) // libraries must not os.Exit
}

// breakerAllow checks the circuit breaker.
func (s *Store) breakerAllow() error {
        if s.breaker == nil {
                return nil
        }
        return s.breaker.Allow()
}

// breakerRecord feeds the circuit breaker. Expected outcomes (nil,
// ErrNotFound) never count as failures.
func (s *Store) breakerRecord(err error) {
        if s.breaker == nil {
                return
        }
        s.breaker.Record(err)
}

// Get implements bedrock.
func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
        if err := s.begin(); err != nil {
                return nil, err
        }
        defer s.inFlight.Done()

        if len(key) == 0 {
                s.metrics.observe(OpKindGet, resultErr, 0)
                return nil, ErrEmptyKey
        }
        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindGet, resultErr, 0)
                return nil, err
        }
        ctx = s.startSpan(ctx, "get", key)
        start := s.metrics.start()

        var val []byte
        var err error
        cacheHit := false
        if s.cache != nil {
                if v, ok := s.cache.get(key); ok {
                        val, cacheHit = v, true
                        s.metrics.observeCache(true)
                } else {
                        s.metrics.observeCache(false)
                }
        }

        if !cacheHit {
                var closer io.Closer
                val, closer, err = s.db.Get(key)
                if err != nil {
                        latency := time.Since(start)
                        if err == pebble.ErrNotFound {
                                s.metrics.observe(OpKindGet, resultNotFound, latency)
                                s.afterOp(ctx, "get", key, nil, latency)
                                s.breakerRecord(nil)
                                return nil, ErrNotFound
                        }
                        s.metrics.observe(OpKindGet, resultErr, latency)
                        s.afterOp(ctx, "get", key, err, latency)
                        s.breakerRecord(err)
                        return nil, mapEngineError("get", key, err)
                }
                // Copy out of engine memory, then release immediately: the
                // closer pins the memtable/SSTable block.
                cp := make([]byte, len(val))
                copy(cp, val)
                val = cp
                _ = closer.Close()
                if s.cache != nil {
                        s.cache.set(key, val, 0)
                }
                s.metrics.bytesRead.Add(uint64(len(val)))
        }

        latency := time.Since(start)
        s.metrics.observe(OpKindGet, resultOK, latency)
        s.afterOp(ctx, "get", key, nil, latency)
        return val, nil
}

// io_Closer removed: use io.Closer directly.

// Set implements bedrock.
func (s *Store) Set(ctx context.Context, key, value []byte) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if len(key) == 0 {
                s.metrics.observe(OpKindSet, resultErr, 0)
                return ErrEmptyKey
        }
        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindSet, resultErr, 0)
                return err
        }
        ctx = s.startSpan(ctx, "set", key)
        start := s.metrics.start()
        err := s.db.Set(key, value, s.writeOpts)
        latency := time.Since(start)

        if err != nil {
                s.metrics.observe(OpKindSet, resultErr, latency)
                s.afterOp(ctx, "set", key, err, latency)
                s.breakerRecord(err)
                return mapEngineError("set", key, err)
        }
        if s.cache != nil {
                s.cache.set(key, value, 0)
        }
        s.metrics.bytesWritten.Add(uint64(len(key) + len(value)))
        s.metrics.observe(OpKindSet, resultOK, latency)
        s.afterOp(ctx, "set", key, nil, latency)
        s.breakerRecord(nil)
        return nil
}

// SetWithTTL implements ExtendedStore: the engine write is permanent; the
// TTL governs hot-cache residency only.
func (s *Store) SetWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if len(key) == 0 {
                s.metrics.observe(OpKindSet, resultErr, 0)
                return ErrEmptyKey
        }
        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindSet, resultErr, 0)
                return err
        }
        ctx = s.startSpan(ctx, "set_ttl", key)
        start := s.metrics.start()
        err := s.db.Set(key, value, s.writeOpts)
        latency := time.Since(start)

        if err != nil {
                s.metrics.observe(OpKindSet, resultErr, latency)
                s.afterOp(ctx, "set_ttl", key, err, latency)
                s.breakerRecord(err)
                return mapEngineError("set_ttl", key, err)
        }
        if s.cache != nil {
                s.cache.set(key, value, ttl)
        }
        s.metrics.bytesWritten.Add(uint64(len(key) + len(value)))
        s.metrics.observe(OpKindSet, resultOK, latency)
        s.afterOp(ctx, "set_ttl", key, nil, latency)
        s.breakerRecord(nil)
        return nil
}

// Merge implements ExtendedStore.
func (s *Store) Merge(ctx context.Context, key, value []byte) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if len(key) == 0 {
                s.metrics.observe(OpKindMerge, resultErr, 0)
                return ErrEmptyKey
        }
        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindMerge, resultErr, 0)
                return err
        }
        ctx = s.startSpan(ctx, "merge", key)
        start := s.metrics.start()
        err := s.db.Merge(key, value, s.writeOpts)
        latency := time.Since(start)

        if err != nil {
                s.metrics.observe(OpKindMerge, resultErr, latency)
                s.afterOp(ctx, "merge", key, err, latency)
                s.breakerRecord(err)
                return mapEngineError("merge", key, err)
        }
        if s.cache != nil {
                s.cache.delete(key) // merged values are opaque; invalidate
        }
        s.metrics.bytesWritten.Add(uint64(len(key) + len(value)))
        s.metrics.observe(OpKindMerge, resultOK, latency)
        s.afterOp(ctx, "merge", key, nil, latency)
        s.breakerRecord(nil)
        return nil
}

// Delete implements bedrock. Deleting a missing key succeeds.
func (s *Store) Delete(ctx context.Context, key []byte) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if len(key) == 0 {
                s.metrics.observe(OpKindDelete, resultErr, 0)
                return ErrEmptyKey
        }
        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindDelete, resultErr, 0)
                return err
        }
        ctx = s.startSpan(ctx, "delete", key)
        start := s.metrics.start()
        err := s.db.Delete(key, s.writeOpts)
        latency := time.Since(start)

        if err != nil {
                s.metrics.observe(OpKindDelete, resultErr, latency)
                s.afterOp(ctx, "delete", key, err, latency)
                s.breakerRecord(err)
                return mapEngineError("delete", key, err)
        }
        if s.cache != nil {
                s.cache.delete(key)
        }
        s.metrics.observe(OpKindDelete, resultOK, latency)
        s.afterOp(ctx, "delete", key, nil, latency)
        s.breakerRecord(nil)
        return nil
}

// Batch implements bedrock: all operations commit atomically.
func (s *Store) Batch(ctx context.Context, ops []Operation) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if len(ops) == 0 {
                s.metrics.observe(OpKindBatch, resultErr, 0)
                return ErrEmptyBatch
        }
        if s.cfg.MaxBatchOps > 0 && len(ops) > s.cfg.MaxBatchOps {
                s.metrics.observe(OpKindBatch, resultErr, 0)
                return ErrTooManyOps
        }
        for i := range ops {
                if err := ops[i].validate(); err != nil {
                        s.metrics.observe(OpKindBatch, resultErr, 0)
                        return err
                }
        }
        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindBatch, resultErr, 0)
                return err
        }

        ctx = s.startSpan(ctx, "batch", nil)
        start := s.metrics.start()
        b := s.batchPool.Get().(*pebble.Batch)
        defer func() {
                b.Reset()
                s.batchPool.Put(b)
        }()

        pending := make([]cacheInval, 0, len(ops))
        for i := range ops {
                op := &ops[i]
                var err error
                switch op.Type {
                case OpSet:
                        err = b.Set(op.Key, op.Value, nil)
                        pending = append(pending, cacheInval{op: 0, key: op.Key, val: op.Value})
                case OpDelete:
                        err = b.Delete(op.Key, nil)
                        pending = append(pending, cacheInval{op: 1, key: op.Key})
                case OpDeleteRange:
                        err = b.DeleteRange(op.Key, op.End, nil)
                        pending = append(pending, cacheInval{op: 2, key: op.Key, end: op.End})
                case OpMerge:
                        err = b.Merge(op.Key, op.Value, nil)
                        pending = append(pending, cacheInval{op: 1, key: op.Key}) // merged value opaque: invalidate
                default:
                        err = ErrInvalidOperation
                }
                if err != nil {
                        latency := time.Since(start)
                        s.metrics.observe(OpKindBatch, resultErr, latency)
                        s.afterOp(ctx, "batch.apply", op.Key, err, latency)
                        return mapEngineError("batch.apply", op.Key, err)
                }
        }

        err := b.Commit(s.writeOpts)
        latency := time.Since(start)
        if err != nil {
                s.metrics.observe(OpKindBatch, resultErr, latency)
                s.afterOp(ctx, "batch.commit", nil, err, latency)
                s.breakerRecord(err)
                return mapEngineError("batch.commit", nil, err)
        }
        if s.cache != nil {
                for i := range pending {
                        ci := &pending[i]
                        switch ci.op {
                        case 0:
                                s.cache.set(ci.key, ci.val, 0)
                        case 1:
                                s.cache.delete(ci.key)
                        case 2:
                                s.cache.deleteRange(ci.key, ci.end)
                        }
                }
        }
        s.metrics.batchOpsTotal.Add(uint64(len(ops)))
        s.metrics.bytesWritten.Add(uint64(b.Len()))
        s.metrics.observe(OpKindBatch, resultOK, latency)
        s.afterOp(ctx, "batch.commit", nil, nil, latency)
        s.breakerRecord(nil)
        return nil
}

// NewWriteBatch implements ExtendedStore.
func (s *Store) NewWriteBatch() WriteBatch {
        b, _ := s.batchPool.Get().(*pebble.Batch)
        return &writeBatch{sb: s, batch: b, owned: true}
}

// Scan implements bedrock. start is inclusive, end exclusive; either
// may be nil to scan to the respective end of the keyspace.
func (s *Store) Scan(ctx context.Context, start, end []byte) (Iterator, error) {
        return s.ScanWithOptions(ctx, start, end, ScanOptions{})
}

// ScanWithOptions is the extended Scan accepting ScanOptions (prefetch).
// The returned iterator is positioned at the first key >= start; its
// tracing span, when enabled, ends on Close.
func (s *Store) ScanWithOptions(ctx context.Context, start, end []byte, opts ScanOptions) (Iterator, error) {
        if err := s.begin(); err != nil {
                return nil, err
        }
        defer s.inFlight.Done()

        if err := s.breakerAllow(); err != nil {
                s.metrics.observe(OpKindScan, resultErr, 0)
                return nil, err
        }
        startAt := s.metrics.start()
        iter, err := s.db.NewIter(&pebble.IterOptions{
                LowerBound: start,
                UpperBound: end,
        })
        if err != nil {
                latency := time.Since(startAt)
                s.metrics.observe(OpKindScan, resultErr, latency)
                s.afterOp(ctx, "scan", start, err, latency)
                s.breakerRecord(err)
                return nil, mapEngineError("scan", start, err)
        }
        si, err := newScanIterator(iter, start, opts)
        if err != nil {
                _ = iter.Close()
                latency := time.Since(startAt)
                s.metrics.observe(OpKindScan, resultErr, latency)
                return nil, mapEngineError("scan", start, err)
        }
        latency := time.Since(startAt)
        s.metrics.observe(OpKindScan, resultOK, latency)
        s.breakerRecord(nil)
        ci := &countingIterator{scanIterator: si, s: s}
        if ci.Valid() {
                // Count the initially positioned entry (Next counts the rest).
                ci.keys++
                s.metrics.scanKeysTotal.Add(1)
        }
        if s.tracing {
                _, sp := s.tracer.StartSpan(ctx, "scan", start)
                ci.span = sp
        }
        return ci, nil
}

// countingIterator counts keys for ScanKeysTotal, ends the scan tracing
// span on Close and releases engine resources.
type countingIterator struct {
        *scanIterator
        s      *Store
        span   Span
        keys   uint64
        closed bool
}

func (c *countingIterator) Next() bool {
        ok := c.scanIterator.Next()
        if ok {
                c.keys++
                c.s.metrics.scanKeysTotal.Add(1)
        }
        return ok
}

func (c *countingIterator) Close() error {
        if c.closed {
                return nil
        }
        c.closed = true
        err := c.scanIterator.Close()
        if c.span != nil {
                c.span.End(err)
        }
        return err
}

// Flush implements ExtendedStore.
func (s *Store) Flush(ctx context.Context) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()
        return mapEngineError("flush", nil, s.db.Flush())
}

// CompactRange implements ExtendedStore. nil start/end compact the whole
// keyspace: the bounds are derived from the first and last live keys
// (Pebble requires start < end; the empty keyspace is a no-op).
func (s *Store) CompactRange(ctx context.Context, start, end []byte) error {
        if err := s.begin(); err != nil {
                return err
        }
        defer s.inFlight.Done()

        if start == nil && end == nil {
                iter, err := s.db.NewIter(nil)
                if err != nil {
                        return mapEngineError("compact", nil, err)
                }
                haveFirst := iter.First()
                if !haveFirst {
                        _ = iter.Close()
                        return nil // empty keyspace: nothing to compact
                }
                first := append([]byte(nil), iter.Key()...)
                if !iter.Last() {
                        _ = iter.Close()
                        return nil
                }
                last := append([]byte(nil), iter.Key()...)
                _ = iter.Close()
                start = first
                last = append(last, 0x00) // exclusive upper bound past the last key
                end = last
        }
        return mapEngineError("compact", nil, s.db.Compact(start, end, false))
}

// CacheStats implements ExtendedStore.
func (s *Store) CacheStats() (entries int, bytes int64) {
        if s.cache == nil {
                return 0, 0
        }
        return s.cache.stats()
}

// engineStats projects Pebble's runtime metrics.
func (s *Store) engineStats() DBStats {
        m := s.db.Metrics()
        stats := DBStats{
                BlockCacheHits:   uint64(m.BlockCache.Hits),
                BlockCacheMisses: uint64(m.BlockCache.Misses),
                BlockCacheSize:   uint64(m.BlockCache.Size),
                MemTableSize:     m.MemTable.Size,
                MemTableCount:    m.MemTable.Count,
                WALFiles:         m.WAL.Files,
                WALSize:          m.WAL.Size,
                WALBytesWritten:  m.WAL.BytesWritten,
                FlushCount:       m.Flush.Count,
                CompactionCount:  m.Compact.Count,
                CompactionDebt:   m.Compact.EstimatedDebt,
                L0Files:          m.Levels[0].NumFiles,
                L6Files:          m.Levels[6].NumFiles,
                KeysTombstones:   m.Keys.TombstoneCount,
                OpenIters:        m.TableIters,
        }
        stats.DiskSize = m.DiskSpaceUsage()
        return stats
}

// MetricsSnapshot implements ExtendedStore.
func (s *Store) MetricsSnapshot() MetricsSnapshot {
        var entries int
        var bytes int64
        if s.cache != nil {
                entries, bytes = s.cache.stats()
        }
        state := ""
        if s.breaker != nil {
                state = s.breaker.State().String()
        }
        return s.metrics.Snapshot(state, entries, bytes, s.engineStats())
}

// Close implements bedrock: it rejects new operations, drains in-flight
// work, stops the cache janitor and closes the engine. Idempotent.
func (s *Store) Close() error {
        s.closeOnce.Do(func() {
                s.closing.Store(true)
                s.inFlight.Wait()
                if s.cache != nil {
                        s.cache.close()
                }
                err := s.db.Close()
                s.blockCach.Unref()
                if err != nil {
                        s.closeErr = mapEngineError("close", nil, err)
                }
                s.logger.Info("bedrock closed",
                        zap.String("name", s.name),
                        zap.Duration("uptime", time.Since(s.startedAt)))
        })
        return s.closeErr
}
