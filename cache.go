package bedrock

import (
        "container/list"
        "runtime"
        "sync"
        "time"
)

// HotCacheConfig configures the in-process LRU+TTL read-through cache that
// sits in front of Pebble's block cache.
type HotCacheConfig struct {
        // Enabled turns the hot cache on.
        Enabled bool
        // MaxBytes is the approximate memory budget for cached values.
        // Default 64 MiB when Enabled with zero fields.
        MaxBytes int64
        // Shards is the number of lock stripes; more shards reduce contention
        // at the cost of per-shard map preallocation. Default: GOMAXPROCS*2,
        // clamped to [8, 64].
        Shards int
        // TTL is the default time-to-live for entries. Default 5m.
        TTL time.Duration
        // JanitorInterval is the period of the background sweeper that removes
        // expired entries. Default TTL/2 clamped to [1s, 30s].
        JanitorInterval time.Duration
}

// defaults fills zero fields.
func (c *HotCacheConfig) defaults() {
        if c.MaxBytes <= 0 {
                c.MaxBytes = 64 << 20
        }
        if c.Shards <= 0 {
                c.Shards = runtime.GOMAXPROCS(0) * 2
                if c.Shards < 8 {
                        c.Shards = 8
                }
                if c.Shards > 64 {
                        c.Shards = 64
                }
        }
        if c.TTL <= 0 {
                c.TTL = 5 * time.Minute
        }
        if c.JanitorInterval <= 0 {
                c.JanitorInterval = c.TTL / 2
                if c.JanitorInterval < time.Second {
                        c.JanitorInterval = time.Second
                }
                if c.JanitorInterval > 30*time.Second {
                        c.JanitorInterval = 30 * time.Second
                }
        }
}

const entryOverhead = 96 // struct + map node amortized cost in bytes

// cacheEntry is one LRU element.
type cacheEntry struct {
        key      string // string form enables alloc-free map lookups on Get
        value    []byte // store-owned, immutable
        size     int
        expireAt int64 // unix nanos
}

// hotShard is one lock stripe of the hot cache.
type hotShard struct {
        mu      sync.Mutex
        entries map[string]*list.Element
        lru     *list.List // of *cacheEntry, front = most recent
        bytes   int64
}

// hotCache is a sharded LRU+TTL cache. It guarantees read-your-writes
// coherence: Set/Delete/DeleteRange/Batch update or invalidate entries
// synchronously after a successful engine commit, so a Get never returns a
// value older than the last successful write performed through this store
// instance. Out-of-band writers (other processes) require a TTL no larger
// than the acceptable staleness window.
type hotCache struct {
        cfg    HotCacheConfig
        shards []*hotShard
        // clock is injectable for tests.
        clock func() time.Time
        done  chan struct{}
        once  sync.Once

        // janitor bookkeeping
        janitorWG sync.WaitGroup

        evictionsMu sync.Mutex
        evictions   uint64
}

func newHotCache(cfg HotCacheConfig) *hotCache {
        cfg.defaults()
        c := &hotCache{
                cfg:    cfg,
                shards: make([]*hotShard, cfg.Shards),
                clock:  time.Now,
                done:   make(chan struct{}),
        }
        for i := range c.shards {
                c.shards[i] = &hotShard{
                        entries: make(map[string]*list.Element, 16),
                        lru:     list.New(),
                }
        }
        c.janitorWG.Add(1)
        go c.janitor()
        return c
}

func (c *hotCache) shardFor(key string) *hotShard {
        return c.shards[fnv32a(key)%uint32(len(c.shards))]
}

// get returns the cached value for key. The returned slice is shared
// (read-only contract). Lookup does not allocate.
func (c *hotCache) get(key []byte) ([]byte, bool) {
        s := c.shardFor(string(key)) // string(key) in map-index position: no alloc
        s.mu.Lock()
        defer s.mu.Unlock()

        el, ok := s.entries[string(key)]
        if !ok {
                return nil, false
        }
        e := el.Value.(*cacheEntry)
        now := c.clock().UnixNano()
        if now >= e.expireAt {
                s.removeElement(el)
                return nil, false
        }
        s.lru.MoveToFront(el)
        return e.value, true
}

// set inserts or refreshes key. The value is copied; ttl overrides the
// default.
func (c *hotCache) set(key, value []byte, ttl time.Duration) {
        if ttl <= 0 {
                ttl = c.cfg.TTL
        }
        k := string(key) // one copy on the write path
        v := make([]byte, len(value))
        copy(v, value)

        s := c.shardFor(k)
        s.mu.Lock()
        defer s.mu.Unlock()

        e := &cacheEntry{
                key:      k,
                value:    v,
                size:     len(k) + len(v) + entryOverhead,
                expireAt: c.clock().Add(ttl).UnixNano(),
        }
        if el, ok := s.entries[k]; ok {
                old := el.Value.(*cacheEntry)
                s.bytes -= int64(old.size)
                el.Value = e
                s.lru.MoveToFront(el)
        } else {
                el := s.lru.PushFront(e)
                s.entries[k] = el
        }
        s.bytes += int64(e.size)
        c.evictLocked(s)
}

// delete drops key if present.
func (c *hotCache) delete(key []byte) {
        s := c.shardFor(string(key))
        s.mu.Lock()
        defer s.mu.Unlock()
        if el, ok := s.entries[string(key)]; ok {
                s.removeElement(el)
        }
}

// deleteRange invalidates every cached key in [start, end). It performs a
// full scan of cached keys (hot caches are small; bounded by MaxBytes).
func (c *hotCache) deleteRange(start, end []byte) {
        sk, ek := string(start), string(end)
        for _, s := range c.shards {
                s.mu.Lock()
                var victims []*list.Element
                for el := s.lru.Front(); el != nil; el = el.Next() {
                        k := el.Value.(*cacheEntry).key
                        if (len(start) == 0 || k >= sk) && (len(end) == 0 || k < ek) {
                                victims = append(victims, el)
                        }
                }
                for _, el := range victims {
                        s.removeElement(el)
                }
                s.mu.Unlock()
        }
}

// clear drops everything.
func (c *hotCache) clear() {
        for _, s := range c.shards {
                s.mu.Lock()
                s.entries = make(map[string]*list.Element, 16)
                s.lru.Init()
                s.bytes = 0
                s.mu.Unlock()
        }
}

// close stops the janitor goroutine.
func (c *hotCache) close() {
        c.once.Do(func() { close(c.done) })
        c.janitorWG.Wait()
}

// janitor periodically evicts expired entries and enforces capacity.
func (c *hotCache) janitor() {
        defer c.janitorWG.Done()
        t := time.NewTicker(c.cfg.JanitorInterval)
        defer t.Stop()
        for {
                select {
                case <-c.done:
                        return
                case <-t.C:
                        c.sweep()
                }
        }
}

// sweep removes expired entries and enforces the global byte budget.
func (c *hotCache) sweep() {
        now := c.clock().UnixNano()
        var total int64
        for _, s := range c.shards {
                s.mu.Lock()
                for el := s.lru.Front(); el != nil; {
                        next := el.Next()
                        e := el.Value.(*cacheEntry)
                        if now >= e.expireAt {
                                s.removeElement(el)
                        }
                        el = next
                }
                total += s.bytes
                s.mu.Unlock()
        }
        // Global capacity: evict from the fullest shards first (approximate).
        for total > c.cfg.MaxBytes {
                var fullest *hotShard
                var fullestBytes int64
                for _, s := range c.shards {
                        if b := s.loadBytes(); b > fullestBytes {
                                fullestBytes, fullest = b, s
                        }
                }
                if fullest == nil || fullestBytes == 0 {
                        return
                }
                fullest.mu.Lock()
                removed := fullest.evictOldest()
                fullest.mu.Unlock()
                if removed == nil {
                        return
                }
                total -= int64(removed.size)
        }
}

// loadBytes returns the shard's byte usage.
func (s *hotShard) loadBytes() int64 {
        s.mu.Lock()
        defer s.mu.Unlock()
        return s.bytes
}

// evictOldest removes the LRU tail and returns it.
func (s *hotShard) evictOldest() *cacheEntry {
        tail := s.lru.Back()
        if tail == nil {
                return nil
        }
        e := tail.Value.(*cacheEntry)
        s.removeElement(tail)
        return e
}

// removeElement unlinks el and updates accounting. Callers hold s.mu.
func (s *hotShard) removeElement(el *list.Element) {
        e := s.lru.Remove(el).(*cacheEntry)
        delete(s.entries, e.key)
        s.bytes -= int64(e.size)
}

// evictLocked enforces the per-shard share of the global budget while
// holding the shard lock.
func (c *hotCache) evictLocked(s *hotShard) {
        budget := c.cfg.MaxBytes / int64(len(c.shards))
        for s.bytes > budget {
                tail := s.lru.Back()
                if tail == nil {
                        return
                }
                s.removeElement(tail)
                c.noteEviction()
        }
}

func (c *hotCache) noteEviction() {
        c.evictionsMu.Lock()
        c.evictions++
        c.evictionsMu.Unlock()
}

func (c *hotCache) evictionCount() uint64 {
        c.evictionsMu.Lock()
        defer c.evictionsMu.Unlock()
        return c.evictions
}

// stats returns global entry count and byte usage.
func (c *hotCache) stats() (entries int, bytes int64) {
        for _, s := range c.shards {
                s.mu.Lock()
                entries += len(s.entries)
                bytes += s.bytes
                s.mu.Unlock()
        }
        return entries, bytes
}

// fnv32a is the 32-bit FNV-1a hash: fast, stable, allocation-free.
func fnv32a(s string) uint32 {
        const (
                offset32 = 2166136261
                prime32  = 16777619
        )
        h := uint32(offset32)
        for i := 0; i < len(s); i++ {
                h ^= uint32(s[i])
                h *= prime32
        }
        return h
}
