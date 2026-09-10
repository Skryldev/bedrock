package bedrock

import (
        "sort"
        "sync/atomic"
        "time"
)

// OpKind identifies a measured operation class.
type OpKind uint8

const (
        OpKindGet OpKind = iota
        OpKindSet
        OpKindDelete
        OpKindBatch
        OpKindScan
        OpKindMerge
        opKindCount // sentinel: keep last
)

var opKindNames = [opKindCount]string{"get", "set", "delete", "batch", "scan", "merge"}

// String returns the canonical metric label for the kind.
func (k OpKind) String() string {
        if int(k) < len(opKindNames) {
                return opKindNames[k]
        }
        return "unknown"
}

// ResultLabel classifies operation outcomes in counters.
type resultLabel uint8

const (
        resultOK resultLabel = iota
        resultNotFound
        resultErr
        resultCount
)

var resultNames = [resultCount]string{"ok", "not_found", "error"}

// LatencyStats is a percentile summary for one operation kind.
type LatencyStats struct {
        Count uint64
        Mean  time.Duration
        P50   time.Duration
        P95   time.Duration
        P99   time.Duration
        P999  time.Duration
        Max   time.Duration
}

// OpStats bundles per-operation counters and latency.
type OpStats struct {
        OK        uint64
        NotFound  uint64
        Errors    uint64
        Latency   LatencyStats
        OpsPerSec float64
}

// MetricsSnapshot is a consistent, copyable view of all store metrics.
type MetricsSnapshot struct {
        Name          string
        Uptime        time.Duration
        Ops           map[string]OpStats
        CacheHits     uint64
        CacheMisses   uint64
        CacheEvictions uint64
        CacheHitRatio  float64
        CacheEntries   int
        CacheBytes     int64
        CircuitOpens   uint64
        CircuitState   string
        BatchOpsTotal  uint64 // operations applied through batches
        ScanKeysTotal  uint64
        BytesRead      uint64
        BytesWritten   uint64
        SlowOps        uint64

        // Engine mirrors Pebble's internal statistics (see DBMetrics).
        Engine DBStats
}

// DBStats is a curated projection of Pebble's *Metrics, stable across the
// module's supported engine versions.
type DBStats struct {
        BlockCacheHits     uint64
        BlockCacheMisses   uint64
        BlockCacheSize     uint64
        MemTableSize       uint64
        MemTableCount      int64
        WALFiles           int64
        WALSize            uint64
        WALBytesWritten    uint64
        FlushCount         int64
        CompactionCount    int64
        CompactionDebt     uint64
        L0Files            int64
        L6Files            int64
        KeysTombstones     uint64
        DiskSize           uint64
        IteratorOpens      int64
        OpenIters          int64
}

// opsWindow is a sharded per-second counter ring used to derive ops/sec
// without locks or unbounded memory.
type opsWindow struct {
        secs  [64]atomic.Int64 // unix second stamp per slot; 0 = empty
        count [64]atomic.Uint64
}

// observe increments the counter for the current wall-clock second.
func (w *opsWindow) observe() {
        sec := time.Now().Unix()
        slot := int(sec) & 63
        for {
                old := w.secs[slot].Load()
                if old == sec {
                        w.count[slot].Add(1)
                        return
                }
                if w.secs[slot].CompareAndSwap(old, sec) {
                        w.count[slot].Store(1)
                        return
                }
        }
}

// rate returns operations per second averaged over the last windowSecs
// populated seconds (best effort; zero when no recent activity).
func (w *opsWindow) rate(windowSecs int64) float64 {
        now := time.Now().Unix()
        var total uint64
        var filled int64
        for i := 0; i < 64 && filled < windowSecs; i++ {
                sec := w.secs[i].Load()
                if sec == 0 || now-sec > int64(len(w.secs)) {
                        continue
                }
                total += w.count[i].Load()
                filled++
        }
        if filled == 0 {
                return 0
        }
        return float64(total) / float64(filled)
}

// opMetrics aggregates latency and outcome counters for one operation kind.
type opMetrics struct {
        latency  Histogram
        ok       atomic.Uint64
        notFound atomic.Uint64
        err      atomic.Uint64
        window   opsWindow
}

// MetricsRegistry is the module's in-process observability core. All record
// paths are lock-free; Snapshot is the only aggregating (and allocating)
// call, intended for admin endpoints and periodic scraping.
type MetricsRegistry struct {
        startedAt time.Time
        name      string

        ops [opKindCount]opMetrics

        cacheHits     atomic.Uint64
        cacheMisses   atomic.Uint64
        cacheEvictions atomic.Uint64
        circuitOpens  atomic.Uint64
        batchOpsTotal atomic.Uint64
        scanKeysTotal atomic.Uint64
        bytesRead     atomic.Uint64
        bytesWritten  atomic.Uint64
        slowOps       atomic.Uint64
}

func newMetricsRegistry(name string) *MetricsRegistry {
        return &MetricsRegistry{name: name, startedAt: time.Now()}
}

// start returns the wall-clock start time for latency measurement.
func (m *MetricsRegistry) start() time.Time { return time.Now() }

// observe records one completed operation.
func (m *MetricsRegistry) observe(kind OpKind, result resultLabel, latency time.Duration) {
        op := &m.ops[kind]
        op.latency.Record(latency)
        switch result {
        case resultOK:
                op.ok.Add(1)
        case resultNotFound:
                op.notFound.Add(1)
        default:
                op.err.Add(1)
        }
        op.window.observe()
}

// observeSlow increments the slow-operation counter.
func (m *MetricsRegistry) observeSlow() { m.slowOps.Add(1) }

// observeCache records a hot-cache hit or miss.
func (m *MetricsRegistry) observeCache(hit bool) {
        if hit {
                m.cacheHits.Add(1)
        } else {
                m.cacheMisses.Add(1)
        }
}

// Snapshot computes a consistent view. Percentile walks are O(512) per op
// kind; the whole call is ~microseconds and allocates a handful of objects.
func (m *MetricsRegistry) Snapshot(circuitState string, cacheEntries int, cacheBytes int64, engine DBStats) MetricsSnapshot {
        snap := MetricsSnapshot{
                Name:           m.name,
                Uptime:         time.Since(m.startedAt),
                Ops:            make(map[string]OpStats, opKindCount),
                CacheHits:      m.cacheHits.Load(),
                CacheMisses:    m.cacheMisses.Load(),
                CacheEvictions: m.cacheEvictions.Load(),
                CircuitOpens:   m.circuitOpens.Load(),
                BatchOpsTotal:  m.batchOpsTotal.Load(),
                ScanKeysTotal:  m.scanKeysTotal.Load(),
                BytesRead:      m.bytesRead.Load(),
                BytesWritten:   m.bytesWritten.Load(),
                SlowOps:        m.slowOps.Load(),
                CircuitState:   circuitState,
                CacheEntries:   cacheEntries,
                CacheBytes:     cacheBytes,
                Engine:         engine,
        }
        totalHits := snap.CacheHits + snap.CacheMisses
        if totalHits > 0 {
                snap.CacheHitRatio = float64(snap.CacheHits) / float64(totalHits)
        }
        for k := 0; k < int(opKindCount); k++ {
                op := &m.ops[k]
                snap.Ops[opKindNames[k]] = OpStats{
                        OK:        op.ok.Load(),
                        NotFound:  op.notFound.Load(),
                        Errors:    op.err.Load(),
                        OpsPerSec: op.window.rate(10),
                        Latency: LatencyStats{
                                Count: op.latency.Count(),
                                Mean:  op.latency.Mean(),
                                P50:   op.latency.ValueAtQuantile(50),
                                P95:   op.latency.ValueAtQuantile(95),
                                P99:   op.latency.ValueAtQuantile(99),
                                P999:  op.latency.ValueAtQuantile(99.9),
                                Max:   op.latency.Max(),
                        },
                }
        }
        return snap
}

// sortedOpNames returns snapshot op names in stable order (for tests and
// deterministic Prometheus rendering).
func sortedOpNames(ops map[string]OpStats) []string {
        names := make([]string, 0, len(ops))
        for n := range ops {
                names = append(names, n)
        }
        sort.Strings(names)
        return names
}
