package bedrock

import (
        "errors"
        "math"
        "strings"
        "testing"
        "time"

        "github.com/cockroachdb/pebble"
)

func TestHistogramBucketLayout(t *testing.T) {
        // Round-trip: every bucket's lower bound must map back to itself.
        for idx := 0; idx < 495; idx++ {
                lo := bucketLowerBound(idx)
                if got := bucketFor(lo); got != idx {
                        t.Fatalf("bucket %d lower bound %d maps to %d", idx, lo, got)
                }
        }
        // Small values map exactly.
        if bucketFor(1) != 0 || bucketFor(7) != 6 {
                t.Fatal("small-value buckets wrong")
        }
        if bucketFor(8) != 7 || bucketFor(15) != 14 || bucketFor(16) != 15 {
                t.Fatal("octave-3 buckets wrong")
        }
        // Massive values stay in range.
        if got := bucketFor(math.MaxUint64); got >= histBuckets {
                t.Fatalf("max value bucket %d out of range", got)
        }
}

func TestHistogramQuantiles(t *testing.T) {
        h := &Histogram{}
        if h.ValueAtQuantile(50) != 0 {
                t.Fatal("empty histogram quantile must be 0")
        }
        if h.Mean() != 0 || h.Max() != 0 || h.Count() != 0 {
                t.Fatal("empty histogram aggregates must be zero")
        }

        // 100 observations: 99 at 100µs, 1 at 10ms.
        for i := 0; i < 99; i++ {
                h.Record(100 * time.Microsecond)
        }
        h.Record(10 * time.Millisecond)

        if got := h.Count(); got != 100 {
                t.Fatalf("count = %d", got)
        }
        if h.Max() != 10*time.Millisecond {
                t.Fatalf("max = %v", h.Max())
        }
        mean := h.Mean()
        if mean < 109*time.Microsecond || mean > 200*time.Microsecond {
                t.Fatalf("mean = %v", mean)
        }
        p50 := h.ValueAtQuantile(50)
        if p50 < 80*time.Microsecond || p50 > 130*time.Microsecond {
                t.Fatalf("p50 = %v (want ~100µs)", p50)
        }
        p99 := h.ValueAtQuantile(99)
        if p99 < 80*time.Microsecond || p99 > 130*time.Microsecond {
                t.Fatalf("p99 = %v (want ~100µs)", p99)
        }
        p999 := h.ValueAtQuantile(99.9)
        if p999 < 8*time.Millisecond || p999 > 13*time.Millisecond {
                t.Fatalf("p99.9 = %v (want ~10ms)", p999)
        }
        p100 := h.ValueAtQuantile(100)
        if p100 < 9*time.Millisecond {
                t.Fatalf("p100 = %v", p100)
        }
        // Clamping: q=150 behaves like q=100; q=-5 like q=0 (minimum).
        if h.ValueAtQuantile(150) != h.ValueAtQuantile(100) {
                t.Fatal("q>100 must clamp to 100")
        }
        if h.ValueAtQuantile(-5) != h.ValueAtQuantile(0) || h.ValueAtQuantile(0) == 0 {
                t.Fatal("negative quantile must clamp to 0 (minimum observation)")
        }
        if h.Sum() != 99*100*time.Microsecond+10*time.Millisecond {
                t.Fatalf("sum = %v", h.Sum())
        }
        s := h.String()
        if !strings.Contains(s, "count=100") || !strings.Contains(s, "p50=") {
                t.Fatalf("string = %s", s)
        }
}

func TestHistogramConcurrentRecording(t *testing.T) {
        h := &Histogram{}
        done := make(chan struct{})
        for w := 0; w < 4; w++ {
                go func() {
                        defer func() { done <- struct{}{} }()
                        for i := 0; i < 10000; i++ {
                                h.Record(time.Duration(i%1000) * time.Microsecond)
                        }
                }()
        }
        for w := 0; w < 4; w++ {
                <-done
        }
        if h.Count() != 40000 {
                t.Fatalf("count = %d, want 40000", h.Count())
        }
        if h.ValueAtQuantile(99) == 0 {
                t.Fatal("quantile must be populated")
        }
}

func TestHistogramVisitAndCumulative(t *testing.T) {
        h := &Histogram{}
        h.Record(5 * time.Microsecond)
        h.Record(500 * time.Microsecond)
        h.Record(5 * time.Millisecond)

        var seen int
        h.VisitBuckets(func(lower uint64, count uint64) {
                seen++
                if count == 0 {
                        t.Fatal("visited empty bucket")
                }
        })
        if seen != 3 {
                t.Fatalf("visited %d buckets, want 3", seen)
        }
        if got := h.cumulativeCount(1 * 1e6); got != 2 { // <= 1ms: 5µs, 500µs
                t.Fatalf("cumulative(1ms) = %d", got)
        }
        if got := h.cumulativeCount(10 * 1e9); got != 3 {
                t.Fatalf("cumulative(10s) = %d", got)
        }
        if got := h.cumulativeCount(100); got != 0 {
                t.Fatalf("cumulative(100ns) = %d", got)
        }
}

func TestOpsWindowRate(t *testing.T) {
        var w opsWindow
        if w.rate(10) != 0 {
                t.Fatal("empty window rate must be 0")
        }
        for i := 0; i < 50; i++ {
                w.observe()
        }
        // Same-second bursts: rate over >= 1 filled second.
        r := w.rate(10)
        if r < 50 {
                t.Fatalf("rate = %f, want >= 50", r)
        }
}

func TestMetricsRegistrySnapshotBasics(t *testing.T) {
        m := newMetricsRegistry("unit")
        m.observe(OpKindGet, resultOK, time.Millisecond)
        m.observe(OpKindGet, resultNotFound, 2*time.Millisecond)
        m.observe(OpKindGet, resultErr, 3*time.Millisecond)
        m.observeCache(true)
        m.observeCache(false)
        m.observeCache(true)
        m.cacheEvictions.Store(7)
        m.circuitOpens.Store(1)
        m.batchOpsTotal.Store(3)
        m.scanKeysTotal.Store(9)
        m.bytesRead.Store(100)
        m.bytesWritten.Store(200)
        m.observeSlow()

        snap := m.Snapshot("closed", 5, 500, DBStats{MemTableSize: 1})
        if snap.Name != "unit" {
                t.Fatalf("name = %s", snap.Name)
        }
        g := snap.Ops["get"]
        if g.OK != 1 || g.NotFound != 1 || g.Errors != 1 {
                t.Fatalf("op counters: %+v", g)
        }
        if g.Latency.P50 == 0 || g.Latency.Max < 3*time.Millisecond {
                t.Fatalf("latency: %+v", g.Latency)
        }
        if snap.CacheHits != 2 || snap.CacheMisses != 1 {
                t.Fatalf("cache counters: %d/%d", snap.CacheHits, snap.CacheMisses)
        }
        if snap.CacheHitRatio < 0.66 || snap.CacheHitRatio > 0.67 {
                t.Fatalf("hit ratio = %f", snap.CacheHitRatio)
        }
        if snap.CacheEntries != 5 || snap.CacheBytes != 500 {
                t.Fatalf("cache occupancy: %d %d", snap.CacheEntries, snap.CacheBytes)
        }
        if snap.CircuitOpens != 1 || snap.CircuitState != "closed" {
                t.Fatalf("circuit: %d %s", snap.CircuitOpens, snap.CircuitState)
        }
        if snap.BatchOpsTotal != 3 || snap.ScanKeysTotal != 9 || snap.SlowOps != 1 {
                t.Fatalf("totals: %+v", snap)
        }
        if snap.BytesRead != 100 || snap.BytesWritten != 200 {
                t.Fatalf("bytes: %d/%d", snap.BytesRead, snap.BytesWritten)
        }
        if snap.Engine.MemTableSize != 1 {
                t.Fatal("engine stats passthrough")
        }
}

func TestMetricsHelpers(t *testing.T) {
        if OpKindGet.String() != "get" || OpKindSet.String() != "set" ||
                OpKindDelete.String() != "delete" || OpKindBatch.String() != "batch" ||
                OpKindScan.String() != "scan" || OpKindMerge.String() != "merge" {
                t.Fatal("OpKind names wrong")
        }
        if OpKind(77).String() != "unknown" {
                t.Fatal("unknown kind")
        }
        ops := map[string]OpStats{"get": {}, "batch": {}, "set": {}}
        names := sortedOpNames(ops)
        if len(names) != 3 || names[0] != "batch" || names[1] != "get" || names[2] != "set" {
                t.Fatalf("sorted names = %v", names)
        }
        // resultNames indexes
        if resultNames[resultOK] != "ok" || resultNames[resultNotFound] != "not_found" || resultNames[resultErr] != "error" {
                t.Fatal("result names wrong")
        }
}

func TestItoaU64(t *testing.T) {
        if itoaU64(0) != "0" || itoaU64(7) != "7" || itoaU64(1234567890123) != "1234567890123" {
                t.Fatal("itoa wrong")
        }
}

func TestErrorClassString(t *testing.T) {
        if ClassOK.String() != "ok" || ClassTransient.String() != "transient" || ClassPermanent.String() != "permanent" {
                t.Fatal("ErrorClass strings wrong")
        }
        if ErrorClass(42).String() != "unknown" {
                t.Fatal("unknown class")
        }
}

func TestMapEngineError(t *testing.T) {
        if mapEngineError("get", nil, nil) != nil {
                t.Fatal("nil passthrough")
        }
        if got := mapEngineError("get", []byte("k"), pebble.ErrNotFound); got != ErrNotFound {
                t.Fatalf("mapped = %v", got)
        }
        custom := errors.New("disk on fire")
        got := mapEngineError("get", []byte("k"), custom)
        var se *StoreError
        if !errors.As(got, &se) || se.Err != custom || se.Op != "get" {
                t.Fatalf("wrapped = %v", got)
        }
}
