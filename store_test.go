package bedrock

import (
        "context"
        "errors"
        "fmt"
        "io"
        "os"
        "path/filepath"
        "sync"
        "sync/atomic"
        "testing"
        "time"

        "go.uber.org/zap"
        "go.uber.org/zap/zaptest"
)

// testCtx is a non-nil context for tests that don't need cancellation.
var testCtx = context.Background()

// openTestStore creates a store in a fresh temp directory with fast,
// test-appropriate settings.
func openTestStore(t *testing.T, opts ...Option) *Store {
        t.Helper()
        dir := t.TempDir()
        base := []Option{
                WithDataDir(dir),
                WithName("test"),
                WithCacheSizeMB(8),
                WithMemTableSizeMB(4),
                WithZapLogger(zaptest.NewLogger(t, zaptest.Level(zap.WarnLevel))),
        }
        base = append(base, opts...)
        s, err := Open(base...)
        if err != nil {
                t.Fatalf("Open: %v", err)
        }
        t.Cleanup(func() { _ = s.Close() })
        return s
}

func TestStoreSetGetDelete(t *testing.T) {
        s := openTestStore(t)

        if err := s.Set(testCtx, []byte("alpha"), []byte("1")); err != nil {
                t.Fatalf("Set: %v", err)
        }
        v, err := s.Get(testCtx, []byte("alpha"))
        if err != nil {
                t.Fatalf("Get: %v", err)
        }
        if string(v) != "1" {
                t.Fatalf("Get = %q, want %q", v, "1")
        }

        // Overwrite
        if err := s.Set(testCtx, []byte("alpha"), []byte("2")); err != nil {
                t.Fatalf("Set overwrite: %v", err)
        }
        v, _ = s.Get(testCtx, []byte("alpha"))
        if string(v) != "2" {
                t.Fatalf("Get after overwrite = %q", v)
        }

        // Delete
        if err := s.Delete(testCtx, []byte("alpha")); err != nil {
                t.Fatalf("Delete: %v", err)
        }
        if _, err := s.Get(testCtx, []byte("alpha")); !errors.Is(err, ErrNotFound) {
                t.Fatalf("Get after Delete err = %v, want ErrNotFound", err)
        }

        // Delete of a missing key is idempotent
        if err := s.Delete(testCtx, []byte("never-existed")); err != nil {
                t.Fatalf("Delete missing: %v", err)
        }
}

func TestStoreGetNotFound(t *testing.T) {
        s := openTestStore(t)
        _, err := s.Get(testCtx, []byte("missing"))
        if !errors.Is(err, ErrNotFound) {
                t.Fatalf("err = %v, want ErrNotFound", err)
        }
        // Error must classify as OK: not a failure.
        if got := Classify(err); got != ClassOK {
                t.Fatalf("Classify(ErrNotFound) = %v, want ok", got)
        }
}

func TestStoreEmptyKey(t *testing.T) {
        s := openTestStore(t)
        for name, fn := range map[string]func() error{
                "get":    func() error { _, err := s.Get(testCtx, nil); return err },
                "set":    func() error { return s.Set(testCtx, nil, []byte("v")) },
                "delete": func() error { return s.Delete(testCtx, []byte{}) },
                "merge":  func() error { return s.Merge(testCtx, nil, []byte("v")) },
        } {
                if err := fn(); !errors.Is(err, ErrEmptyKey) {
                        t.Fatalf("%s with empty key err = %v, want ErrEmptyKey", name, err)
                }
        }
        // Batch with an empty key inside.
        err := s.Batch(testCtx, []Operation{{Type: OpSet, Key: nil, Value: []byte("v")}})
        if !errors.Is(err, ErrEmptyKey) {
                t.Fatalf("Batch empty key err = %v", err)
        }
}

func TestStoreBatchAtomicity(t *testing.T) {
        s := openTestStore(t)
        ops := []Operation{
                {Type: OpSet, Key: []byte("b1"), Value: []byte("v1")},
                {Type: OpSet, Key: []byte("b2"), Value: []byte("v2")},
                {Type: OpMerge, Key: []byte("b1"), Value: []byte("v1")},
                {Type: OpDelete, Key: []byte("b2")},
        }
        if err := s.Batch(testCtx, ops); err != nil {
                t.Fatalf("Batch: %v", err)
        }
        if _, err := s.Get(testCtx, []byte("b1")); err != nil {
                t.Fatalf("b1: %v", err)
        }
        if _, err := s.Get(testCtx, []byte("b2")); !errors.Is(err, ErrNotFound) {
                t.Fatalf("b2 should be deleted, err=%v", err)
        }

        // Empty batch
        if err := s.Batch(testCtx, nil); !errors.Is(err, ErrEmptyBatch) {
                t.Fatalf("empty batch err = %v", err)
        }
        // Invalid op type
        err := s.Batch(testCtx, []Operation{{Type: OpType(99), Key: []byte("k")}})
        if !errors.Is(err, ErrInvalidOperation) {
                t.Fatalf("invalid op err = %v", err)
        }
        // MaxBatchOps enforcement
        s2 := openTestStore(t, WithMaxBatchOps(2))
        err = s2.Batch(testCtx, []Operation{
                {Type: OpSet, Key: []byte("a"), Value: []byte("1")},
                {Type: OpSet, Key: []byte("b"), Value: []byte("2")},
                {Type: OpSet, Key: []byte("c"), Value: []byte("3")},
        })
        if !errors.Is(err, ErrTooManyOps) {
                t.Fatalf("max batch ops err = %v", err)
        }
}

func TestStoreBatchDeleteRange(t *testing.T) {
        s := openTestStore(t)
        var ops []Operation
        for i := 0; i < 20; i++ {
                ops = append(ops, Operation{Type: OpSet, Key: []byte(fmt.Sprintf("r/%03d", i)), Value: []byte("v")})
        }
        if err := s.Batch(testCtx, ops); err != nil {
                t.Fatalf("seed batch: %v", err)
        }
        err := s.Batch(testCtx, []Operation{
                {Type: OpDeleteRange, Key: []byte("r/005"), End: []byte("r/010")},
        })
        if err != nil {
                t.Fatalf("delete range: %v", err)
        }
        it, err := s.Scan(testCtx, []byte("r/"), nil)
        if err != nil {
                t.Fatalf("scan: %v", err)
        }
        count := 0
        for it.Valid() {
                k := string(it.Key())
                if k >= "r/005" && k < "r/010" {
                        t.Fatalf("key %s survived range delete", k)
                }
                count++
                it.Next()
        }
        _ = it.Close()
        if count != 15 {
                t.Fatalf("count = %d, want 15", count)
        }
}

func TestStoreScanBoundsAndOrder(t *testing.T) {
        s := openTestStore(t)
        for _, kv := range [][2]string{
                {"a/1", "1"}, {"a/2", "2"}, {"a/3", "3"}, {"b/1", "4"}, {"c/1", "5"},
        } {
                if err := s.Set(testCtx, []byte(kv[0]), []byte(kv[1])); err != nil {
                        t.Fatal(err)
                }
        }

        it, err := s.Scan(testCtx, []byte("a/"), []byte("c/"))
        if err != nil {
                t.Fatalf("scan: %v", err)
        }
        defer it.Close()

        var got []string
        for ; it.Valid(); it.Next() {
                got = append(got, string(it.Key()))
        }
        if err := it.Error(); err != nil {
                t.Fatalf("iter error: %v", err)
        }
        want := []string{"a/1", "a/2", "a/3", "b/1"}
        if len(got) != len(want) {
                t.Fatalf("got %v, want %v", got, want)
        }
        for i := range want {
                if got[i] != want[i] {
                        t.Fatalf("got[%d]=%s want %s", i, got[i], want[i])
                }
        }

        // SeekGE / SeekLT / Last / First on the positioned iterator
        if !it.SeekGE([]byte("a/2")) || string(it.Key()) != "a/2" {
                t.Fatalf("SeekGE(a/2) = %q", it.Key())
        }
        if !it.SeekLT([]byte("b/1")) || string(it.Key()) != "a/3" {
                t.Fatalf("SeekLT(b/1) = %q", it.Key())
        }
        if !it.First() || string(it.Key()) != "a/1" {
                t.Fatalf("First = %q", it.Key())
        }
        if !it.Last() || string(it.Key()) != "b/1" {
                t.Fatalf("Last = %q", it.Key())
        }
}

func TestStoreScanEmptyRange(t *testing.T) {
        s := openTestStore(t)
        it, err := s.Scan(testCtx, []byte("zzz"), []byte("zzzz"))
        if err != nil {
                t.Fatalf("scan: %v", err)
        }
        defer it.Close()
        if it.Valid() {
                t.Fatal("iterator over empty range must be invalid")
        }
        if err := it.Error(); err != nil {
                t.Fatalf("error: %v", err)
        }
}

func TestStoreScanWithPrefetch(t *testing.T) {
        s := openTestStore(t)
        const n = 100
        var ops []Operation
        for i := 0; i < n; i++ {
                ops = append(ops, Operation{Type: OpSet,
                        Key: []byte(fmt.Sprintf("p/%04d", i)), Value: []byte(fmt.Sprintf("v%d", i))})
        }
        if err := s.Batch(testCtx, ops); err != nil {
                t.Fatal(err)
        }
        for _, prefetch := range []int{7, 64, 5000} {
                it, err := s.ScanWithOptions(testCtx, nil, nil, ScanOptions{Prefetch: prefetch})
                if err != nil {
                        t.Fatalf("prefetch=%d scan: %v", prefetch, err)
                }
                count := 0
                for ; it.Valid(); it.Next() {
                        count++
                }
                if err := it.Error(); err != nil {
                        t.Fatalf("prefetch=%d error: %v", prefetch, err)
                }
                if err := it.Close(); err != nil {
                        t.Fatalf("prefetch=%d close: %v", prefetch, err)
                }
                if count != n {
                        t.Fatalf("prefetch=%d count=%d want %d", prefetch, count, n)
                }
        }
}

func TestStoreHotCacheCoherence(t *testing.T) {
        s := openTestStore(t, WithHotCache(1<<20, time.Minute))

        if err := s.Set(testCtx, []byte("k"), []byte("v1")); err != nil {
                t.Fatal(err)
        }
        v1, _ := s.Get(testCtx, []byte("k"))
        if string(v1) != "v1" {
                t.Fatalf("first get = %q", v1)
        }
        // Overwrite: cache must be updated, not stale.
        if err := s.Set(testCtx, []byte("k"), []byte("v2")); err != nil {
                t.Fatal(err)
        }
        v2, _ := s.Get(testCtx, []byte("k"))
        if string(v2) != "v2" {
                t.Fatalf("get after overwrite = %q (stale cache)", v2)
        }
        // Delete: cache must be invalidated.
        if err := s.Delete(testCtx, []byte("k")); err != nil {
                t.Fatal(err)
        }
        if _, err := s.Get(testCtx, []byte("k")); !errors.Is(err, ErrNotFound) {
                t.Fatalf("get after delete = %v (stale cache)", err)
        }

        // Batch coherence
        err := s.Batch(testCtx, []Operation{
                {Type: OpSet, Key: []byte("bk"), Value: []byte("bv")},
                {Type: OpSet, Key: []byte("dk"), Value: []byte("dv")},
        })
        if err != nil {
                t.Fatal(err)
        }
        if _, err := s.Get(testCtx, []byte("bk")); err != nil {
                t.Fatalf("batch set coherence: %v", err)
        }
        if err := s.Delete(testCtx, []byte("dk")); err != nil {
                t.Fatal(err)
        }

        // SetWithTTL + expiry
        if err := s.SetWithTTL(testCtx, []byte("ttl"), []byte("tv"), time.Millisecond); err != nil {
                t.Fatal(err)
        }
        if _, err := s.Get(testCtx, []byte("ttl")); err != nil {
                t.Fatalf("ttl get before expiry: %v", err)
        }
        entries, _ := s.CacheStats()
        if entries == 0 {
                t.Fatal("hot cache should hold entries")
        }
        s.cache.close() // stop janitor before manual clock test
        s.cache.clock = func() time.Time { return time.Now().Add(time.Hour) }
        if _, ok := s.cache.get([]byte("ttl")); ok {
                t.Fatal("entry must be expired after TTL")
        }
}

func TestHotCacheEvictionAndSweep(t *testing.T) {
        cfg := HotCacheConfig{Enabled: true, MaxBytes: 2048, Shards: 2, TTL: time.Minute, JanitorInterval: 10 * time.Millisecond}
        c := newHotCache(cfg)
        defer c.close()

        value := make([]byte, 256)
        for i := 0; i < 100; i++ {
                c.set([]byte(fmt.Sprintf("key-%04d", i)), value, 0)
        }
        entries, bytes := c.stats()
        if bytes > int64(cfg.MaxBytes)+1024 { // shard budget rounding slack
                t.Fatalf("cache bytes %d exceeds budget %d", bytes, cfg.MaxBytes)
        }
        if entries == 0 {
                t.Fatal("cache should not be empty")
        }
        if c.evictionCount() == 0 {
                t.Fatal("expected evictions")
        }

        // TTL sweep via janitor
        c.set([]byte("expiring"), []byte("v"), time.Millisecond)
        time.Sleep(60 * time.Millisecond) // janitor runs every 10ms
        if _, ok := c.get([]byte("expiring")); ok {
                t.Fatal("janitor should have swept expired entry")
        }

        // deleteRange
        c.set([]byte("dr/a"), []byte("1"), 0)
        c.set([]byte("dr/b"), []byte("2"), 0)
        c.set([]byte("other"), []byte("3"), 0)
        c.deleteRange([]byte("dr/"), []byte("dr0"))
        if _, ok := c.get([]byte("dr/a")); ok {
                t.Fatal("dr/a should be invalidated")
        }
        if _, ok := c.get([]byte("dr/b")); ok {
                t.Fatal("dr/b should be invalidated")
        }
        if _, ok := c.get([]byte("other")); !ok {
                t.Fatal("other must survive range invalidation")
        }

        // clear
        c.clear()
        if entries, _ := c.stats(); entries != 0 {
                t.Fatalf("clear left %d entries", entries)
        }
}

func TestStoreNoCacheMode(t *testing.T) {
        s := openTestStore(t) // no WithHotCache
        if err := s.Set(testCtx, []byte("k"), []byte("v")); err != nil {
                t.Fatal(err)
        }
        if _, err := s.Get(testCtx, []byte("k")); err != nil {
                t.Fatal(err)
        }
        if e, b := s.CacheStats(); e != 0 || b != 0 {
                t.Fatalf("cache stats must be zero without hot cache: %d %d", e, b)
        }
        snap := s.MetricsSnapshot()
        if snap.CacheHits != 0 || snap.CacheMisses != 0 {
                t.Fatalf("cache metrics must be zero: %+v", snap)
        }
}

func TestStoreWriteBatchLifecycle(t *testing.T) {
        s := openTestStore(t, WithHotCache(1<<20, time.Minute))

        wb := s.NewWriteBatch()
        if err := wb.Set([]byte("wb1"), []byte("v1")); err != nil {
                t.Fatal(err)
        }
        if err := wb.Set([]byte("wb2"), []byte("v2")); err != nil {
                t.Fatal(err)
        }
        if err := wb.Delete([]byte("wb2")); err != nil {
                t.Fatal(err)
        }
        if wb.Count() != 3 {
                t.Fatalf("count = %d, want 3", wb.Count())
        }
        if wb.Len() == 0 {
                t.Fatal("encoded length must be > 0")
        }
        if err := wb.Commit(testCtx); err != nil {
                t.Fatalf("commit: %v", err)
        }
        if _, err := s.Get(testCtx, []byte("wb1")); err != nil {
                t.Fatalf("wb1: %v", err)
        }
        if _, err := s.Get(testCtx, []byte("wb2")); !errors.Is(err, ErrNotFound) {
                t.Fatalf("wb2: %v", err)
        }

        // Reuse after commit
        wb.Reset()
        if wb.Count() != 0 {
                t.Fatalf("count after reset = %d", wb.Count())
        }
        if err := wb.Set([]byte("wb3"), []byte("v3")); err != nil {
                t.Fatal(err)
        }
        if err := wb.Commit(testCtx); err != nil {
                t.Fatal(err)
        }
        if err := wb.Close(); err != nil {
                t.Fatal(err)
        }

        // Double close is a no-op; use-after-close errors.
        if err := wb.Close(); err != nil {
                t.Fatal(err)
        }
        if err := wb.Set([]byte("x"), []byte("y")); err == nil {
                t.Fatal("use after close must fail")
        }
        if err := wb.Commit(testCtx); err == nil {
                t.Fatal("commit after close must fail")
        }

        // DeleteRange via write batch
        wb2 := s.NewWriteBatch()
        if err := wb2.DeleteRange([]byte("wb1"), []byte("wb2")); err != nil {
                t.Fatal(err)
        }
        if err := wb2.Commit(testCtx); err != nil {
                t.Fatal(err)
        }
        if _, err := s.Get(testCtx, []byte("wb1")); !errors.Is(err, ErrNotFound) {
                t.Fatalf("wb1 after range delete: %v", err)
        }
        // Invalid ops
        if err := wb2.Delete(nil); !errors.Is(err, ErrEmptyKey) {
                t.Fatalf("empty key err = %v", err)
        }
        if err := wb2.DeleteRange(nil, []byte("z")); err == nil {
                t.Fatal("invalid range must fail")
        }
        _ = wb2.Close()
}

func TestStoreMerge(t *testing.T) {
        s := openTestStore(t)
        if err := s.Merge(testCtx, []byte("m"), []byte("a,")); err != nil {
                t.Fatal(err)
        }
        if err := s.Merge(testCtx, []byte("m"), []byte("b,")); err != nil {
                t.Fatal(err)
        }
        v, err := s.Get(testCtx, []byte("m"))
        if err != nil {
                t.Fatal(err)
        }
        if string(v) != "a,b," { // default pebble merge operator concatenates
                t.Fatalf("merge = %q", v)
        }
}

func TestStoreMetricsSnapshot(t *testing.T) {
        s := openTestStore(t)
        _, _ = s.Get(testCtx, []byte("nope"))  // not found
        _ = s.Set(testCtx, []byte("k"), []byte("v"))
        _, _ = s.Get(testCtx, []byte("k")) // hit
        _ = s.Delete(testCtx, []byte("k"))
        _ = s.Batch(testCtx, []Operation{{Type: OpSet, Key: []byte("b"), Value: []byte("v")}})
        it, _ := s.Scan(testCtx, nil, nil)
        for it.Valid() {
                it.Next()
        }
        _ = it.Close()

        snap := s.MetricsSnapshot()
        if snap.Ops["get"].NotFound != 1 {
                t.Fatalf("get notfound = %d", snap.Ops["get"].NotFound)
        }
        if snap.Ops["get"].OK < 1 {
                t.Fatalf("get ok = %d", snap.Ops["get"].OK)
        }
        if snap.Ops["set"].OK != 1 || snap.Ops["delete"].OK != 1 || snap.Ops["batch"].OK != 1 || snap.Ops["scan"].OK != 1 {
                t.Fatalf("op counters wrong: %+v", snap.Ops)
        }
        if snap.Ops["scan"].Latency.P50 == 0 {
                t.Fatal("latency percentiles must be populated")
        }
        if snap.BatchOpsTotal != 1 {
                t.Fatalf("batch ops total = %d", snap.BatchOpsTotal)
        }
        if snap.ScanKeysTotal < 1 {
                t.Fatalf("scan keys = %d", snap.ScanKeysTotal)
        }
        if snap.Engine.MemTableCount < 1 {
                t.Fatalf("engine memtable count = %d", snap.Engine.MemTableCount)
        }
        if snap.Uptime <= 0 {
                t.Fatal("uptime must be positive")
        }
}

func TestPrometheusRendering(t *testing.T) {
        s := openTestStore(t, WithHotCache(1<<20, time.Minute), WithCircuitBreaker(CircuitConfig{}))
        _ = s.Set(testCtx, []byte("k"), []byte("v"))
        _, _ = s.Get(testCtx, []byte("k"))
        _, _ = s.Get(testCtx, []byte("missing"))

        out := s.PrometheusMetrics()
        for _, want := range []string{
                "bedrock_operations_total{op=\"get\",result=\"ok\"}",
                "bedrock_operations_total{op=\"get\",result=\"not_found\"}",
                "bedrock_operation_duration_seconds_bucket{op=\"get\",le=\"+Inf\"}",
                "bedrock_operation_duration_seconds_sum{op=\"get\"}",
                "bedrock_cache_hits_total",
                "bedrock_cache_hit_ratio",
                "bedrock_circuit_state",
                "bedrock_engine_memtable_size_bytes",
                "bedrock_engine_block_cache_hits_total",
                "bedrock_uptime_seconds",
        } {
                if !contains(out, want) {
                        t.Fatalf("prom output missing %q\n---\n%s", want, out[:min(len(out), 2000)])
                }
        }
}

func contains(s, sub string) bool {
        return len(s) >= len(sub) && (func() bool {
                for i := 0; i+len(sub) <= len(s); i++ {
                        if s[i:i+len(sub)] == sub {
                                return true
                        }
                }
                return false
        })()
}

func min(a, b int) int {
        if a < b {
                return a
        }
        return b
}

func TestStoreConcurrentOperations(t *testing.T) {
        s := openTestStore(t, WithHotCache(4<<20, time.Minute))
        var wg sync.WaitGroup
        errCh := make(chan error, 64)
        for w := 0; w < 8; w++ {
                wg.Add(1)
                go func(worker int) {
                        defer wg.Done()
                        for i := 0; i < 200; i++ {
                                key := []byte(fmt.Sprintf("w%d-k%04d", worker, i))
                                val := []byte(fmt.Sprintf("v%d-%d", worker, i))
                                if err := s.Set(testCtx, key, val); err != nil {
                                        errCh <- err
                                        return
                                }
                                got, err := s.Get(testCtx, key)
                                if err != nil {
                                        errCh <- err
                                        return
                                }
                                if string(got) != string(val) {
                                        errCh <- fmt.Errorf("read-your-write violation: %q != %q", got, val)
                                        return
                                }
                                if i%25 == 0 {
                                        _ = s.Delete(testCtx, key)
                                }
                        }
                }(w)
        }
        wg.Wait()
        close(errCh)
        for err := range errCh {
                t.Fatal(err)
        }
}

func TestStoreConcurrentMixedWorkload(t *testing.T) {
        s := openTestStore(t, WithHotCache(2<<20, time.Minute))
        var stop atomic.Bool
        var wg sync.WaitGroup

        // writers
        for w := 0; w < 2; w++ {
                wg.Add(1)
                go func(id int) {
                        defer wg.Done()
                        for i := 0; !stop.Load(); i++ {
                                _ = s.Set(testCtx, []byte(fmt.Sprintf("hot%d", i%50)), []byte("v"))
                        }
                }(w)
        }
        // readers
        for r := 0; r < 4; r++ {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        for !stop.Load() {
                                _, _ = s.Get(testCtx, []byte("hot5"))
                                _, _ = s.Get(testCtx, []byte("absent"))
                        }
                }()
        }
        // scanner
        wg.Add(1)
        go func() {
                defer wg.Done()
                for !stop.Load() {
                        it, err := s.Scan(testCtx, []byte("hot"), []byte("hoz"))
                        if err == nil {
                                for it.Valid() {
                                        it.Next()
                                }
                                _ = it.Close()
                        }
                }
        }()
        time.Sleep(300 * time.Millisecond)
        stop.Store(true)
        wg.Wait()
}

func TestStoreGracefulShutdown(t *testing.T) {
        dir := t.TempDir()
        s, err := Open(WithDataDir(dir), WithCacheSizeMB(8), WithMemTableSizeMB(4),
                WithZapLogger(zap.NewNop()))
        if err != nil {
                t.Fatal(err)
        }
        if err := s.Set(testCtx, []byte("k"), []byte("v")); err != nil {
                t.Fatal(err)
        }
        if err := s.Close(); err != nil {
                t.Fatalf("close: %v", err)
        }
        // Idempotent
        if err := s.Close(); err != nil {
                t.Fatalf("second close: %v", err)
        }
        // Operations after close
        if err := s.Set(testCtx, []byte("k2"), []byte("v")); err == nil {
                t.Fatal("set after close must fail")
        }
        if _, err := s.Get(testCtx, []byte("k")); err == nil {
                t.Fatal("get after close must fail")
        }

        // Reopen: data must survive (WAL replay).
        s2, err := Open(WithDataDir(dir), WithCacheSizeMB(8), WithMemTableSizeMB(4), WithZapLogger(zap.NewNop()))
        if err != nil {
                t.Fatalf("reopen: %v", err)
        }
        defer s2.Close()
        v, err := s2.Get(testCtx, []byte("k"))
        if err != nil || string(v) != "v" {
                t.Fatalf("reopened get = %q, %v", v, err)
        }
}

func TestStoreShutdownDrainsInFlight(t *testing.T) {
        s := openTestStore(t)
        var wg sync.WaitGroup
        var opErrCount atomic.Int64
        for i := 0; i < 4; i++ {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        for j := 0; j < 1000; j++ {
                                err := s.Set(testCtx, []byte(fmt.Sprintf("d%d", j)), []byte("v"))
                                if err != nil {
                                        opErrCount.Add(1)
                                        return
                                }
                        }
                }()
        }
        time.Sleep(50 * time.Millisecond)
        _ = s.Close() // must not panic; in-flight ops complete or reject cleanly
        wg.Wait()
}

func TestStoreFlushAndCompact(t *testing.T) {
        s := openTestStore(t)
        for i := 0; i < 100; i++ {
                if err := s.Set(testCtx, []byte(fmt.Sprintf("c/%04d", i)), []byte("v")); err != nil {
                        t.Fatal(err)
                }
        }
        if err := s.Flush(testCtx); err != nil {
                t.Fatalf("flush: %v", err)
        }
        for i := 0; i < 50; i++ {
                _ = s.Delete(testCtx, []byte(fmt.Sprintf("c/%04d", i)))
        }
        if err := s.Flush(testCtx); err != nil {
                t.Fatal(err)
        }
        if err := s.CompactRange(testCtx, nil, nil); err != nil {
                t.Fatalf("compact: %v", err)
        }
        // Tombstones compacted away: remaining 50 readable
        count := 0
        it, _ := s.Scan(testCtx, []byte("c/"), nil)
        for ; it.Valid(); it.Next() {
                count++
        }
        _ = it.Close()
        if count != 50 {
                t.Fatalf("count after compact = %d, want 50", count)
        }
}

func TestStoreCheckpointAndRestore(t *testing.T) {
        root := t.TempDir()
        dataDir := filepath.Join(root, "data")
        backupDir := filepath.Join(root, "backups", "backup-001")

        s, err := Open(WithDataDir(dataDir), WithCacheSizeMB(8), WithMemTableSizeMB(4), WithZapLogger(zap.NewNop()))
        if err != nil {
                t.Fatal(err)
        }
        for i := 0; i < 10; i++ {
                if err := s.Set(testCtx, []byte(fmt.Sprintf("cp/%d", i)), []byte("v")); err != nil {
                        t.Fatal(err)
                }
        }
        if err := s.CreateCheckpoint(backupDir); err != nil {
                t.Fatalf("checkpoint: %v", err)
        }
        // Refuse overwrite of a non-empty destination
        if err := s.CreateCheckpoint(backupDir); !errors.Is(err, ErrCheckpointExists) {
                t.Fatalf("second checkpoint err = %v", err)
        }
        // More data after checkpoint must not be in the backup
        _ = s.Set(testCtx, []byte("cp/after"), []byte("x"))
        _ = s.Close()

        // Restore into a new dir and verify contents
        restoreDir := filepath.Join(root, "restored")
        if err := RestoreCheckpoint(backupDir, restoreDir); err != nil {
                t.Fatalf("restore: %v", err)
        }
        r, err := Open(WithDataDir(restoreDir), WithCacheSizeMB(8), WithZapLogger(zap.NewNop()))
        if err != nil {
                t.Fatalf("open restored: %v", err)
        }
        defer r.Close()
        if _, err := r.Get(testCtx, []byte("cp/3")); err != nil {
                t.Fatalf("restored cp/3: %v", err)
        }
        if _, err := r.Get(testCtx, []byte("cp/after")); !errors.Is(err, ErrNotFound) {
                t.Fatal("post-checkpoint key must not appear in backup")
        }

        // Restore refusal: non-empty destination
        if err := RestoreCheckpoint(backupDir, restoreDir); !errors.Is(err, ErrCheckpointExists) {
                t.Fatalf("restore to non-empty err = %v", err)
        }
        // Missing backup dir
        if err := RestoreCheckpoint(filepath.Join(root, "nope"), filepath.Join(root, "x")); err == nil {
                t.Fatal("missing backup must fail")
        }

        // ListCheckpoints: scan the dedicated backups root.
        list, err := ListCheckpoints(filepath.Join(root, "backups"))
        if err != nil {
                t.Fatal(err)
        }
        if len(list) != 1 || list[0] != backupDir {
                t.Fatalf("list = %v", list)
        }
}

func TestOpenOrRestoreRecoversFromCorruption(t *testing.T) {
        root := t.TempDir()
        dataDir := filepath.Join(root, "data")
        backupDir := filepath.Join(root, "backup")

        cfg := DefaultConfig("rec")
        cfg.DataDir = dataDir
        cfg.CacheSizeMB = 8
        cfg.MemTableSizeMB = 4
        cfg.Logger = zap.NewNop()

        s, err := Open(withDataDirOption(cfg)...)
        if err != nil {
                t.Fatal(err)
        }
        _ = s.Set(testCtx, []byte("survivor"), []byte("yes"))
        if err := s.CreateCheckpoint(backupDir); err != nil {
                t.Fatal(err)
        }
        _ = s.Close()

        // Corrupt: remove the MANIFEST (engine cannot open without it).
        matches, _ := filepath.Glob(filepath.Join(dataDir, "MANIFEST-*"))
        if len(matches) == 0 {
                t.Skip("no manifest found to corrupt")
        }
        for _, m := range matches {
                _ = os.Remove(m)
        }

        // OpenOrRestore without backup returns the original error.
        if _, err := OpenOrRestore(cfg, ""); err == nil {
                t.Fatal("open of corrupt dir must fail without backup")
        }
        // With backup it recovers.
        s2, err := OpenOrRestore(cfg, backupDir)
        if err != nil {
                t.Fatalf("OpenOrRestore: %v", err)
        }
        defer s2.Close()
        v, err := s2.Get(testCtx, []byte("survivor"))
        if err != nil || string(v) != "yes" {
                t.Fatalf("recovered value = %q, %v", v, err)
        }
}

func TestRepositoryLifecycle(t *testing.T) {
        base := t.TempDir()
        r := NewRepository(base, WithCacheSizeMB(8), WithMemTableSizeMB(4), WithZapLogger(zap.NewNop()))

        a1, err := r.Open("tenant-a")
        if err != nil {
                t.Fatal(err)
        }
        a2, err := r.Open("tenant-a")
        if err != nil {
                t.Fatal(err)
        }
        if a1 != a2 {
                t.Fatal("same name must return the same store instance")
        }
        b, err := r.Open("tenant-b")
        if err != nil {
                t.Fatal(err)
        }
        if b.DataDir() != filepath.Join(base, "tenant-b") {
                t.Fatalf("b dir = %s", b.DataDir())
        }
        // Isolation: writes in a are invisible to b.
        _ = a1.Set(testCtx, []byte("k"), []byte("a"))
        if _, err := b.Get(testCtx, []byte("k")); !errors.Is(err, ErrNotFound) {
                t.Fatal("stores must be isolated")
        }
        // Refcount: single close keeps it open.
        if err := r.Close("tenant-a"); err != nil {
                t.Fatal(err)
        }
        if _, ok := r.Get("tenant-a"); !ok {
                t.Fatal("store must stay open with outstanding refs")
        }
        if err := r.Close("tenant-a"); err != nil {
                t.Fatal(err)
        }
        if _, ok := r.Get("tenant-a"); ok {
                t.Fatal("store must be gone after final close")
        }
        // Unknown name
        if err := r.Close("ghost"); !errors.Is(err, ErrStoreNotFound) {
                t.Fatalf("close ghost = %v", err)
        }
        // Name validation
        if _, err := r.Open("../evil"); err == nil {
                t.Fatal("path traversal names must be rejected")
        }
        if _, err := r.Open(""); err == nil {
                t.Fatal("empty names must be rejected")
        }
        // Names listing
        names := r.Names()
        if len(names) != 1 || names[0] != "tenant-b" {
                t.Fatalf("names = %v", names)
        }
        // Custom data dir per name
        customDir := filepath.Join(base, "custom")
        c, err := r.Open("tenant-c", WithDataDir(customDir))
        if err != nil {
                t.Fatal(err)
        }
        if c.DataDir() != customDir {
                t.Fatalf("custom dir = %s", c.DataDir())
        }
        // CloseAll
        if err := r.CloseAll(testCtx); err != nil {
                t.Fatalf("close all: %v", err)
        }
        // Rejected after CloseAll
        if _, err := r.Open("late"); !errors.Is(err, ErrRepositoryClosing) {
                t.Fatalf("open after CloseAll = %v", err)
        }
        // Idempotent CloseAll
        if err := r.CloseAll(testCtx); err != nil {
                t.Fatalf("second CloseAll: %v", err)
        }
}

func TestRepositoryConcurrentOpen(t *testing.T) {
        base := t.TempDir()
        r := NewRepository(base, WithCacheSizeMB(8), WithMemTableSizeMB(4), WithZapLogger(zap.NewNop()))
        var wg sync.WaitGroup
        for i := 0; i < 8; i++ {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        s, err := r.Open("shared")
                        if err != nil {
                                t.Error(err)
                                return
                        }
                        _ = s.Set(testCtx, []byte("k"), []byte("v"))
                }()
        }
        wg.Wait()
        if len(r.Names()) != 1 {
                t.Fatalf("names = %v, want single instance", r.Names())
        }
        _ = r.CloseAll(testCtx)
}

func TestHealthCheck(t *testing.T) {
        s := openTestStore(t)
        h := s.HealthCheck(testCtx)
        if h.Status != "healthy" || !h.Healthy || !h.Alive() || !h.Ready() {
                t.Fatalf("health = %+v", h)
        }
        found := 0
        for _, c := range h.Checks {
                if c.Name == "open" || c.Name == "read_probe" || c.Name == "engine_pressure" {
                        found++
                }
        }
        if found != 3 {
                t.Fatalf("expected 3 checks, got %d: %+v", found, h.Checks)
        }

        // After close the store must report unhealthy.
        s2, _ := Open(WithDataDir(t.TempDir()), WithZapLogger(zap.NewNop()))
        _ = s2.Close()
        h2 := s2.HealthCheck(testCtx)
        if h2.Status != "unhealthy" || h2.Alive() {
                t.Fatalf("closed store health = %+v", h2)
        }
}

func TestCircuitBreakerTransitions(t *testing.T) {
        cb := NewCircuitBreaker(CircuitConfig{
                Enabled:           true,
                FailureThreshold:  3,
                FailureWindow:     time.Minute,
                Cooldown:          10 * time.Millisecond,
                HalfOpenSuccesses: 2,
        })
        var transitions []string
        cb.OnStateChange = func(from, to CircuitState) {
                transitions = append(transitions, from.String()+"->"+to.String())
        }

        if err := cb.Allow(); err != nil {
                t.Fatal("closed breaker must allow")
        }
        // Two failures: still closed.
        cb.Record(errors.New("io"))
        cb.Record(errors.New("io"))
        if cb.State() != CircuitClosed {
                t.Fatal("breaker must be closed below threshold")
        }
        // NotFound is not a failure.
        cb.Record(ErrNotFound)
        if cb.State() != CircuitClosed {
                t.Fatal("not-found must not count as failure")
        }
        // Third real failure trips.
        cb.Record(errors.New("io"))
        if cb.State() != CircuitOpen {
                t.Fatal("breaker must open at threshold")
        }
        if err := cb.Allow(); !errors.Is(err, ErrCircuitOpen) {
                t.Fatalf("open breaker Allow = %v", err)
        }
        // Cooldown elapses -> half-open.
        time.Sleep(20 * time.Millisecond)
        if cb.State() != CircuitHalfOpen {
                t.Fatal("breaker must go half-open after cooldown")
        }
        if err := cb.Allow(); err != nil {
                t.Fatalf("half-open must allow probes: %v", err)
        }
        // Failure while probing -> open again.
        cb.Record(errors.New("io"))
        if cb.State() != CircuitOpen {
                t.Fatal("probe failure must re-open")
        }
        time.Sleep(20 * time.Millisecond)
        _ = cb.Allow() // probe
        cb.Record(nil)
        time.Sleep(20 * time.Millisecond)
        _ = cb.Allow()
        cb.Record(nil)
        if cb.State() != CircuitClosed {
                t.Fatalf("consecutive probe successes must close: %s", cb.State())
        }
        if len(transitions) == 0 {
                t.Fatal("state-change hook must fire")
        }
}

func TestCircuitBreakerDefaultsAndFailureWindow(t *testing.T) {
        cfg := CircuitConfig{Enabled: true}
        cfg.defaults()
        if cfg.FailureThreshold != 50 || cfg.FailureWindow != 30*time.Second || cfg.Cooldown != 5*time.Second || cfg.HalfOpenSuccesses != 3 {
                t.Fatalf("defaults wrong: %+v", cfg)
        }

        // Failure window expiry resets the counter.
        cb := NewCircuitBreaker(CircuitConfig{
                FailureThreshold: 2, FailureWindow: 20 * time.Millisecond, Cooldown: time.Millisecond, HalfOpenSuccesses: 1,
        })
        cb.Record(errors.New("io"))
        time.Sleep(40 * time.Millisecond) // window expires
        cb.Record(errors.New("io"))
        if cb.State() != CircuitClosed {
                t.Fatal("failures outside the window must not trip")
        }
        // Successes do not count.
        cb.Record(nil)
        cb.Record(nil)
        if cb.State() != CircuitClosed {
                t.Fatal("successes must not trip")
        }
}

func TestStoreCircuitBreakerIntegration(t *testing.T) {
        // The breaker integrates with store ops: with a healthy engine nothing
        // trips; verify allow path and state exposure via metrics.
        s := openTestStore(t, WithCircuitBreaker(CircuitConfig{FailureThreshold: 5}))
        if s.breaker == nil {
                t.Fatal("breaker must be installed")
        }
        for i := 0; i < 100; i++ {
                _, _ = s.Get(testCtx, []byte("missing")) // ErrNotFound: never trips
        }
        if s.breaker.State() != CircuitClosed {
                t.Fatal("not-found storms must not trip the breaker")
        }
        snap := s.MetricsSnapshot()
        if snap.CircuitOpens != 0 || snap.CircuitState != "closed" {
                t.Fatalf("circuit metrics = %s/%d", snap.CircuitState, snap.CircuitOpens)
        }
}

func TestErrorTaxonomy(t *testing.T) {
        se := &StoreError{Op: "get", Key: []byte("k"), Err: ErrNotFound}
        if se.Error() != `bedrock: get key="k": bedrock: key not found` {
                t.Fatalf("error text = %q", se.Error())
        }
        seNoKey := &StoreError{Op: "close", Err: io.EOF}
        if seNoKey.Error() != "bedrock: close: EOF" {
                t.Fatalf("error text = %q", seNoKey.Error())
        }
        longKey := &StoreError{Op: "set", Key: []byte("0123456789012345678901234567890123456789012345678901234567890123456789"), Err: io.EOF}
        if len(longKey.Error()) > 130 {
                t.Fatalf("long key not truncated: %q", longKey.Error())
        }
        // Classify table
        cases := map[error]ErrorClass{
                nil:                   ClassOK,
                ErrNotFound:           ClassOK,
                ErrEmptyKey:           ClassOK,
                ErrCircuitOpen:        ClassTransient,
                ErrShuttingDown:       ClassTransient,
                ErrClosed:             ClassTransient,
                ErrInvalidConfig:      ClassPermanent,
                ErrInvalidOperation:   ClassPermanent,
                errors.New("unknown"): ClassTransient,
        }
        for err, want := range cases {
                if got := Classify(err); got != want {
                        t.Fatalf("Classify(%v) = %v, want %v", err, got, want)
                }
        }
        // StoreError.Classify unwraps.
        if got := (&StoreError{Err: ErrInvalidConfig}).Classify(); got != ClassPermanent {
                t.Fatalf("wrapped classify = %v", got)
        }
        if !errors.Is(&StoreError{Err: ErrNotFound}, ErrNotFound) {
                t.Fatal("StoreError must unwrap to cause")
        }
}

func TestOpTypeString(t *testing.T) {
        if OpSet.String() != "set" || OpDelete.String() != "delete" ||
                OpDeleteRange.String() != "delete_range" || OpMerge.String() != "merge" ||
                OpType(200).String() != "unknown" {
                t.Fatal("OpType.String wrong")
        }
}

func TestTracingIntegration(t *testing.T) {
        var spans atomic.Int64
        tracer := &countingTracer{count: &spans}
        s := openTestStore(t, WithTracer(tracer))
        _ = s.Set(testCtx, []byte("k"), []byte("v"))
        _, _ = s.Get(testCtx, []byte("k"))
        if spans.Load() < 2 {
                t.Fatalf("spans = %d, want >= 2", spans.Load())
        }
        // Noop default: no panics.
        s2 := openTestStore(t)
        _ = s2.Set(testCtx, []byte("k"), []byte("v"))
        if SpanFromContext(testCtx) != NoopSpan {
                t.Fatal("default span must be noop")
        }
        // nil tracer options normalize to noop
        cfg := DefaultConfig("x")
        WithTracer(nil)(cfg)
        if cfg.Tracer != NoopTracer {
                t.Fatal("nil tracer must become NoopTracer")
        }
}

type countingTracer struct{ count *atomic.Int64 }

func (c *countingTracer) StartSpan(ctx context.Context, op string, key []byte) (context.Context, Span) {
        c.count.Add(1)
        return ctx, &testSpan{count: c.count}
}

type testSpan struct{ count *atomic.Int64 }

func (s *testSpan) SetAttr(k, v string) {}
func (s *testSpan) End(err error)       { s.count.Add(1) }

func TestSlowOpLogging(t *testing.T) {
        s := openTestStore(t, WithSlowOpThreshold(time.Nanosecond))
        // Every op is slow with a 1ns threshold; must not panic and counter grows.
        _ = s.Set(testCtx, []byte("k"), []byte("v"))
        snap := s.MetricsSnapshot()
        if snap.SlowOps == 0 {
                t.Fatal("slow op counter must grow")
        }
        // Disabled
        s2 := openTestStore(t, WithSlowOpThreshold(-1))
        _ = s2.Set(testCtx, []byte("k"), []byte("v"))
        if s2.MetricsSnapshot().SlowOps != 0 {
                t.Fatal("slow ops disabled must not count")
        }
        // 0 is clamped to the 50ms default at open time
        s3 := openTestStore(t, WithSlowOpThreshold(0))
        if s3.cfg.SlowOpThreshold != 50*time.Millisecond {
                t.Fatalf("zero threshold = %v, want 50ms", s3.cfg.SlowOpThreshold)
        }
}

func TestContextFieldPropagation(t *testing.T) {
        type ctxKey struct{}
        s := openTestStore(t, WithContextFields(func(ctx context.Context) []zap.Field {
                if v, ok := ctx.Value(ctxKey{}).(string); ok {
                        return []zap.Field{zap.String("request_id", v)}
                }
                return nil
        }), WithSlowOpThreshold(time.Nanosecond))
        _ = s.Set(context.WithValue(testCtx, ctxKey{}, "req-1"), []byte("k"), []byte("v"))
}
