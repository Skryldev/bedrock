// Package benchmarks measures the Bedrock abstraction against raw
// Pebble and in-memory baselines across sequential, random and mixed
// workloads. Run with:
//
//      go test -bench . -benchmem ./benchmarks/
package benchmarks

import (
        "context"
        "encoding/binary"
        "fmt"
        "math/rand"
        "sync"
        "testing"
        "time"

        "github.com/Skryldev/bedrock"

        "github.com/cockroachdb/pebble"
        "go.uber.org/zap"
)

const (
        datasetSize  = 100_000 // keys pre-populated per benchmark DB
        valueSize    = 100     // bytes per value
        hotKeyCount  = 1_000   // zipf-ish hot set for cache benchmarks
        keyPrefixLen = 8       // fixed-width prefix of every key
)

var benchCtx = context.Background()

// --- key layout helpers -------------------------------------------------

// makeKey builds "kp-<8 hex>" keys with a zero-padded 8-byte numeric tail
// so lexicographic order == numeric order (scan benchmarks stay linear).
func makeKey(i int) []byte {
        k := make([]byte, keyPrefixLen+8)
        copy(k, "kp-")
        binary.BigEndian.PutUint64(k[keyPrefixLen:], uint64(i))
        return k
}

func mustOpen(tb testing.TB, dir string, opts ...bedrock.Option) *bedrock.Store {
        tb.Helper()
        base := []bedrock.Option{
                bedrock.WithDataDir(dir),
                bedrock.WithCacheSizeMB(64),
                bedrock.WithMemTableSizeMB(16),
                bedrock.WithSyncWrites(false), // throughput-oriented default
                bedrock.WithZapLogger(zap.NewNop()),
        }
        base = append(base, opts...)
        s, err := bedrock.Open(base...)
        if err != nil {
                tb.Fatalf("open: %v", err)
        }
        tb.Cleanup(func() { _ = s.Close() })
        return s
}

// populate seeds n keys using large batches (fast path).
func populate(tb testing.TB, s *bedrock.Store, n int) {
        tb.Helper()
        const batch = 5_000
        val := make([]byte, valueSize)
        for i := range val {
                val[i] = byte('a' + i%26)
        }
        ops := make([]bedrock.Operation, 0, batch)
        for i := 0; i < n; i++ {
                ops = append(ops, bedrock.Operation{
                        Type:  bedrock.OpSet,
                        Key:   makeKey(i),
                        Value: val,
                })
                if len(ops) == batch {
                        if err := s.Batch(benchCtx, ops); err != nil {
                                tb.Fatal(err)
                        }
                        ops = ops[:0]
                }
        }
        if len(ops) > 0 {
                if err := s.Batch(benchCtx, ops); err != nil {
                        tb.Fatal(err)
                }
        }
        if err := s.Flush(benchCtx); err != nil {
                tb.Fatal(err)
        }
}

func benchName(dir string) string { return dir }

var _ = benchName // reserved for future per-store naming

// --- GET ----------------------------------------------------------------

func BenchmarkGet(b *testing.B) {
        for _, tc := range []struct {
                name    string
                hotDB   bool
        }{
                {"miss_no_cache", false},
                {"hit_hotcache", true},
        } {
                b.Run(tc.name, func(b *testing.B) {
                        var opts []bedrock.Option
                        if tc.hotDB {
                                opts = append(opts, bedrock.WithHotCache(32<<20, time.Minute))
                        }
                        s := mustOpen(b, b.TempDir(), opts...)
                        populate(b, s, datasetSize)

                        b.ResetTimer()
                        b.RunParallel(func(pb *testing.PB) {
                                i := 0
                                for pb.Next() {
                                        key := makeKey(i % hotKeyCount) // zipf-ish hot set
                                        if _, err := s.Get(benchCtx, key); err != nil {
                                                b.Fatal(err)
                                        }
                                        i++
                                }
                        })
                        b.ReportMetric(float64(b.N)/b.Elapsed().Seconds()/1e6, "Mops/s")
                })
        }
}

func BenchmarkGetSequential(b *testing.B) {
        s := mustOpen(b, b.TempDir())
        populate(b, s, datasetSize)
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
                if _, err := s.Get(benchCtx, makeKey(i%datasetSize)); err != nil {
                        b.Fatal(err)
                }
        }
}

func BenchmarkGetRandom(b *testing.B) {
        s := mustOpen(b, b.TempDir())
        populate(b, s, datasetSize)
        rng := rand.New(rand.NewSource(42))
        idx := make([]int, 65536)
        for i := range idx {
                idx[i] = rng.Intn(datasetSize)
        }
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
                if _, err := s.Get(benchCtx, makeKey(idx[i&65535])); err != nil {
                        b.Fatal(err)
                }
        }
}

func BenchmarkGetParallelRandom(b *testing.B) {
        s := mustOpen(b, b.TempDir())
        populate(b, s, datasetSize)
        rng := rand.New(rand.NewSource(7))
        idx := make([]int, 65536)
        for i := range idx {
                idx[i] = rng.Intn(datasetSize)
        }
        b.ResetTimer()
        b.RunParallel(func(pb *testing.PB) {
                counter := 0
                for pb.Next() {
                        key := makeKey(idx[counter&65535])
                        if _, err := s.Get(benchCtx, key); err != nil {
                                b.Fatal(err)
                        }
                        counter++
                }
        })
}

// --- SET ----------------------------------------------------------------

func BenchmarkSet(b *testing.B) {
        for _, sync := range []bool{false, true} {
                name := "nosync"
                if sync {
                        name = "sync"
                }
                b.Run(name, func(b *testing.B) {
                        s := mustOpen(b, b.TempDir(), bedrock.WithSyncWrites(sync))
                        b.ResetTimer()
                        for i := 0; i < b.N; i++ {
                                if err := s.Set(benchCtx, makeKey(i), make([]byte, valueSize)); err != nil {
                                        b.Fatal(err)
                                }
                        }
                })
        }
}

func BenchmarkSetParallel(b *testing.B) {
        s := mustOpen(b, b.TempDir())
        b.ResetTimer()
        b.RunParallel(func(pb *testing.PB) {
                i := 0
                for pb.Next() {
                        if err := s.Set(benchCtx, makeKey(i), make([]byte, valueSize)); err != nil {
                                b.Fatal(err)
                        }
                        i++
                }
        })
}

// --- DELETE -------------------------------------------------------------

func BenchmarkDelete(b *testing.B) {
        s := mustOpen(b, b.TempDir())
        populate(b, s, datasetSize)
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
                if err := s.Delete(benchCtx, makeKey(i%datasetSize)); err != nil {
                        b.Fatal(err)
                }
        }
}

// --- BATCH --------------------------------------------------------------

func BenchmarkBatchSet(b *testing.B) {
        for _, size := range []int{10, 100, 1000} {
                b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
                        s := mustOpen(b, b.TempDir())
                        val := make([]byte, valueSize)
                        b.ResetTimer()
                        for i := 0; i < b.N; i++ {
                                ops := make([]bedrock.Operation, size)
                                for j := 0; j < size; j++ {
                                        ops[j] = bedrock.Operation{
                                                Type:  bedrock.OpSet,
                                                Key:   makeKey((i*size + j) % (datasetSize * 2)),
                                                Value: val,
                                        }
                                }
                                if err := s.Batch(benchCtx, ops); err != nil {
                                        b.Fatal(err)
                                }
                        }
                        b.ReportMetric(float64(b.N)*float64(size)/b.Elapsed().Seconds(), "ops/s")
                })
        }
}

// --- SCAN ---------------------------------------------------------------

func BenchmarkScan(b *testing.B) {
        for _, tc := range []struct {
                name  string
                n     int
                pref  int
        }{
                {"keys_10", 10, 0},
                {"keys_1000", 1000, 0},
                {"keys_1000_prefetch", 1000, 256},
        } {
                b.Run(tc.name, func(b *testing.B) {
                        s := mustOpen(b, b.TempDir())
                        populate(b, s, datasetSize)
                        start := makeKey(0)
                        end := makeKey(tc.n)
                        b.ResetTimer()
                        for i := 0; i < b.N; i++ {
                                it, err := s.ScanWithOptions(benchCtx, start, end, bedrock.ScanOptions{Prefetch: tc.pref})
                                if err != nil {
                                        b.Fatal(err)
                                }
                                for ; it.Valid(); it.Next() {
                                }
                                if err := it.Close(); err != nil {
                                        b.Fatal(err)
                                }
                        }
                        b.ReportMetric(float64(b.N)*float64(tc.n)/b.Elapsed().Seconds(), "keys/s")
                })
        }
}

// --- MIXED WORKLOAD -----------------------------------------------------

// BenchmarkMixedWorkload runs read-heavy and balanced mixes with concurrent
// workers. Reported ops/s is the per-operation average across the mix.
func BenchmarkMixedWorkload(b *testing.B) {
        for _, tc := range []struct {
                name     string
                readPct  int // percentage of reads (rest writes)
                hotCache bool
        }{
                {"read_heavy_95_5", 95, false},
                {"read_heavy_95_5_hotcache", 95, true},
                {"balanced_50_50", 50, false},
        } {
                b.Run(tc.name, func(b *testing.B) {
                        var opts []bedrock.Option
                        if tc.hotCache {
                                opts = append(opts, bedrock.WithHotCache(32<<20, time.Minute))
                        }
                        s := mustOpen(b, b.TempDir(), opts...)
                        populate(b, s, datasetSize)

                        rng := rand.New(rand.NewSource(99))
                        thresholds := make([]int, 65536)
                        for i := range thresholds {
                                thresholds[i] = rng.Intn(100)
                        }
                        b.ResetTimer()
                        b.RunParallel(func(pb *testing.PB) {
                                i, counter := 0, 0
                                for pb.Next() {
                                        if thresholds[counter&65535] < tc.readPct {
                                                if _, err := s.Get(benchCtx, makeKey(i%datasetSize)); err != nil {
                                                        b.Fatal(err)
                                                }
                                        } else {
                                                if err := s.Set(benchCtx, makeKey(i%datasetSize), make([]byte, valueSize)); err != nil {
                                                        b.Fatal(err)
                                                }
                                        }
                                        i++
                                        counter++
                                }
                        })
                        b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
                })
        }
}

// --- BASELINES ----------------------------------------------------------

// BenchmarkBaselineRWMutexMap is the in-process memory baseline: a Go map
// behind an RWMutex. It bounds what an in-memory abstraction can achieve
// and contextualizes the LSM numbers (map has zero durability).
func BenchmarkBaselineRWMutexMap(b *testing.B) {
        m := make(map[string][]byte, datasetSize)
        var mu sync.RWMutex
        val := make([]byte, valueSize)
        for i := 0; i < datasetSize; i++ {
                m[string(makeKey(i))] = val
        }
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
                switch i % 2 {
                case 0:
                        mu.RLock()
                        _ = m[string(makeKey(i%datasetSize))]
                        mu.RUnlock()
                default:
                        mu.Lock()
                        m[string(makeKey(i%datasetSize))] = val
                        mu.Unlock()
                }
        }
}

// BenchmarkRawPebbleSet measures un-wrapped Pebble to quantify the
// abstraction overhead of Bedrock.
func BenchmarkRawPebbleGet(b *testing.B) {
        dir := b.TempDir()
        db, err := pebble.Open(dir, &pebble.Options{
                MemTableSize: 16 << 20,
        })
        if err != nil {
                b.Fatal(err)
        }
        defer db.Close()
        val := make([]byte, valueSize)
        for i := 0; i < datasetSize; i++ {
                if err := db.Set(makeKey(i), val, nil); err != nil {
                        b.Fatal(err)
                }
        }
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
                v, closer, err := db.Get(makeKey(i % datasetSize))
                if err != nil {
                        b.Fatal(err)
                }
                _ = v
                _ = closer.Close()
        }
}

func BenchmarkRawPebbleSet(b *testing.B) {
        dir := b.TempDir()
        db, err := pebble.Open(dir, &pebble.Options{
                MemTableSize: 16 << 20,
        })
        if err != nil {
                b.Fatal(err)
        }
        defer db.Close()
        b.ResetTimer()
        for i := 0; i < b.N; i++ {
                if err := db.Set(makeKey(i), make([]byte, valueSize), nil); err != nil {
                        b.Fatal(err)
                }
        }
}
