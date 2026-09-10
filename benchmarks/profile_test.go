package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"time"
	"testing"

	"github.com/Skryldev/bedrock"
	"go.uber.org/zap"
)

// TestLatencyProfile drives steady-state workloads through the store and
// prints the module's own latency percentiles (its HDR-style histograms).
// Run: go test -v -run TestLatencyProfile ./benchmarks/
// Output is used verbatim in docs/performance.md.
func TestLatencyProfile(t *testing.T) {
	if testing.Short() {
		t.Skip("latency profile skipped in -short mode")
	}
	ctx := context.Background()
	val := make([]byte, valueSize)

	printSnap := func(phase string, snap bedrock.MetricsSnapshot, ops ...string) {
		fmt.Printf("PHASE %s\n", phase)
		for _, op := range ops {
			st, ok := snap.Ops[op]
			if !ok || st.Latency.Count == 0 {
				continue
			}
			fmt.Printf("  %s: n=%d p50=%v p95=%v p99=%v p99.9=%v max=%v\n",
				op, st.Latency.Count, st.Latency.P50, st.Latency.P95,
				st.Latency.P99, st.Latency.P999, st.Latency.Max)
		}
	}

	// Phase 1: sequential point reads.
	func() {
		s := mustOpen(t, t.TempDir())
		defer s.Close()
		populate(t, s, datasetSize)
		for i := 0; i < 100_000; i++ {
			if _, err := s.Get(ctx, makeKey(i%datasetSize)); err != nil {
				t.Fatal(err)
			}
		}
		printSnap("sequential_get", s.MetricsSnapshot(), "get")
	}()

	// Phase 2: random point reads (random access pattern).
	func() {
		s := mustOpen(t, t.TempDir())
		defer s.Close()
		populate(t, s, datasetSize)
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 100_000; i++ {
			if _, err := s.Get(ctx, makeKey(rng.Intn(datasetSize))); err != nil {
				t.Fatal(err)
			}
		}
		printSnap("random_get", s.MetricsSnapshot(), "get")
	}()

	// Phase 3: durable writes (WAL fsync per commit).
	func() {
		s, err := bedrock.Open(
			bedrock.WithDataDir(t.TempDir()),
			bedrock.WithSyncWrites(true),
			bedrock.WithMemTableSizeMB(16),
			bedrock.WithCacheSizeMB(64),
			bedrock.WithZapLogger(zap.NewNop()),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		for i := 0; i < 20_000; i++ {
			if err := s.Set(ctx, makeKey(i), val); err != nil {
				t.Fatal(err)
			}
		}
		printSnap("sync_set", s.MetricsSnapshot(), "set")
	}()

	// Phase 4: batched writes, 100 ops per batch.
	func() {
		s := mustOpen(t, t.TempDir())
		defer s.Close()
		populate(t, s, datasetSize)
		rng := rand.New(rand.NewSource(2))
		for i := 0; i < 1_000; i++ {
			ops := make([]bedrock.Operation, 100)
			for j := range ops {
				ops[j] = bedrock.Operation{
					Type:  bedrock.OpSet,
					Key:   makeKey(rng.Intn(datasetSize * 2)),
					Value: val,
				}
			}
			if err := s.Batch(ctx, ops); err != nil {
				t.Fatal(err)
			}
		}
		printSnap("batch_set_100", s.MetricsSnapshot(), "batch")
	}()

	// Phase 5: mixed workload with hot cache (95/5 read/write).
	func() {
		s, err := bedrock.Open(
			bedrock.WithDataDir(t.TempDir()),
			bedrock.WithMemTableSizeMB(16),
			bedrock.WithCacheSizeMB(64),
			bedrock.WithHotCache(32<<20, 10*time.Minute),
			bedrock.WithZapLogger(zap.NewNop()),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		populate(t, s, datasetSize)
		rng := rand.New(rand.NewSource(3))
		for i := 0; i < 200_000; i++ {
			key := makeKey(rng.Intn(hotKeyCount)) // zipf-ish hot set
			if rng.Intn(100) < 95 {
				if _, err := s.Get(ctx, key); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := s.Set(ctx, key, val); err != nil {
					t.Fatal(err)
				}
			}
		}
		printSnap("mixed_95_5_hotcache", s.MetricsSnapshot(), "get", "set")
	}()
}
