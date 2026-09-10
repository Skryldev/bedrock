// Command example demonstrates the complete Bedrock lifecycle:
// configuration, open, CRUD, batch, scan, TTL entries, checkpoint backup,
// metrics and graceful shutdown. It is intentionally slim — no HTTP —
// so the lifecycle is readable in one screen.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Skryldev/bedrock"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()

	// Structured logging: route store events into zap.
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	dataDir := "./data/example"
	if len(os.Args) > 1 {
		dataDir = os.Args[1]
	}

	// 1. Open with functional options.
	store, err := bedrock.Open(
		bedrock.WithDataDir(dataDir),
		bedrock.WithName("example"),
		bedrock.WithCacheSizeMB(32),
		bedrock.WithMemTableSizeMB(16),
		bedrock.WithSyncWrites(true),
		bedrock.WithHotCache(32<<20, 5*time.Minute),
		bedrock.WithCircuitBreaker(bedrock.CircuitConfig{}),
		bedrock.WithZapLogger(logger),
	)
	if err != nil {
		log.Fatalf("open: %v", err)
	}

	// 2. CRUD.
	if err := store.Set(ctx, []byte("greeting"), []byte("hello, pebble")); err != nil {
		log.Fatal(err)
	}
	v, err := store.Get(ctx, []byte("greeting"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("get greeting      -> %s\n", v)

	// 3. TTL-pinned hot-cache entry.
	_ = store.SetWithTTL(ctx, []byte("session:42"), []byte("active"), time.Minute)

	// 4. Atomic batch.
	err = store.Batch(ctx, []bedrock.Operation{
		{Type: bedrock.OpSet, Key: []byte("acct:1"), Value: []byte("100")},
		{Type: bedrock.OpSet, Key: []byte("acct:2"), Value: []byte("250")},
		{Type: bedrock.OpMerge, Key: []byte("log"), Value: []byte("batch-applied;")},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("batch applied     -> acct:1, acct:2, log")

	// 5. Range scan.
	it, err := store.Scan(ctx, []byte("acct:"), []byte("acct:~"))
	if err != nil {
		log.Fatal(err)
	}
	for ; it.Valid(); it.Next() {
		fmt.Printf("scan %-12s -> %s\n", it.Key(), it.Value())
	}
	if err := it.Close(); err != nil {
		log.Fatal(err)
	}

	// 6. Streaming write batch (reusable, pooled).
	wb := store.NewWriteBatch()
	for i := 0; i < 5; i++ {
		_ = wb.Set([]byte(fmt.Sprintf("stream/%d", i)), []byte("payload"))
	}
	if err := wb.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	_ = wb.Close()
	fmt.Println("write batch       -> stream/0..4 committed atomically")

	// 7. Checkpoint backup (crash-consistent, hardlink-based).
	backup := "./data/example-backup"
	_ = os.RemoveAll(backup)
	if err := store.CreateCheckpoint(backup); err != nil {
		log.Fatalf("checkpoint: %v", err)
	}
	fmt.Printf("checkpoint        -> %s\n", backup)

	// 8. Health + metrics.
	h := store.HealthCheck(ctx)
	fmt.Printf("health            -> %s (%d checks)\n", h.Status, len(h.Checks))
	snap := store.MetricsSnapshot()
	fmt.Printf("metrics           -> get.ok=%d set.ok=%d uptime=%s\n",
		snap.Ops["get"].OK, snap.Ops["set"].OK, snap.Uptime.Round(time.Millisecond))
	fmt.Printf("prometheus        -> %d bytes of exposition text\n", len(store.PrometheusMetrics()))

	// 9. Graceful shutdown: drains in-flight ops, flushes WAL, closes.
	if err := store.Close(); err != nil {
		log.Fatalf("close: %v", err)
	}
	fmt.Println("closed cleanly")
}
