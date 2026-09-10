// Package tests contains black-box integration tests exercising
// bedrock through its public API only, on real filesystem-backed
// Pebble instances: recovery/reopen flows, crash-consistency via WAL
// replay, concurrent mixed workloads and checkpoint lifecycle.
package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Skryldev/bedrock"

	"go.uber.org/zap"
)

var ctx = context.Background()

func openIntegrationStore(t *testing.T, dir string, opts ...bedrock.Option) *bedrock.Store {
	t.Helper()
	base := []bedrock.Option{
		bedrock.WithDataDir(dir),
		bedrock.WithCacheSizeMB(8),
		bedrock.WithMemTableSizeMB(4),
		bedrock.WithZapLogger(zap.NewNop()),
	}
	base = append(base, opts...)
	s, err := bedrock.Open(base...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestIntegrationBasicLifecycle(t *testing.T) {
	dir := t.TempDir()
	s := openIntegrationStore(t, dir)

	// CRUD
	if err := s.Set(ctx, []byte("user:1"), []byte("alice")); err != nil {
		t.Fatal(err)
	}
	v, err := s.Get(ctx, []byte("user:1"))
	if err != nil || string(v) != "alice" {
		t.Fatalf("get: %q %v", v, err)
	}
	if err := s.Delete(ctx, []byte("user:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(ctx, []byte("user:1")); !errors.Is(err, bedrock.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}

	// Atomic batch
	err = s.Batch(ctx, []bedrock.Operation{
		{Type: bedrock.OpSet, Key: []byte("acct:1"), Value: []byte("100")},
		{Type: bedrock.OpSet, Key: []byte("acct:2"), Value: []byte("200")},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Scan
	it, err := s.Scan(ctx, []byte("acct:"), []byte("acct:~"))
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for ; it.Valid(); it.Next() {
		count++
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("scan found %d, want 2", count)
	}

	// Metrics snapshot is live
	snap := s.MetricsSnapshot()
	if snap.Ops["get"].OK == 0 {
		t.Fatal("get metrics missing")
	}
	if !contains(s.PrometheusMetrics(), "bedrock_operations_total") {
		t.Fatal("prometheus output missing")
	}

	// Health
	h := s.HealthCheck(ctx)
	if !h.Healthy {
		t.Fatalf("health: %+v", h)
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

// TestIntegrationReopenAfterClose verifies durability across a clean
// shutdown: every acknowledged write (SyncWrites=true) must reappear.
func TestIntegrationReopenAfterClose(t *testing.T) {
	dir := t.TempDir()
	s := openIntegrationStore(t, dir)

	const n = 500
	for i := 0; i < n; i++ {
		if err := s.Set(ctx, []byte(fmt.Sprintf("k/%06d", i)), []byte(fmt.Sprintf("v-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := bedrock.Open(
		bedrock.WithDataDir(dir),
		bedrock.WithCacheSizeMB(8),
		bedrock.WithMemTableSizeMB(4),
		bedrock.WithZapLogger(zap.NewNop()),
	)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	for i := 0; i < n; i++ {
		want := []byte(fmt.Sprintf("v-%d", i))
		got, err := s2.Get(ctx, []byte(fmt.Sprintf("k/%06d", i)))
		if err != nil || string(got) != string(want) {
			t.Fatalf("after reopen k/%06d = %q, %v", i, got, err)
		}
	}
}

// TestIntegrationWALReplayWithoutClose simulates a crash: the process
// abandons the DB handle (no Close), the directory is copied as-is, and a
// fresh instance replays the WAL. All synced writes must survive.
func TestIntegrationWALReplayWithoutClose(t *testing.T) {
	src := t.TempDir()
	s := openIntegrationStore(t, src)

	const n = 200
	for i := 0; i < n; i++ {
		if err := s.Set(ctx, []byte(fmt.Sprintf("w/%05d", i)), []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	// Abandon the handle: detach cleanup so Close is NOT called.
	// (Simulates abrupt process death; the OS releases file locks when
	// the test process would die. Copy the directory instead.)
	t.Cleanup(func() { _ = s.Close() })

	dst := filepath.Join(t.TempDir(), "copied")
	if err := copyTree(src, dst); err != nil {
		t.Fatalf("copy: %v", err)
	}
	s2, err := bedrock.Open(
		bedrock.WithDataDir(dst),
		bedrock.WithCacheSizeMB(8),
		bedrock.WithZapLogger(zap.NewNop()),
	)
	if err != nil {
		t.Fatalf("open copied dir: %v", err)
	}
	defer s2.Close()
	found := 0
	for i := 0; i < n; i++ {
		if _, err := s2.Get(ctx, []byte(fmt.Sprintf("w/%05d", i))); err == nil {
			found++
		}
	}
	// WAL replay guarantees all synced writes survive.
	if found != n {
		t.Fatalf("WAL replay recovered %d/%d keys", found, n)
	}
}

func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, path)
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
}

// TestIntegrationConcurrentMixedWorkload hammers one store with concurrent
// writers, readers, scanners and batchers — the race-detector's favorite.
func TestIntegrationConcurrentMixedWorkload(t *testing.T) {
	dir := t.TempDir()
	s := openIntegrationStore(t, dir, bedrock.WithHotCache(2<<20, time.Minute))

	var wg sync.WaitGroup
	const (
		writers  = 4
		readers  = 4
		batchers = 2
		scanners = 1
		rounds   = 300
	)
	errCh := make(chan error, writers+readers+batchers+scanners)

	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				k := []byte(fmt.Sprintf("w%d/%06d", id, i))
				if err := s.Set(ctx, k, []byte("v")); err != nil {
					errCh <- err
					return
				}
				if i%10 == 0 {
					if err := s.Delete(ctx, k); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(w)
	}
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds*2; i++ {
				if _, err := s.Get(ctx, []byte(fmt.Sprintf("w%d/%06d", i%writers, i%rounds))); err != nil {
					if !errors.Is(err, bedrock.ErrNotFound) {
						errCh <- err
						return
					}
				}
			}
		}()
	}
	for b := 0; b < batchers; b++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < rounds/3; i++ {
				ops := make([]bedrock.Operation, 0, 10)
				for j := 0; j < 10; j++ {
					ops = append(ops, bedrock.Operation{
						Type:  bedrock.OpSet,
						Key:   []byte(fmt.Sprintf("b%d/%06d/%02d", id, i, j)),
						Value: []byte("batched"),
					})
				}
				if err := s.Batch(ctx, ops); err != nil {
					errCh <- err
					return
				}
			}
		}(b)
	}
	for sc := 0; sc < scanners; sc++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				it, err := s.Scan(ctx, []byte("b0/"), []byte("b0/~"))
				if err != nil {
					errCh <- err
					return
				}
				for it.Valid() {
					it.Next()
				}
				if err := it.Close(); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("workload deadlocked")
	}
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
}

// TestIntegrationRepositoryMultiTenant exercises the repository with
// several isolated tenants, refcounted close and graceful CloseAll.
func TestIntegrationRepositoryMultiTenant(t *testing.T) {
	base := t.TempDir()
	r := bedrock.NewRepository(base,
		bedrock.WithCacheSizeMB(8),
		bedrock.WithMemTableSizeMB(4),
		bedrock.WithZapLogger(zap.NewNop()),
	)
	tenants := []string{"alpha", "beta", "gamma"}
	stores := make(map[string]*bedrock.Store)
	for _, tn := range tenants {
		s, err := r.Open(tn)
		if err != nil {
			t.Fatal(err)
		}
		stores[tn] = s
		if err := s.Set(ctx, []byte("who"), []byte(tn)); err != nil {
			t.Fatal(err)
		}
	}
	// Isolation
	for tn, s := range stores {
		v, err := s.Get(ctx, []byte("who"))
		if err != nil || string(v) != tn {
			t.Fatalf("tenant %s read %q, %v", tn, v, err)
		}
	}
	// Refcount: open twice, close once -> still usable.
	if _, err := r.Open("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := r.Close("alpha"); err != nil {
		t.Fatal(err)
	}
	if err := stores["alpha"].Set(ctx, []byte("k"), []byte("v")); err != nil {
		t.Fatalf("alpha must still be open: %v", err)
	}
	if err := r.CloseAll(ctx); err != nil {
		t.Fatalf("CloseAll: %v", err)
	}
}

// TestIntegrationCheckpointRoundTrip backs up, mutates, restores and
// verifies the snapshot contents on a real instance.
func TestIntegrationCheckpointRoundTrip(t *testing.T) {
	root := t.TempDir()
	s := openIntegrationStore(t, filepath.Join(root, "live"))

	for i := 0; i < 100; i++ {
		if err := s.Set(ctx, []byte(fmt.Sprintf("row/%04d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	backup := filepath.Join(root, "backups", "snap-1")
	if err := s.CreateCheckpoint(backup); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	// Post-backup mutations
	for i := 100; i < 150; i++ {
		_ = s.Set(ctx, []byte(fmt.Sprintf("row/%04d", i)), []byte("v"))
	}

	restoreDir := filepath.Join(root, "restored")
	if err := bedrock.RestoreCheckpoint(backup, restoreDir); err != nil {
		t.Fatalf("restore: %v", err)
	}
	r := openIntegrationStore(t, restoreDir)
	count := 0
	it, err := r.Scan(ctx, []byte("row/"), nil)
	if err != nil {
		t.Fatal(err)
	}
	for ; it.Valid(); it.Next() {
		count++
	}
	_ = it.Close()
	if count != 100 {
		t.Fatalf("restored %d rows, want exactly 100", count)
	}
}
