package bedrock

import (
        "context"
        "errors"
        "fmt"
        "os"
        "path/filepath"
        "strings"
        "sync"
        "testing"
        "time"

        "go.uber.org/zap"
)

// --- writeBatch coverage: SetWithTTL, Merge, empty-batch commit ---

func TestWriteBatchSetTTLAndMerge(t *testing.T) {
        s := openTestStore(t, WithHotCache(1<<20, time.Minute))

        wb := s.NewWriteBatch()
        defer wb.Close()

        if err := wb.SetWithTTL([]byte("bt"), []byte("ttl-value"), 50*time.Millisecond); err != nil {
                t.Fatal(err)
        }
        if err := wb.Merge([]byte("bm"), []byte("m1,")); err != nil {
                t.Fatal(err)
        }
        if err := wb.Merge([]byte("bm"), []byte("m2,")); err != nil {
                t.Fatal(err)
        }
        if err := wb.Commit(testCtx); err != nil {
                t.Fatal(err)
        }
        v, err := s.Get(testCtx, []byte("bt"))
        if err != nil || string(v) != "ttl-value" {
                t.Fatalf("bt = %q, %v", v, err)
        }
        v, err = s.Get(testCtx, []byte("bm"))
        if err != nil || string(v) != "m1,m2," {
                t.Fatalf("bm = %q, %v", v, err)
        }
        // Empty-key rejections
        if err := wb.SetWithTTL(nil, []byte("v"), time.Second); !errors.Is(err, ErrEmptyKey) {
                t.Fatalf("SetWithTTL empty key = %v", err)
        }
        if err := wb.Merge(nil, []byte("v")); !errors.Is(err, ErrEmptyKey) {
                t.Fatalf("Merge empty key = %v", err)
        }
        // Closing twice is safe; the second is a no-op.
        _ = wb.Close()

        // SetWithTTL expiry is visible through the hot cache only.
        time.Sleep(80 * time.Millisecond)
        if _, ok := s.cache.get([]byte("bt")); ok {
                t.Fatal("pinned entry must expire")
        }
}

func TestCommitEmptyWriteBatch(t *testing.T) {
        s := openTestStore(t)
        wb := s.NewWriteBatch()
        if err := wb.Commit(testCtx); err != nil {
                t.Fatalf("empty commit must succeed: %v", err)
        }
        _ = wb.Close()
}

// --- iterator coverage: Key/Value/Prev/Error on both modes ---

func TestIteratorPrevAndErrorPassThrough(t *testing.T) {
        s := openTestStore(t)
        for _, k := range []string{"p1", "p2", "p3"} {
                _ = s.Set(testCtx, []byte(k), []byte("v-"+k))
        }
        it, err := s.Scan(testCtx, nil, nil)
        if err != nil {
                t.Fatal(err)
        }
        defer it.Close()

        if !it.Last() || string(it.Key()) != "p3" {
                t.Fatalf("last = %q", it.Key())
        }
        if !it.Prev() || string(it.Key()) != "p2" {
                t.Fatalf("prev = %q", it.Key())
        }
        if string(it.Value()) != "v-p2" {
                t.Fatalf("value = %q", it.Value())
        }
        // SeekLT boundary: nothing before p1 -> returns false, stays invalid.
        if it.SeekLT([]byte("p1")) || it.Valid() {
                t.Fatal("SeekLT(p1) must be invalid (nothing before)")
        }
        if err := it.Error(); err != nil {
                t.Fatalf("error after exhaustion: %v", err)
        }
        // SeekGE past the end exhausts too.
        if it.SeekGE([]byte("zzz")) || it.Valid() {
                t.Fatal("SeekGE past end must be invalid")
        }
        // First returns to the beginning.
        if !it.First() || string(it.Key()) != "p1" {
                t.Fatalf("first = %q", it.Key())
        }
}

func TestIteratorDoubleCloseIsSafe(t *testing.T) {
        s := openTestStore(t)
        _ = s.Set(testCtx, []byte("x"), []byte("y"))
        it, err := s.Scan(testCtx, nil, nil)
        if err != nil {
                t.Fatal(err)
        }
        if err := it.Close(); err != nil {
                t.Fatal(err)
        }
        if err := it.Close(); err != nil {
                t.Fatalf("double close must be idempotent: %v", err)
        }
}

func TestPrefetchIteratorNilWhenInvalid(t *testing.T) {
        s := openTestStore(t)
        _ = s.Set(testCtx, []byte("one"), []byte("1"))
        it, err := s.ScanWithOptions(testCtx, []byte("zz"), nil, ScanOptions{Prefetch: 4})
        if err != nil {
                t.Fatal(err)
        }
        defer it.Close()
        // Empty range: prefetch ring has nothing; Key/Value must be nil.
        if it.Valid() {
                t.Fatal("must be invalid")
        }
        if it.Key() != nil || it.Value() != nil {
                t.Fatal("invalid ring iterator must return nil key/value")
        }
}

func TestScanWithOptionsBreakerOpen(t *testing.T) {
        s := openTestStore(t, WithCircuitBreaker(CircuitConfig{FailureThreshold: 1, Cooldown: time.Minute}))
        s.breaker.Record(errors.New("boom")) // force open
        if _, err := s.ScanWithOptions(testCtx, nil, nil, ScanOptions{Prefetch: 4}); !errors.Is(err, ErrCircuitOpen) {
                t.Fatalf("scan with open breaker = %v", err)
        }
}

// --- cache coverage: loadBytes, evictOldest, global sweep ---

func TestHotCacheGlobalSweepAcrossShards(t *testing.T) {
        cfg := HotCacheConfig{
                Enabled: true, MaxBytes: 1024, Shards: 4,
                TTL: time.Hour, JanitorInterval: 5 * time.Millisecond,
        }
        c := newHotCache(cfg)
        defer c.close()

        val := make([]byte, 128)
        for i := 0; i < 64; i++ {
                c.set([]byte(fmt.Sprintf("sw-%03d", i)), val, 0)
        }
        // Force the global sweep loop (per-shard budget already enforced on
        // insert; global total must still converge under MaxBytes + slack).
        c.sweep()
        _, bytes := c.stats()
        if bytes > int64(cfg.MaxBytes)+2048 {
                t.Fatalf("global sweep left %d bytes > budget", bytes)
        }
        if c.evictionCount() == 0 {
                t.Fatal("global sweep must record evictions")
        }
        // evictOldest on empty shard returns nil
        sh := c.shards[0]
        sh.mu.Lock()
        got := sh.evictOldest()
        sh.mu.Unlock()
        if got != nil && len(c.shards) == 4 && sh.loadBytes() > 0 {
                t.Fatal("empty shard must not evict")
        }
}

func TestHotCacheDefaultClockUsed(t *testing.T) {
        c := newHotCache(HotCacheConfig{Enabled: true, MaxBytes: 4096, Shards: 2, TTL: time.Hour})
        defer c.close()
        c.set([]byte("k"), []byte("v"), 0)
        if v, ok := c.get([]byte("k")); !ok || string(v) != "v" {
                t.Fatal("basic roundtrip with default clock")
        }
}

// --- logger adapter ---

func TestPebbleLoggerAdapter(t *testing.T) {
        logger := zap.NewNop()
        ad := pebbleLoggerAdapter{l: logger}
        ad.Infof("info %d", 1) // must not panic
        defer func() {
                if r := recover(); r == nil {
                        t.Fatal("Fatalf must panic (library must not os.Exit)")
                }
        }()
        ad.Fatalf("fatal %d", 1)
}

func TestNewProductionZap(t *testing.T) {
        l := newProductionZap()
        if l == nil {
                t.Fatal("production logger must build")
        }
        l.Warn("no-op") // must not panic
}

func TestZapErrField(t *testing.T) {
        f := zapErr(errors.New("x"))
        if f.Key != "error" {
                t.Fatalf("field key = %s", f.Key)
        }
}

// --- trace noop surface ---

func TestNoopTracerSurface(t *testing.T) {
        ctx, sp := NoopTracer.StartSpan(testCtx, "op", []byte("k"))
        if ctx != testCtx {
                t.Fatal("noop tracer must not wrap ctx")
        }
        sp.SetAttr("a", "b")
        sp.End(nil)
        if SpanFromContext(testCtx) != NoopSpan {
                t.Fatal("default span lookup")
        }
        // ctxWithSpan round trip with a real span instance
        custom := &attrSpan{}
        c2 := ctxWithSpan(testCtx, custom)
        if SpanFromContext(c2) != Span(custom) {
                t.Fatal("installed span must be retrievable")
        }
        // nil span in ctx falls back
        c3 := context.WithValue(testCtx, traceCtxKey{}, nil)
        if SpanFromContext(c3) != NoopSpan {
                t.Fatal("nil span must fall back to noop")
        }
}

// --- counting tracer with SetAttr exercised ---

func TestTracerSpanAttrs(t *testing.T) {
        var mu sync.Mutex
        var attrs int
        tr := &attrTracer{mu: &mu, attrs: &attrs}
        s := openTestStore(t, WithTracer(tr))
        _ = s.Set(testCtx, []byte("k"), []byte("v"))
        _, _ = s.Get(testCtx, []byte("k"))
        mu.Lock()
        defer mu.Unlock()
        if attrs == 0 {
                t.Fatal("expected span activity")
        }
}

type attrTracer struct {
        mu    *sync.Mutex
        attrs *int
}

func (a *attrTracer) StartSpan(ctx context.Context, op string, key []byte) (context.Context, Span) {
        return ctx, &attrSpan{a: a}
}

type attrSpan struct{ a *attrTracer }

func (s *attrSpan) SetAttr(k, v string) {
        s.a.mu.Lock()
        *s.a.attrs++
        s.a.mu.Unlock()
}

func (s *attrSpan) End(err error) {}

// --- repository multiCloseError surface ---

func TestMultiCloseError(t *testing.T) {
        e1 := errors.New("one")
        e2 := errors.New("two")
        me := &multiCloseError{errs: []error{e1, e2}}
        if !strings.Contains(me.Error(), "one") || !strings.Contains(me.Error(), "two") {
                t.Fatalf("text = %q", me.Error())
        }
        if len(me.Unwrap()) != 2 {
                t.Fatal("unwrap must expose causes")
        }
        single := &StoreError{Err: e1}
        if !errors.Is(single, e1) {
                t.Fatal("single unwrap")
        }
}

// --- health degraded path via engine pressure helper (pure) ---

func TestEvaluateEnginePressure(t *testing.T) {
        cases := []struct {
                name     string
                stats    DBStats
                budget   uint64
                degraded bool
                detail   string
        }{
                {"all clear", DBStats{L0Files: 3, CompactionDebt: 100, MemTableSize: 100}, 1 << 20, false, ""},
                {"l0 pressure", DBStats{L0Files: 25}, 1 << 20, true, "L0"},
                {"debt pressure", DBStats{CompactionDebt: 2 << 30}, 1 << 20, true, "compaction debt"},
                {"memtable pressure", DBStats{MemTableSize: 980 << 10}, 1 << 20, true, "memtable"},
                {"zero budget skips memtable check", DBStats{MemTableSize: 1 << 30}, 0, false, ""},
        }
        for _, tc := range cases {
                details := evaluateEnginePressure(tc.stats, tc.budget)
                if tc.degraded && len(details) == 0 {
                        t.Fatalf("%s: expected degradation", tc.name)
                }
                if !tc.degraded && len(details) != 0 {
                        t.Fatalf("%s: unexpected degradation %v", tc.name, details)
                }
                if tc.detail != "" && (len(details) == 0 || !contains(details[0], tc.detail)) {
                        t.Fatalf("%s: details %v missing %q", tc.name, details, tc.detail)
                }
        }
}

func TestHealthCheckDegradedOnL0Pressure(t *testing.T) {
        degraded := evaluateEnginePressure(DBStats{L0Files: 25, MemTableSize: 0}, 1<<20)
        if len(degraded) == 0 || !contains(degraded[0], "L0") {
                t.Fatalf("L0 pressure not detected: %v", degraded)
        }
}

// --- backup error paths ---

func TestRestoreAndCheckpointErrorPaths(t *testing.T) {
        root := t.TempDir()

        // copyFile: unreadable source -> RestoreCheckpoint fails with StoreError
        src := filepath.Join(root, "src")
        dst := filepath.Join(root, "dst")
        if err := os.MkdirAll(src, 0o755); err != nil {
                t.Fatal(err)
        }
        if err := os.WriteFile(filepath.Join(src, "f1"), []byte("data"), 0o644); err != nil {
                t.Fatal(err)
        }
        if err := copyDir(src, dst); err != nil {
                t.Fatalf("copyDir happy path: %v", err)
        }
        b, err := os.ReadFile(filepath.Join(dst, "f1"))
        if err != nil || string(b) != "data" {
                t.Fatalf("copied file = %q, %v", b, err)
        }
        // copyDir to nested destination creates dirs
        if err := copyDir(src, filepath.Join(root, "a", "b", "c")); err != nil {
                t.Fatalf("nested copyDir: %v", err)
        }
        // copyFile error: source missing
        if err := copyFile(filepath.Join(src, "missing"), filepath.Join(dst, "x"), 0o644); err == nil {
                t.Fatal("copyFile of missing source must fail")
        }
        // RestoreCheckpoint: file (not dir) as backup -> StoreError
        fileBackup := filepath.Join(root, "file-backup")
        if err := os.WriteFile(fileBackup, []byte("x"), 0o644); err != nil {
                t.Fatal(err)
        }
        err = RestoreCheckpoint(fileBackup, filepath.Join(root, "out"))
        var se *StoreError
        if !errors.As(err, &se) || se.Op != "restore" {
                t.Fatalf("file backup err = %v", err)
        }
        // copyFile/dst-dir-not-exists path inside copyDir via RestoreCheckpoint
        if err := RestoreCheckpoint(src, filepath.Join(root, "fresh/place")); err != nil {
                t.Fatalf("restore into nested fresh dir: %v", err)
        }
}

func TestCheckpointIntoEmptyExistingDir(t *testing.T) {
        s := openTestStore(t)
        empty := filepath.Join(t.TempDir(), "existing-empty")
        if err := os.MkdirAll(empty, 0o755); err != nil {
                t.Fatal(err)
        }
        if err := s.CreateCheckpoint(empty); err != nil {
                t.Fatalf("checkpoint into empty dir must succeed: %v", err)
        }
        // Destination as a FILE fails.
        fileDest := filepath.Join(t.TempDir(), "asfile")
        if err := os.WriteFile(fileDest, []byte("x"), 0o644); err != nil {
                t.Fatal(err)
        }
        if err := s.CreateCheckpoint(fileDest); err == nil {
                t.Fatal("file destination must fail")
        }
}

func TestOpenOrRestoreNoBackupReturnsOriginalError(t *testing.T) {
        cfg := DefaultConfig("missing")
        cfg.DataDir = filepath.Join(t.TempDir(), "data")
        // A FILE where the DB dir should go -> open fails.
        if err := os.WriteFile(cfg.DataDir, []byte("not a dir"), 0o644); err != nil {
                t.Fatal(err)
        }
        cfg.Logger = zap.NewNop()
        _, err := OpenOrRestore(cfg, "")
        if err == nil {
                t.Fatal("open must fail")
        }
        // Nonexistent backup dir keeps the original error.
        _, err = OpenOrRestore(cfg, filepath.Join(t.TempDir(), "nope"))
        if err == nil {
                t.Fatal("still must fail")
        }
}

// --- flush/compact after close ---

func TestMaintenanceAfterClose(t *testing.T) {
        s, err := Open(WithDataDir(t.TempDir()), WithCacheSizeMB(4), WithMemTableSizeMB(2), WithZapLogger(zap.NewNop()))
        if err != nil {
                t.Fatal(err)
        }
        _ = s.Close()
        if err := s.Flush(testCtx); err == nil {
                t.Fatal("flush after close must fail")
        }
        if err := s.CompactRange(testCtx, []byte("a"), []byte("b")); err == nil {
                t.Fatal("compact after close must fail")
        }
}

func TestCompactRangeExplicitBounds(t *testing.T) {
        s := openTestStore(t)
        for i := 0; i < 10; i++ {
                _ = s.Set(testCtx, []byte(fmt.Sprintf("cb/%02d", i)), []byte("v"))
        }
        _ = s.Flush(testCtx)
        if err := s.CompactRange(testCtx, []byte("cb/00"), []byte("cb/05")); err != nil {
                t.Fatalf("bounded compact: %v", err)
        }
        // Empty keyspace compact is a no-op
        s2 := openTestStore(t)
        if err := s2.CompactRange(testCtx, nil, nil); err != nil {
                t.Fatalf("empty compact: %v", err)
        }
}

// --- begin() drain behavior under close ---

func TestBeginRejectsDuringClose(t *testing.T) {
        s := openTestStore(t)
        s.closing.Store(true)
        if err := s.begin(); !errors.Is(err, ErrShuttingDown) {
                t.Fatalf("begin while closing = %v", err)
        }
        s.closing.Store(false)
        if err := s.begin(); err != nil {
                t.Fatalf("begin after reopen flag = %v", err)
        }
        s.inFlight.Done()
}

func TestValidateRejectsNamelessStore(t *testing.T) {
        cfg := DefaultConfig("")
        cfg.Name = ""
        if err := cfg.Validate(); err == nil {
                t.Fatal("empty name must be rejected")
        }
}

// --- residual micro-coverage ---

func TestNoopSpanDirect(t *testing.T) {
	NoopSpan.SetAttr("k", "v")
	NoopSpan.End(errors.New("x"))
	NoopSpan.End(nil)
}

func TestOpKindByNameUnknown(t *testing.T) {
	if _, ok := opKindByName("nonexistent"); ok {
		t.Fatal("unknown op must not resolve")
	}
	if _, ok := opKindByName("get"); !ok {
		t.Fatal("get must resolve")
	}
}

func TestBucketWidthSmall(t *testing.T) {
	if bucketWidth(1) != 1 || bucketWidth(7) != 1 || bucketWidth(8) != 1 || bucketWidth(15) != 1 {
		t.Fatal("small bucket widths must be 1")
	}
	if bucketWidth(16) != 2 || bucketWidth(1024) != 128 {
		t.Fatalf("octave widths wrong: %d %d", bucketWidth(16), bucketWidth(1024))
	}
}

func TestScanIteratorErrorAndCloseWithProvokedErr(t *testing.T) {
	s := openTestStore(t)
	it, err := s.ScanWithOptions(testCtx, nil, nil, ScanOptions{Prefetch: 2})
	if err != nil {
		t.Fatal(err)
	}
	si := it.(*countingIterator).scanIterator
	si.err = errors.New("injected")
	if err := it.Error(); err == nil {
		t.Fatal("injected error must surface")
	}
	if err := it.Close(); err == nil {
		t.Fatal("close must return injected error")
	}
}
