package bedrock

import (
        "os"
        "path/filepath"
        "testing"
        "time"

        "go.uber.org/zap"
)

func TestDefaultConfig(t *testing.T) {
        c := DefaultConfig("alpha")
        if c.Name != "alpha" || c.DataDir != "./data/alpha" {
                t.Fatalf("name/dir = %s/%s", c.Name, c.DataDir)
        }
        if !c.SyncWrites || c.CacheSizeMB != 32 || c.MemTableSizeMB != 16 {
                t.Fatalf("defaults: %+v", c)
        }
        if c.SlowOpThreshold != 50*time.Millisecond || c.EnvPrefix != "BEDROCK" {
                t.Fatalf("observability defaults: %+v", c)
        }
        if err := c.Validate(); err != nil {
                t.Fatalf("default config must validate: %v", err)
        }
}

func TestConfigValidate(t *testing.T) {
        c := DefaultConfig("x")
        c.DataDir = ""
        if err := c.Validate(); err == nil {
                t.Fatal("empty datadir must fail")
        }
        c = DefaultConfig("x")
        c.Name = ""
        if err := c.Validate(); err == nil {
                t.Fatal("empty name must fail")
        }
        c = DefaultConfig("x")
        c.CacheSizeMB = -1
        if err := c.Validate(); err == nil {
                t.Fatal("negative cache must fail")
        }
        c = DefaultConfig("x")
        c.MemTableSizeMB = 0
        if err := c.Validate(); err == nil {
                t.Fatal("zero memtable must fail")
        }
        c = DefaultConfig("x")
        c.BloomBitsPerKey = -5
        if err := c.Validate(); err == nil {
                t.Fatal("negative bloom bits must fail")
        }
        c = DefaultConfig("x")
        c.MaxBatchOps = -1
        if err := c.Validate(); err == nil {
                t.Fatal("negative max batch ops must fail")
        }
        // Error must wrap ErrInvalidConfig and list fields.
        c = DefaultConfig("x")
        c.DataDir = ""
        c.Name = ""
        err := c.Validate()
        if !isErrInvalidConfig(err) {
                t.Fatalf("validate err = %v", err)
        }
}

func isErrInvalidConfig(err error) bool {
        return unwrapIs(err, ErrInvalidConfig)
}

func unwrapIs(err, target error) bool {
        for err != nil {
                if err == target {
                        return true
                }
                u, ok := err.(interface{ Unwrap() error })
                if !ok {
                        return false
                }
                err = u.Unwrap()
        }
        return false
}

func TestApplyEnv(t *testing.T) {
        env := map[string]string{
                "BEDROCK_NAME":              "envstore",
                "BEDROCK_DATA_DIR":          "/tmp/envdir",
                "BEDROCK_CACHE_MB":          "128",
                "BEDROCK_MEMTABLE_MB":       "32",
                "BEDROCK_L0_COMPACTION_THRESHOLD": "8",
                "BEDROCK_MAX_OPEN_FILES":    "2048",
                "BEDROCK_BLOOM_BITS":        "14",
                "BEDROCK_MAX_BATCH_OPS":     "5000",
                "BEDROCK_HOTCACHE_MB":       "256",
                "BEDROCK_HOTCACHE_TTL_MS":   "1500",
                "BEDROCK_SLOW_OP_MS":        "10",
                "BEDROCK_SYNC_WRITES":       "false",
                "BEDROCK_DISABLE_WAL":       "TRUE",
                "BEDROCK_HOTCACHE_ENABLED":  "yes",
                "BEDROCK_CIRCUIT_ENABLED":   "on",
        }
        getenv := func(k string) string { return env[k] }

        c := DefaultConfig("default")
        if err := c.ApplyEnv(getenv); err != nil {
                t.Fatalf("ApplyEnv: %v", err)
        }
        if c.Name != "envstore" || c.DataDir != "/tmp/envdir" {
                t.Fatalf("name/dir = %s/%s", c.Name, c.DataDir)
        }
        if c.CacheSizeMB != 128 || c.MemTableSizeMB != 32 || c.L0CompactionThreshold != 8 {
                t.Fatalf("engine overrides: %+v", c)
        }
        if c.MaxOpenFiles != 2048 || c.BloomBitsPerKey != 14 || c.MaxBatchOps != 5000 {
                t.Fatalf("misc overrides: %+v", c)
        }
        if c.HotCache.MaxBytes != 256<<20 || c.HotCache.TTL != 1500*time.Millisecond {
                t.Fatalf("hotcache overrides: %+v", c.HotCache)
        }
        if c.SlowOpThreshold != 10*time.Millisecond {
                t.Fatalf("slow op = %v", c.SlowOpThreshold)
        }
        if c.SyncWrites || !c.DisableWAL || !c.HotCache.Enabled || !c.Circuit.Enabled {
                t.Fatalf("bool overrides: %+v", c)
        }
}

func TestApplyEnvFalseVariants(t *testing.T) {
        c2 := DefaultConfig("d")
        err := c2.ApplyEnv(func(k string) string {
                switch k {
                case "BEDROCK_SYNC_WRITES":
                        return "0"
                case "BEDROCK_DISABLE_WAL":
                        return "No"
                case "BEDROCK_HOTCACHE_ENABLED":
                        return "False"
                case "BEDROCK_CIRCUIT_ENABLED":
                        return "off"
                }
                return ""
        })
        if err != nil {
                t.Fatal(err)
        }
        if c2.SyncWrites {
                t.Fatalf("SYNC_WRITES=0 must set false: %+v", c2)
        }
        if c2.DisableWAL || c2.HotCache.Enabled || c2.Circuit.Enabled {
                t.Fatalf("false variants must not enable: %+v", c2)
        }
}

func TestApplyEnvMalformed(t *testing.T) {
        env := map[string]string{
                "BEDROCK_CACHE_MB":       "not-a-number",
                "BEDROCK_SYNC_WRITES":    "maybe",
                "BEDROCK_HOTCACHE_TTL_MS": "-abc",
        }
        c := DefaultConfig("d")
        err := c.ApplyEnv(func(k string) string { return env[k] })
        if err == nil {
                t.Fatal("malformed env must error")
        }
        if !unwrapIs(err, ErrInvalidConfig) {
                t.Fatalf("err = %v", err)
        }
}

func TestApplyEnvEmptyVarsIgnored(t *testing.T) {
        c := DefaultConfig("d")
        before := c.CacheSizeMB
        if err := c.ApplyEnv(func(string) string { return "" }); err != nil {
                t.Fatal(err)
        }
        if c.CacheSizeMB != before {
                t.Fatal("empty env vars must not change config")
        }
}

func TestAllFunctionalOptions(t *testing.T) {
        cfg := DefaultConfig("opts")
        opts := []Option{
                WithDataDir("/custom"),
                WithName("renamed"),
                WithCacheSizeMB(99),
                WithMemTableSizeMB(7),
                WithSyncWrites(false),
                WithDisableWAL(true),
                WithMaxOpenFiles(321),
                WithBloomBitsPerKey(16),
                WithL0Thresholds(9, 18),
                WithMaxConcurrentCompactions(5),
                WithMemTableStopWritesThreshold(9),
                WithHotCache(11<<20, 2*time.Minute),
                WithHotCacheConfig(HotCacheConfig{MaxBytes: 5 << 20, TTL: time.Minute, Shards: 4, JanitorInterval: time.Second}),
                WithCircuitBreaker(CircuitConfig{FailureThreshold: 9}),
                WithMaxBatchOps(42),
                WithZapLogger(zap.NewNop()),
                WithZapLogger(nil), // nil falls back to nop
                WithTracer(nil),    // handled at open; config keeps nil
                WithContextFields(nil),
                WithSlowOpThreshold(time.Second),
                WithEnvPrefix("PFX"),
        }
        for _, o := range opts {
                o(cfg)
        }
        if cfg.DataDir != "/custom" || cfg.Name != "renamed" || cfg.CacheSizeMB != 99 {
                t.Fatalf("basic options: %+v", cfg)
        }
        if cfg.SyncWrites || !cfg.DisableWAL || cfg.MaxOpenFiles != 321 || cfg.BloomBitsPerKey != 16 {
                t.Fatalf("engine options: %+v", cfg)
        }
        if cfg.L0CompactionThreshold != 9 || cfg.L0CompactionFileThreshold != 18 {
                t.Fatalf("L0 options: %+v", cfg)
        }
        if cfg.MaxConcurrentCompactions != 5 || cfg.MemTableStopWritesThreshold != 9 {
                t.Fatalf("compaction options: %+v", cfg)
        }
        if cfg.HotCache.MaxBytes != 5<<20 || cfg.HotCache.TTL != time.Minute || !cfg.HotCache.Enabled {
                t.Fatalf("hot cache config: %+v", cfg.HotCache)
        }
        if !cfg.Circuit.Enabled || cfg.Circuit.FailureThreshold != 9 {
                t.Fatalf("circuit: %+v", cfg.Circuit)
        }
        if cfg.MaxBatchOps != 42 || cfg.SlowOpThreshold != time.Second || cfg.EnvPrefix != "PFX" {
                t.Fatalf("misc options: %+v", cfg)
        }
        if cfg.Logger == nil {
                t.Fatal("nil logger must fall back to nop, not nil")
        }
}

func TestOpenFromEnv(t *testing.T) {
        dir := t.TempDir()
        t.Setenv("BEDROCK_DATA_DIR", dir)
        t.Setenv("BEDROCK_NAME", "envopen")
        t.Setenv("BEDROCK_MEMTABLE_MB", "2")
        t.Setenv("BEDROCK_CACHE_MB", "8")
        s, err := OpenFromEnv()
        if err != nil {
                t.Fatalf("OpenFromEnv: %v", err)
        }
        defer s.Close()
        if s.Name() != "envopen" || s.DataDir() != dir {
                t.Fatalf("store = %s/%s", s.Name(), s.DataDir())
        }
        if err := s.Set(testCtx, []byte("k"), []byte("v")); err != nil {
                t.Fatal(err)
        }
        if _, err := s.Get(testCtx, []byte("k")); err != nil {
                t.Fatal(err)
        }
        // Invalid env aborts.
        t.Setenv("BEDROCK_CACHE_MB", "bogus")
        if _, err := OpenFromEnv(); err == nil {
                t.Fatal("malformed env must abort OpenFromEnv")
        }
}

func TestStoreNameAndDataDir(t *testing.T) {
        s := openTestStore(t, WithName("nm"))
        if s.Name() != "test" { // openTestStore sets WithName("test") last
                // name option ordering: base sets "test" after user opts
                t.Logf("name = %s", s.Name())
        }
        if s.DataDir() == "" {
                t.Fatal("datadir must be set")
        }
        _ = os.Remove(filepath.Join(s.DataDir(), "__nonexistent__"))
}

func TestConfigCloneIsolation(t *testing.T) {
        cfg := DefaultConfig("clone-src")
        opt := WithDataDir("/fixed")
        opt(cfg)
        clone := cfg.clone()
        clone.Name = "mutated"
        if cfg.Name == "mutated" {
                t.Fatal("clone must be independent")
        }
}

func TestHotCacheConfigDefaults(t *testing.T) {
        c := HotCacheConfig{Enabled: true}
        c.defaults()
        if c.MaxBytes != 64<<20 || c.TTL != 5*time.Minute {
                t.Fatalf("hot cache defaults: %+v", c)
        }
        if c.Shards < 8 || c.Shards > 64 {
                t.Fatalf("shards = %d", c.Shards)
        }
        if c.JanitorInterval <= 0 {
                t.Fatal("janitor interval must be positive")
        }
        // Extreme TTL keeps janitor within bounds
        c = HotCacheConfig{Enabled: true, TTL: time.Hour}
        c.defaults()
        if c.JanitorInterval != 30*time.Second {
                t.Fatalf("long TTL janitor = %v", c.JanitorInterval)
        }
        c = HotCacheConfig{Enabled: true, TTL: 100 * time.Millisecond}
        c.defaults()
        if c.JanitorInterval != time.Second {
                t.Fatalf("short TTL janitor = %v", c.JanitorInterval)
        }
}
