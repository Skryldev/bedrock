package bedrock

import (
        "fmt"
        "os"
        "strconv"
        "strings"
        "time"

        "go.uber.org/zap"
)

// Config holds every tunable of the store. Build it through functional
// options (Open(WithDataDir(...), ...)) or via environment variables
// (Config.ApplyEnv). The zero value is not valid; always start from
// DefaultConfig or Open's built-in defaults.
type Config struct {
        // Name is the logical instance name (metrics labels, logs, repository
        // keys). Default "default".
        Name string
        // DataDir is the Pebble directory. Default "./data/bedrock".
        DataDir string

        // --- Engine performance tuning ---

        // CacheSizeMB is the Pebble block cache size in MiB. Default 32.
        CacheSizeMB int
        // MemTableSizeMB is the memtable size in MiB. Larger memtables batch
        // WAL syncs and produce larger, fewer SSTables. Default 16.
        MemTableSizeMB int
        // MemTableStopWritesThreshold caps queued memtables before writes
        // stall. Default 4.
        MemTableStopWritesThreshold int
        // L0CompactionThreshold triggers L0->L1 compaction at N L0 sublevels.
        // Default 4.
        L0CompactionThreshold int
        // L0CompactionFileThreshold triggers compaction at N L0 files.
        // Default 8.
        L0CompactionFileThreshold int
        // LBaseMaxBytesMB caps level-1 size before compaction pushes to lower
        // levels. Default 64.
        LBaseMaxBytesMB int64
        // MaxConcurrentCompactions bounds parallel compactions. Default 2.
        MaxConcurrentCompactions int
        // MaxOpenFiles bounds open file descriptors. Default 1024.
        MaxOpenFiles int
        // SyncWrites fsyncs the WAL on every commit (durable, slower). When
        // false, durability degrades to OS flush cadence. Default true.
        SyncWrites bool
        // BytesPerSync syncs data files periodically for smoother IO. Default
        // 512 KiB.
        BytesPerSync int
        // WALBytesPerSync syncs the WAL periodically. Default 256 KiB.
        WALBytesPerSync int
        // BloomBitsPerKey sets the Bloom filter density (10 ≈ 1% FP rate).
        // Default 10.
        BloomBitsPerKey int
        // DisableWAL drops write-ahead logging entirely (data loss on crash;
        // benchmarking only). Default false.
        DisableWAL bool

        // --- Application-level tuning ---

        // HotCache enables the LRU+TTL read cache.
        HotCache HotCacheConfig
        // Circuit enables the circuit breaker. Zero-value fields receive
        // defaults (50 failures / 30s window / 5s cooldown / 3 probes).
        Circuit CircuitConfig
        // MaxBatchOps rejects Batch calls larger than this when > 0.
        MaxBatchOps int
        // BatchPoolCap bounds the pooled batch count hint (informational).
        BatchPoolCap int

        // --- Observability ---

        // Logger receives structured lifecycle and slow-op events. Default: nop.
        Logger *zap.Logger
        // Tracer receives operation spans. Default: NoopTracer.
        Tracer Tracer
        // CtxFields extracts log fields from contexts (request/trace IDs).
        CtxFields CtxFieldExtractor
        // SlowOpThreshold logs a warn for operations slower than this.
        // Default 50ms. Set negative to disable.
        SlowOpThreshold time.Duration
        // EnvPrefix is the prefix consumed by ApplyEnv. Default "BEDROCK".
        EnvPrefix string
}

// DefaultConfig returns the production-default configuration for name.
func DefaultConfig(name string) *Config {
        return &Config{
                Name:                        name,
                DataDir:                     "./data/" + name,
                CacheSizeMB:                 32,
                MemTableSizeMB:              16,
                MemTableStopWritesThreshold: 4,
                L0CompactionThreshold:       4,
                L0CompactionFileThreshold:   8,
                LBaseMaxBytesMB:             64,
                MaxConcurrentCompactions:    2,
                MaxOpenFiles:                1024,
                SyncWrites:                  true,
                BytesPerSync:                512 << 10,
                WALBytesPerSync:             256 << 10,
                BloomBitsPerKey:             10,
                MaxBatchOps:                 0,
                Logger:                      defaultLogger(),
                Tracer:                      NoopTracer,
                SlowOpThreshold:             50 * time.Millisecond,
                EnvPrefix:                   "BEDROCK",
        }
}

// clone deep-copies user-owned reference fields so that later mutation of
// the returned *Config does not race with the store.
func (c *Config) clone() *Config {
        cp := *c
        return &cp
}

// Validate checks configuration sanity.
func (c *Config) Validate() error {
        var errs []string
        if c.DataDir == "" {
                errs = append(errs, "data dir must not be empty")
        }
        if c.Name == "" {
                errs = append(errs, "name must not be empty")
        }
        if c.CacheSizeMB < 0 {
                errs = append(errs, "cache size must be >= 0")
        }
        if c.MemTableSizeMB < 1 {
                errs = append(errs, "memtable size must be >= 1 MiB")
        }
        if c.BloomBitsPerKey < 0 {
                errs = append(errs, "bloom bits per key must be >= 0")
        }
        if c.MaxBatchOps < 0 {
                errs = append(errs, "max batch ops must be >= 0")
        }
        if len(errs) > 0 {
                return fmt.Errorf("%w: %s", ErrInvalidConfig, strings.Join(errs, "; "))
        }
        return nil
}

// ApplyEnv overrides configuration fields from environment variables
// "<EnvPrefix>_*" (e.g. BEDROCK_DATA_DIR). Unset variables are
// ignored; malformed numeric values produce an error listing the offending
// variables. ApplyEnv composes with functional options: call Open after
// applying env overrides, or use OpenFromEnv.
func (c *Config) ApplyEnv(getenv func(string) string) error {
        p := c.EnvPrefix + "_"
        if v := getenv(p + "NAME"); v != "" {
                c.Name = v
        }
        if v := getenv(p + "DATA_DIR"); v != "" {
                c.DataDir = v
        }
        var errs []string
        parseInt := func(name string, dst *int) {
                if v := getenv(p + name); v != "" {
                        n, err := strconv.Atoi(v)
                        if err != nil {
                                errs = append(errs, p+name)
                                return
                        }
                        *dst = n
                }
        }
        parseInt("CACHE_MB", &c.CacheSizeMB)
        parseInt("MEMTABLE_MB", &c.MemTableSizeMB)
        parseInt("L0_COMPACTION_THRESHOLD", &c.L0CompactionThreshold)
        parseInt("MAX_OPEN_FILES", &c.MaxOpenFiles)
        parseInt("BLOOM_BITS", &c.BloomBitsPerKey)
        parseInt("MAX_BATCH_OPS", &c.MaxBatchOps)
        if v := getenv(p + "HOTCACHE_MB"); v != "" {
                if n, err := strconv.Atoi(v); err == nil {
                        c.HotCache.MaxBytes = int64(n) << 20 // MiB -> bytes
                } else {
                        errs = append(errs, p+"HOTCACHE_MB")
                }
        }

        if v := getenv(p + "HOTCACHE_TTL_MS"); v != "" {
                if n, err := strconv.ParseInt(v, 10, 64); err == nil {
                        c.HotCache.TTL = time.Duration(n) * time.Millisecond
                } else {
                        errs = append(errs, p+"HOTCACHE_TTL_MS")
                }
        }
        if v := getenv(p + "SLOW_OP_MS"); v != "" {
                if n, err := strconv.ParseInt(v, 10, 64); err == nil {
                        c.SlowOpThreshold = time.Duration(n) * time.Millisecond
                } else {
                        errs = append(errs, p+"SLOW_OP_MS")
                }
        }
        switch v := getenv(p + "SYNC_WRITES"); strings.ToLower(v) {
        case "":
        case "true", "1", "yes", "on":
                c.SyncWrites = true
        case "false", "0", "no", "off":
                c.SyncWrites = false
        default:
                errs = append(errs, p+"SYNC_WRITES")
        }
        switch v := getenv(p + "DISABLE_WAL"); strings.ToLower(v) {
        case "":
        case "true", "1", "yes", "on":
                c.DisableWAL = true
        case "false", "0", "no", "off":
                c.DisableWAL = false
        default:
                errs = append(errs, p+"DISABLE_WAL")
        }
        switch v := getenv(p + "HOTCACHE_ENABLED"); strings.ToLower(v) {
        case "":
        case "true", "1", "yes", "on":
                c.HotCache.Enabled = true
        case "false", "0", "no", "off":
                c.HotCache.Enabled = false
        default:
                errs = append(errs, p+"HOTCACHE_ENABLED")
        }
        switch v := getenv(p + "CIRCUIT_ENABLED"); strings.ToLower(v) {
        case "":
        case "true", "1", "yes", "on":
                c.Circuit.Enabled = true
        case "false", "0", "no", "off":
                c.Circuit.Enabled = false
        default:
                errs = append(errs, p+"CIRCUIT_ENABLED")
        }
        if len(errs) > 0 {
                return fmt.Errorf("%w: malformed env vars: %s", ErrInvalidConfig, strings.Join(errs, ", "))
        }
        return nil
}

// Option mutates the store configuration.
type Option func(*Config)

// WithDataDir sets the engine directory.
func WithDataDir(dir string) Option { return func(c *Config) { c.DataDir = dir } }

// WithName sets the logical instance name.
func WithName(name string) Option { return func(c *Config) { c.Name = name } }

// WithCacheSizeMB sets the Pebble block cache size in MiB.
func WithCacheSizeMB(mb int) Option { return func(c *Config) { c.CacheSizeMB = mb } }

// WithMemTableSizeMB sets the memtable size in MiB.
func WithMemTableSizeMB(mb int) Option { return func(c *Config) { c.MemTableSizeMB = mb } }

// WithSyncWrites toggles per-commit WAL fsync.
func WithSyncWrites(v bool) Option { return func(c *Config) { c.SyncWrites = v } }

// WithDisableWAL disables the write-ahead log (dangerous; benchmarking).
func WithDisableWAL(v bool) Option { return func(c *Config) { c.DisableWAL = v } }

// WithMaxOpenFiles bounds open file descriptors.
func WithMaxOpenFiles(n int) Option { return func(c *Config) { c.MaxOpenFiles = n } }

// WithBloomBitsPerKey configures Bloom filter density.
func WithBloomBitsPerKey(bits int) Option { return func(c *Config) { c.BloomBitsPerKey = bits } }

// WithL0Thresholds sets L0 compaction triggers.
func WithL0Thresholds(compactionThreshold, fileThreshold int) Option {
        return func(c *Config) {
                c.L0CompactionThreshold = compactionThreshold
                c.L0CompactionFileThreshold = fileThreshold
        }
}

// WithMaxConcurrentCompactions bounds parallel compactions.
func WithMaxConcurrentCompactions(n int) Option {
        return func(c *Config) { c.MaxConcurrentCompactions = n }
}

// WithMemTableStopWritesThreshold caps queued memtables.
func WithMemTableStopWritesThreshold(n int) Option {
        return func(c *Config) { c.MemTableStopWritesThreshold = n }
}

// WithHotCache enables the LRU+TTL hot cache with the given byte budget
// and default TTL.
func WithHotCache(maxBytes int64, ttl time.Duration) Option {
        return func(c *Config) {
                c.HotCache.Enabled = true
                if maxBytes > 0 {
                        c.HotCache.MaxBytes = maxBytes
                }
                if ttl > 0 {
                        c.HotCache.TTL = ttl
                }
        }
}

// WithHotCacheConfig installs a fully specified hot-cache configuration.
func WithHotCacheConfig(cfg HotCacheConfig) Option {
        return func(c *Config) { c.HotCache = cfg; c.HotCache.Enabled = true }
}

// WithCircuitBreaker enables the circuit breaker with the given config
// (zero fields receive defaults).
func WithCircuitBreaker(cfg CircuitConfig) Option {
        return func(c *Config) {
                cfg.Enabled = true
                c.Circuit = cfg
        }
}

// WithMaxBatchOps caps the number of operations per Batch call.
func WithMaxBatchOps(n int) Option { return func(c *Config) { c.MaxBatchOps = n } }

// WithZapLogger installs the structured logger.
func WithZapLogger(l *zap.Logger) Option {
        return func(c *Config) {
                if l == nil {
                        l = defaultLogger()
                }
                c.Logger = l
        }
}

// WithTracer installs the tracing adapter.
func WithTracer(t Tracer) Option {
        return func(c *Config) {
                if t == nil {
                        t = NoopTracer
                }
                c.Tracer = t
        }
}

// WithContextFields installs a context field extractor for log lines.
func WithContextFields(fn CtxFieldExtractor) Option { return func(c *Config) { c.CtxFields = fn } }

// WithSlowOpThreshold sets the slow-operation warn threshold; negative
// disables.
func WithSlowOpThreshold(d time.Duration) Option {
        return func(c *Config) { c.SlowOpThreshold = d }
}

// WithEnvPrefix sets the environment variable prefix for ApplyEnv.
func WithEnvPrefix(p string) Option { return func(c *Config) { c.EnvPrefix = p } }

// OpenFromEnv builds a store from DefaultConfig plus environment overrides
// plus the supplied options (options win).
func OpenFromEnv(opts ...Option) (*Store, error) {
        cfg := DefaultConfig("default")
        if err := cfg.ApplyEnv(os.Getenv); err != nil {
                return nil, err
        }
        for _, o := range opts {
                o(cfg)
        }
        return openWithConfig(cfg)
}
