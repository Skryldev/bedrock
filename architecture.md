# Architecture

## Layered view

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Application / Service                        │
└──────────────┬──────────────────────────────────────────────────────┘
               │ Bedrock / ExtendedStore (interfaces)
┌──────────────▼──────────────────────────────────────────────────────┐
│  Repository (factory & registry)                                    │
│  · named instances, refcounting, path-safety, graceful CloseAll     │
└──────────────┬──────────────────────────────────────────────────────┘
               │
┌──────────────▼──────────────────────────────────────────────────────┐
│  Store (core facade)                                                │
│  ┌────────────┐  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  │
│  │ HotCache   │  │ Metrics     │  │ Circuit      │  │ Tracer /   │  │
│  │ LRU+TTL    │  │ Registry    │  │ Breaker      │  │ zap Logger │  │
│  │ sharded    │  │ histograms  │  │ closed/open/ │  │ ctx fields │  │
│  │ 64 stripes │  │ ops windows │  │ half-open    │  │            │  │
│  └────────────┘  └─────────────┘  └──────────────┘  └────────────┘  │
│  ┌──────────────────────────┐  ┌─────────────────────────────────┐  │
│  │ Batch pool (sync.Pool)   │  │ Health / Checkpoint / Recovery  │  │
│  └──────────────────────────┘  └─────────────────────────────────┘  │
└──────────────┬──────────────────────────────────────────────────────┘
               │ tuned Options (cache, memtable, bloom, L0, sync)
┌──────────────▼──────────────────────────────────────────────────────┐
│  Pebble LSM engine (WAL → memtable → L0..L6 SSTables, block cache)  │
└─────────────────────────────────────────────────────────────────────┘
```

## Component design

### Store — the concurrency core

`Store` serializes lifecycle, not operations. A single `atomic.Bool` (`closing`) plus a `sync.WaitGroup` (`inFlight`) implement graceful shutdown with no mutex on the hot path:

1. `begin()` — re-check `closing` (cheap load), register on `inFlight`, re-check again (closes the race with `Close`).
2. Operation executes lock-free; per-op latency recorded into a lock-free histogram.
3. `inFlight.Done()`.

`Close()` flips `closing`, waits on `inFlight`, stops the cache janitor, closes the engine, and unrefs the block cache — exactly once (`sync.Once`). All methods are safe for concurrent use.

### Hot cache — sharded LRU+TTL

- **Sharding:** FNV-1a hash over the key → one of `GOMAXPROCS*2` (clamped [8,64]) shards, each a mutex + `map[string]*list.Element` + intrusive LRU list. Get-path lookups use Go's alloc-free `m[string(bytes)]` idiom; the only copy happens on the write path.
- **Memory accounting:** each entry charges `len(k)+len(v)+96`; inserts evict from the LRU tail until the shard sits within its share of `MaxBytes`; a janitor goroutine (TTL/2 cadence) sweeps expired entries and enforces the global budget.
- **Coherence:** Set/Delete/DeleteRange/Batch update or invalidate entries *only after a successful engine commit*, giving read-your-writes semantics within the process. The cache is a **read-through, write-coherent** layer — not a distributed cache; TTL bounds staleness for out-of-band writers.
- **TTL:** per-entry expiry with `SetWithTTL` overrides.

### Metrics registry — lock-free by construction

- **Histogram:** HDR-style log-scale layout — 8 sub-buckets per octave, 512 atomic counters spanning 1 ns → 584 years. `Record` is one shift + mask + atomic add; percentiles are computed by a single O(512) walk inside `Snapshot` (±6% bucket precision).
- **Ops/sec:** 64-slot per-second counter ring with CAS-based slot rotation; the 10-second rate is computed on snapshot.
- **Prometheus:** `PrometheusMetrics()` renders counters, per-op cumulative-bucket histograms (`le` buckets 1 µs → 10 s), cache gauges, breaker state and mapped engine gauges — text format 0.0.4, no client dependency.

### Circuit breaker

Classic three-state machine (closed → open at N failures in W window → half-open after cooldown → closed after M probe successes). Failure accounting reuses the error taxonomy: `ErrNotFound` and other expected outcomes never count; transient and permanent engine failures do. State transitions emit warn logs and bump `circuit_opens_total`.

### Error taxonomy

Sentinels (`ErrNotFound`, `ErrClosed`, `ErrCircuitOpen`, `ErrEmptyKey`, `ErrInvalidOperation`, …) wrap into `*StoreError{Op, Key, Err}` with `Unwrap` support. `Classify(err)` sorts any error into `ok / transient / permanent`, consumed by the breaker, retry policies and alerting. Pebble's `ErrNotFound` is remapped to the module sentinel so callers never import the engine for error checks.

### Iterator

`Scan(ctx, start, end)` positions the engine iterator at `start` before returning (Pebble iterators are born unpositioned), with `LowerBound`/`UpperBound` pushed down so table-level bounds pruning applies. Two modes:

- **Pass-through (default):** zero copies — `Key()/Value()` point into engine memory, valid until the next positioning call.
- **Copy-ahead (`ScanOptions.Prefetch`):** a ring buffer of up to 4096 entries is filled synchronously as the consumer advances, amortizing iterator-return latency; `Key()/Value()` remain valid for the whole ring window. Choose this for bulk ETL scans; benchmarks show it trades per-key speed (~1.9× slower in RAM-resident scans) for stable, allocator-friendly streaming.

### Batching

`Batch(ctx, ops)` pulls a `*pebble.Batch` from a `sync.Pool`, applies validated operations, commits atomically with the configured `WriteOptions`, then returns the batch to the pool (`Reset` keeps buffers warm). Cache invalidations are staged and applied only post-commit. `NewWriteBatch` exposes the pooled buffer for streaming workloads (`Commit` → `Reset` → reuse → `Close`).

### Backup & recovery

- `CreateCheckpoint(dest)` uses Pebble's hardlink-based `Checkpoint` — O(sstables), crash-consistent, includes the WAL. Non-empty destinations are refused to prevent clobbering.
- `RestoreCheckpoint(src, dst)` materializes the hardlinks into a fresh directory; `OpenOrRestore` automates the crash playbook: try open → on unrecoverable failure and backup present → wipe data dir → restore → retry.

### Repository

Registry of named stores with reference counting: repeated `Open(name)` returns the same engine and bumps the count; `Close(name)` decrements, closing at zero. Names become directory names — path separators and `..` are rejected. `CloseAll` rejects new opens, drains and closes every instance, aggregating errors.

## Memory strategy

| Allocation | Strategy |
|---|---|
| Value copy on `Get` miss | Single `make+copy`, released to caller contract |
| Hot-cache hit | Zero copy (shared immutable value) |
| Batches | Pooled `*pebble.Batch`, `Reset` reuse |
| Metric recording | Zero alloc (atomic adds into pre-allocated arrays) |
| Iterator (pass-through) | Zero alloc |
| Iterator (prefetch) | Amortized ring-buffer copies |
| Prometheus render | Allocation allowed (admin path, not hot) |

## Consistency notes

- **Atomic batches:** all-or-nothing via Pebble's WAL; visible state never shows partial batches.
- **Read-your-writes:** guaranteed within one process through cache coherence on the write path.
- **Cross-process:** Pebble single-writer per directory; readers in other processes see committed state. Out-of-process mutations require a hot-cache TTL no larger than the acceptable staleness window (or disable the cache).
- **Durability:** `SyncWrites=true` fsyncs the WAL per commit; `false` batches syncs at OS cadence — throughput vs. RPO is a per-deployment knob.
