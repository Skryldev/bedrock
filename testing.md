# Testing Strategy

The test architecture has four layers, each answering a different question:

| Layer | Question | Location | Gate |
|---|---|---|---|
| Unit (white-box) | Is each component correct in isolation? | `*_test.go` (package `bedrock`) | `make test` |
| Integration (black-box) | Does the public API hold on real engines? | `tests/` | `make test-race` |
| Fuzzing | Does arbitrary input preserve the data model? | `tests/fuzz_test.go` | `make fuzz` |
| Benchmarks | Are we fast, and where is the time going? | `benchmarks/` | `make bench` |

## 1. Unit tests (white-box, same package)

White-box placement gives access to internals (`s.cache`, `s.breaker`, `s.metrics`) for state assertions that would be invisible through the public API.

- **CRUD & semantics** (`store_test.go`): set/get/delete round-trips, overwrite, `ErrNotFound` mapping, empty-key rejection across every method, idempotent delete, merge-operator behavior.
- **Atomic batches:** success path, cache coherence after commit, `ErrEmptyBatch`, unknown op type, `MaxBatchOps` enforcement, range tombstones verified by scanning the survivors.
- **Scan & iterator:** half-open bounds `[start, end)`, key ordering, `SeekGE/SeekLT/First/Last` positioning, empty-range behavior, prefetch ring correctness at sizes 7 / 64 / 5000 (ring power-of-two rounding), double-Close idempotence, invalid-iterator nil key/value in ring mode.
- **Hot cache:** coherence (set→get→overwrite→get, delete invalidation, batch invalidation), TTL expiry via injected clock, janitor sweeps, per-shard and global eviction budgets, range invalidation (`deleteRange`), `clear`, disabled-mode zero metrics.
- **Metrics:** histogram bucket layout round-trip (every bucket lower-bound maps back), quantile math (p50/p95/p99/p99.9/100 with clamp behavior), concurrent recording (4 goroutines × 10k), ops/sec window rate, snapshot field completeness, Prometheus output string assertions (families, labels, op/result pairs).
- **Circuit breaker:** closed→open at threshold, `ErrNotFound` never counts, open rejects with `ErrCircuitOpen`, cooldown→half-open transition, probe failure re-opens, probe successes close, failure-window expiry resets counters, state-change hook fires.
- **Errors:** taxonomy classification table (`Classify`), `StoreError` formatting, key truncation, unwrap chains.
- **Config:** every functional option, defaults, `Validate` rejections, `ApplyEnv` (numeric, boolean in 4 lexical variants, malformed-variable error listing, MiB→bytes conversion), `OpenFromEnv` with `t.Setenv`.
- **Lifecycle:** graceful shutdown (idempotent `Close`, post-close rejections, reopen via WAL replay), drain of in-flight operations under concurrent writers, slow-op logging thresholds (0 clamps to 50 ms, negative disables), context field propagation into zap.

## 2. Integration tests (black-box, `tests/` package)

Through the public API only, on filesystem-backed engines:

- **Basic lifecycle:** full CRUD + batch + scan + metrics + Prometheus + health in one pass.
- **Durability across clean shutdown:** 500 keys survive `Close` → `Open` (WAL replay of synced writes).
- **Crash simulation:** DB directory copied without `Close` (WAB files intact), reopened — asserts WAL replay recovers every synced write.
- **Concurrent mixed workload:** 4 writers + 4 readers + 2 batchers + 1 scanner × hundreds of rounds with a 60s deadlock watchdog — the primary `-race` soak test.
- **Repository multi-tenancy:** three isolated engines, isolation verified by cross-reads, refcount semantics (open ×2, close ×1 → still usable), graceful `CloseAll`.
- **Checkpoint round-trip:** 100 rows → checkpoint → 50 more rows → restore → exactly the first 100 rows present.

## 3. Fuzzing

`FuzzStoreOperations` is **model-based**: the harness decodes raw fuzz bytes into a deterministic operation sequence (set/get/delete/batch/scan/delete-range over random keys), maintains an exact `map[string]string` model, and asserts after every step — plus a final full-keyspace sweep — that:

- `Get` returns exactly the model value or `ErrNotFound`;
- `Scan` yields only model-consistent key/value pairs in order;
- range deletions remove precisely `[start, end)`;
- final store state is set-equal to the model.

Seeds cover structured sequences; the fuzz cache retains interesting inputs and any crash reproduction under `tests/testdata/fuzz/`.

`FuzzConfigMatrix` fuzzes option combinations asserting they open cleanly or fail with classified errors — never panic.

Run: `make fuzz` (30s default; CI: 60–120s; nightly: minutes).

## 4. Race detection & coverage

- `make test-race` runs every suite under `-race`; the concurrent unit tests (`TestStoreConcurrentOperations`, `TestStoreConcurrentMixedWorkload`) and the integration soak exercise shared state across cache shards, metrics ring slots and the shutdown path.
- Coverage gate: `make cover-check` fails below **90%** statement coverage (currently 90.2%). The remaining uncovered lines are engine-failure branches that require fault injection at the VFS layer; they are exercised indirectly by Pebble's own test suite.
- Determinism: no wall-clock sleeps outside explicitly-clock-injected tests; tests use `t.TempDir()` for isolation and run safely under `-count=N` and parallel package execution.

## 5. Benchmarks & profiling

- `benchmarks/bench_test.go` covers Get (sequential/random/parallel/hot), Set (sync/unsync/parallel), Delete, Batch (10/100/1000), Scan (10/1000/prefetch), mixed workloads, and baselines (RWMutex map, raw Pebble) for overhead attribution.
- `benchmarks/profile_test.go` prints module-measured latency percentiles per phase (used verbatim in `docs/performance.md`).
- Profiles: `go test -bench BenchmarkGetRandom -cpuprofile cpu.out -memprofile mem.out ./benchmarks/` then `go tool pprof`. GC behavior is observable via `GODEBUG=gctrace=1` — allocation discipline (≤3 allocs/op hot paths) keeps pauses well under the 1 ms target.

## 6. CI pipeline sketch

```yaml
steps:
  - run: make lint && make tidy
  - run: make test
  - run: make test-race
  - run: make cover-check          # ≥90% gate
  - run: make fuzz                 # 60s
  - run: make bench-short          # regression smoke (compare vs. baseline JSON)
  - run: make docker-build
```

Benchmark regression tracking: store `make bench` output as JSON (`-json`), compare op/s with `benchstat` across three runs; alert on >15% regressions in Get/Set/Mixed.
