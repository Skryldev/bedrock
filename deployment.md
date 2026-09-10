# Deployment

## Filesystem requirements

- **Local SSD/NVMe strongly recommended.** Pebble is an LSM engine: WAL fsync latency and compaction IO dominate tail latency. Network filesystems (NFS/EFS) are unsupported — file-lock and fsync semantics break LSM correctness assumptions.
- Budget disk ≈ 2–3× logical dataset (write amplification + compaction headroom). Monitor `bedrock_engine_disk_size_bytes`.
- `ulimit -n` / pod `nofile` ≥ `WithMaxOpenFiles` (default 1024). Pebble errors on exhaustion.
- One store **owns one directory**. Never open the same directory from two processes (Pebble takes a process-local lock; cross-process access corrupts state).

## Embedding in a service

```go
func main() {
    store, err := bedrock.OpenFromEnv() // 12-factor config
    if err != nil {
        log.Fatalf("store: %v", err) // recovery alternative: OpenOrRestore(cfg, backupDir)
    }
    defer store.Close()

    http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
        h := store.HealthCheck(r.Context())
        if !h.Alive() { w.WriteHeader(http.StatusServiceUnavailable) }
        json.NewEncoder(w).Encode(h)
    })
    http.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
        h := store.HealthCheck(r.Context())
        if !h.Ready() { w.WriteHeader(http.StatusServiceUnavailable) }
        io.WriteString(w, h.Status)
    })
    http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
        io.WriteString(w, store.PrometheusMetrics())
    })
    // ... application handlers using store ...
}
```

Health semantics:

| Status | Meaning | Liveness (`Alive`) | Readiness (`Ready`) |
|---|---|---|---|
| `healthy` | all probes pass | ✅ | ✅ |
| `degraded` | engine pressure (L0 backlog ≥ 20 files, compaction debt ≥ 1 GiB, memtable ≥ 90%) | ✅ (don't restart) | ❌ (shed traffic) |
| `unhealthy` | engine unreachable/read probe fails | ❌ | ❌ |

## Kubernetes

- **Run as a StatefulSet** with one PVC per replica; Pebble is single-writer per directory. `volumeClaimTemplates` with `storageClass: ssd`.
- **Probes:** wire `/readyz` and `/healthz` as above. Liveness failures must only ever come from `unhealthy` — restarting a `degraded` store mid-compaction makes pressure worse.
- **Resources:** cache budgets are the RAM floor: `CacheSizeMB + HotCache.MaxBytes/2^20 + ~512MiB` working set. Set requests ≈ limits (Guaranteed QoS) to protect fsync latency; CPU contention directly inflates p99.
- **Stop signals:** on `SIGTERM`, stop accepting application traffic, then call `store.Close()` — it drains in-flight operations and closes the WAL cleanly. Allow `terminationGracePeriodSeconds ≥ 30s` for long-running batches.
- **Do not** run more than one pod against the same PVC in `ReadWriteMany` mode.

```yaml
lifecycle:
  preStop:
    exec: { command: ["/bin/sh", "-c", "sleep 5"] }  # let LB drain first
```

## Backup & restore

```go
// Nightly or pre-deploy snapshot (near-instant, hardlink-based):
if err := store.CreateCheckpoint("/backups/kv/2026-09-10T00:00"); err != nil { ... }
```

- Checkpoints are **crash-consistent** and include the WAL — restore yields the exact state at call time.
- Copy/mount checkpoints off-node (they are plain directories). A checkpoint shares SSTables via hardlinks until the next compaction rewrites them, so ship the archive before heavy compactions churn the link source.
- Restore: `bedrock.RestoreCheckpoint(src, dst)` then open `dst`. Automated crash recovery: `bedrock.OpenOrRestore(cfg, latestBackup)` wipes an unopenable data dir and restores the checkpoint in one step.
- Verify restores with `Scan` counts before cutting over; keep ≥ 2 generations on different volumes.

## Monitoring & alerting recommendations

Scrape `PrometheusMetrics()`; the metric families:

**SLO-critical (page):**
| Metric | Alert | Rationale |
|---|---|---|
| `bedrock_operation_duration_seconds{quantile over histogram}` | p99 > 10ms for 5m | sustained tail regression |
| `bedrock_operations_total{result="error"}` rate | > 1% of ops for 5m | engine errors; check `Classify` breakdown in app logs |
| `bedrock_circuit_opens_total` increase | any | breaker tripped: upstream is shedding load |
| `bedrock_health_status` (export via gauge in service layer) | ≠ healthy for 2m | read path failure |

**Capacity (ticket):**
| Metric | Threshold | Action |
|---|---|---|
| `bedrock_engine_l0_files` | > 20 sustained | compaction backlog: throttle writes, check disk IO |
| `bedrock_engine_compaction_debt_bytes` | > 1 GiB | same; also check `max_concurrent_compactions` |
| `bedrock_engine_memtable_size_bytes` | ≥ 90% of budget | write burst; consider bigger memtable |
| `bedrock_engine_disk_size_bytes` | > 75% volume | provision before WAL stalls |
| `bedrock_engine_wal_files` | growing unbounded | stuck flush or read-pinned memtables (leaked iterator?) |
| `bedrock_engine_open_iterators` | baseline × 5 | iterator leak hunting — every `Scan` must be `Close`d |
| `bedrock_cache_hit_ratio` | < 50% with hot cache enabled | hot set larger than budget; resize |

**Tracing:** inject any OpenTelemetry-compatible adapter via `WithTracer` (implement `Tracer.StartSpan` by wrapping `otel.Tracer`); spans cover `get/set/delete/merge/batch/scan` with `op` and key attributes. The noop default costs one interface call.

## Capacity planning

- Reads: with the hot cache, plan per-shard ≈ 0.3–1.0 M ops/s per core (sandbox numbers, NVMe will exceed); without, size by disk read IOPS × read amplification (≈ 1 + L0 depth).
- Writes: unsynced ≈ 700K–1M ops/s/core; fsync-bound ≈ 1/fsync-latency; batched writes amortize to batch throughput (≈ 1.8M ops/s here).
- Memory: `block cache + hot cache + memtable(s) × stop-writes threshold + iterator working set`.

## Upgrade notes

Pebble's format major version is forward-compatible and ratcheted on open. Pin the engine version in `go.mod`, test restores from the previous version's checkpoints in staging, and keep the last two checkpoint generations before upgrading.
