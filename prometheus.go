package bedrock

import (
        "fmt"
        "strings"
)

// promLatencyBuckets are the cumulative latency buckets exposed in
// Prometheus text format (seconds). They map onto the internal log-scale
// histogram without loss of usable fidelity.
var promLatencyBuckets = [...]uint64{
        1e3,        //   1µs
        10 * 1e3,   //  10µs
        100 * 1e3,  // 100µs
        1e6,        //   1ms
        5 * 1e6,    //   5ms
        10 * 1e6,   //  10ms
        50 * 1e6,   //  50ms
        100 * 1e6,  // 100ms
        500 * 1e6,  // 500ms
        1e9,        //   1s
        10 * 1e9,   //  10s
}

// cumulativeCount returns the number of observations <= leNS using the
// bucket layout of Histogram.
func (h *Histogram) cumulativeCount(leNS uint64) uint64 {
        var total uint64
        upper := bucketFor(leNS) // buckets whose lower bound <= leNS are fully counted
        for i := 0; i <= upper; i++ {
                total += h.counts[i].Load()
        }
        return total
}

// PrometheusMetrics renders the store's metrics in the Prometheus text
// exposition format (version 0.0.4). The output includes module counters,
// latency histograms per operation, hot-cache statistics, circuit-breaker
// state and a curated set of Pebble engine gauges.
//
// This renderer intentionally avoids the prometheus/client_golang
// dependency: the text format is trivial to emit and scrapes reliably with
// any pull-based collector.
func (s *Store) PrometheusMetrics() string {
        snap := s.MetricsSnapshot()
        var b strings.Builder
        b.Grow(8192)

        writeCounter(&b, "bedrock_cache_hits_total", "Hot cache lookups served without engine access.", snap.CacheHits, "")
        writeCounter(&b, "bedrock_cache_misses_total", "Hot cache lookups that required engine access.", snap.CacheMisses, "")
        writeCounter(&b, "bedrock_cache_evictions_total", "Hot cache entries evicted (capacity or TTL).", snap.CacheEvictions, "")
        writeGauge(&b, "bedrock_cache_entries", "Entries currently held by the hot cache.", float64(snap.CacheEntries))
        writeGauge(&b, "bedrock_cache_bytes", "Approximate bytes held by the hot cache.", float64(snap.CacheBytes))
        writeGauge(&b, "bedrock_cache_hit_ratio", "Hot cache hit ratio since start.", snap.CacheHitRatio)
        writeCounter(&b, "bedrock_circuit_opens_total", "Times the circuit breaker transitioned to open.", snap.CircuitOpens, "")
        writeCounter(&b, "bedrock_batch_ops_total", "Individual operations applied via atomic batches.", snap.BatchOpsTotal, "")
        writeCounter(&b, "bedrock_scan_keys_total", "Keys yielded by Scan iterators.", snap.ScanKeysTotal, "")
        writeCounter(&b, "bedrock_bytes_read_total", "Application bytes read from engine or cache.", snap.BytesRead, "")
        writeCounter(&b, "bedrock_bytes_written_total", "Application bytes written to the engine.", snap.BytesWritten, "")
        writeCounter(&b, "bedrock_slow_ops_total", fmt.Sprintf("Operations slower than the %s threshold.", s.cfg.SlowOpThreshold), snap.SlowOps, "")

        stateVal := 0.0
        switch snap.CircuitState {
        case "closed":
                stateVal = 0
        case "half_open":
                stateVal = 1
        case "open":
                stateVal = 2
        }
        writeGauge(&b, "bedrock_circuit_state", "Circuit state: 0=closed, 1=half_open, 2=open.", stateVal)

        for _, name := range sortedOpNames(snap.Ops) {
                st := snap.Ops[name]
                writeCounter(&b, "bedrock_operations_total",
                        "Operations executed.", st.OK, fmt.Sprintf(",op=%q,result=%q", name, "ok"))
                if st.NotFound > 0 {
                        writeCounter(&b, "bedrock_operations_total", "", st.NotFound,
                                fmt.Sprintf(",op=%q,result=%q", name, "not_found"))
                }
                if st.Errors > 0 {
                        writeCounter(&b, "bedrock_operations_total", "", st.Errors,
                                fmt.Sprintf(",op=%q,result=%q", name, "error"))
                }
                writeGauge(&b, "bedrock_operations_per_second",
                        "Average operations per second over the last ~10s window.", st.OpsPerSec)
                // histogram
                b.WriteString("# TYPE bedrock_operation_duration_seconds histogram\n")
                // Access the histogram through the registry for bucket data.
                if kind, ok := opKindByName(name); ok {
                        h := &s.metrics.ops[kind].latency
                        var cumulative uint64
                        for _, le := range promLatencyBuckets {
                                cumulative = h.cumulativeCount(le)
                                fmt.Fprintf(&b, "bedrock_operation_duration_seconds_bucket{op=%q,le=%q} %d\n",
                                        name, formatLE(le), cumulative)
                        }
                        fmt.Fprintf(&b, "bedrock_operation_duration_seconds_bucket{op=%q,le=\"+Inf\"} %d\n",
                                name, h.Count())
                        fmt.Fprintf(&b, "bedrock_operation_duration_seconds_sum{op=%q} %.9f\n", name, h.Sum().Seconds())
                        fmt.Fprintf(&b, "bedrock_operation_duration_seconds_count{op=%q} %d\n", name, h.Count())
                }
        }

        e := snap.Engine
        writeCounter(&b, "bedrock_engine_block_cache_hits_total", "Pebble block cache hits.", e.BlockCacheHits, "")
        writeCounter(&b, "bedrock_engine_block_cache_misses_total", "Pebble block cache misses.", e.BlockCacheMisses, "")
        writeGauge(&b, "bedrock_engine_block_cache_size_bytes", "Pebble block cache usage.", float64(e.BlockCacheSize))
        writeGauge(&b, "bedrock_engine_memtable_size_bytes", "Current memtable size.", float64(e.MemTableSize))
        writeGauge(&b, "bedrock_engine_memtable_count", "Number of immutable+active memtables.", float64(e.MemTableCount))
        writeGauge(&b, "bedrock_engine_wal_files", "Live WAL files.", float64(e.WALFiles))
        writeGauge(&b, "bedrock_engine_wal_size_bytes", "Live WAL size.", float64(e.WALSize))
        writeCounter(&b, "bedrock_engine_wal_bytes_written_total", "Bytes written to the WAL.", e.WALBytesWritten, "")
        writeCounter(&b, "bedrock_engine_flushes_total", "Memtable flushes.", uint64(e.FlushCount), "")
        writeCounter(&b, "bedrock_engine_compactions_total", "Completed compactions.", uint64(e.CompactionCount), "")
        writeGauge(&b, "bedrock_engine_compaction_debt_bytes", "Estimated compaction debt.", float64(e.CompactionDebt))
        writeGauge(&b, "bedrock_engine_l0_files", "L0 SSTable files (high values hurt reads).", float64(e.L0Files))
        writeGauge(&b, "bedrock_engine_disk_size_bytes", "Approximate on-disk size.", float64(e.DiskSize))
        writeGauge(&b, "bedrock_engine_open_iterators", "Currently open iterators (must return to ~0).", float64(e.OpenIters))
        writeGauge(&b, "bedrock_uptime_seconds", "Time since store open.", snap.Uptime.Seconds())

        return b.String()
}

// opKindByName resolves an op label back to its registry slot.
func opKindByName(name string) (OpKind, bool) {
        for k := 0; k < int(opKindCount); k++ {
                if opKindNames[k] == name {
                        return OpKind(k), true
                }
        }
        return 0, false
}

// formatLE renders a Prometheus le label: plain integers keep format
// stability across scrapes.
func formatLE(ns uint64) string {
        return fmt.Sprintf("%.9f", float64(ns)/1e9)
}

func writeCounter(b *strings.Builder, name, help string, v uint64, extraLabels string) {
        if help != "" {
                fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
        }
        base := strings.SplitN(name, "{", 2)[0]
        fmt.Fprintf(b, "%s{%s} %d\n", base, labelsTail(extraLabels), v)
}

// labelsTail trims the leading comma for label-less counters.
func labelsTail(extra string) string {
        if len(extra) > 0 && extra[0] == ',' {
                return extra[1:]
        }
        return extra
}

func writeGauge(b *strings.Builder, name, help string, v float64) {
        if help != "" {
                fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s gauge\n", name, help, name)
        }
        fmt.Fprintf(b, "%s %s\n", name, formatFloat(v))
}

// formatFloat avoids exponent formatting for common small values so that
// naive scrapers stay happy.
func formatFloat(v float64) string {
        switch {
        case v == float64(int64(v)):
                return fmt.Sprintf("%d", int64(v))
        default:
                return fmt.Sprintf("%.4f", v)
        }
}
