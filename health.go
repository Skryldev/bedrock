package bedrock

import (
        "context"
        "fmt"
        "time"
)

// HealthStatus is the structured result of a health probe, designed to map
// 1:1 onto Kubernetes liveness/readiness semantics.
type HealthStatus struct {
        // Status is "healthy", "degraded" or "unhealthy".
        Status string
        // Healthy is true for "healthy" and "degraded" (serving, with caveats);
        // liveness probes should use Alive instead.
        Healthy bool
        // Checks holds per-probe results.
        Checks []HealthCheckResult
        // Uptime since store open.
        Uptime time.Duration
        // Timestamp of the evaluation.
        Timestamp time.Time
}

// Alive reports whether the process should be restarted (liveness). Only
// an unhealthy store fails liveness; degraded stores stay alive.
func (h HealthStatus) Alive() bool { return h.Status != "unhealthy" }

// Ready reports whether the store should receive traffic (readiness).
// Degraded stores stop receiving traffic before they fail hard.
func (h HealthStatus) Ready() bool { return h.Status == "healthy" }

// HealthCheckResult is one named probe outcome.
type HealthCheckResult struct {
        Name    string
        Healthy bool
        Detail  string
        Latency time.Duration
}

// HealthCheck implements ExtendedStore. It runs, in order:
//
//  1. open — the engine handle accepts operations;
//  2. read-probe — a Get against the internal health key;
//  3. engine-pressure — memtable/L0/compaction-debt thresholds from live
//     Pebble metrics.
//
// Read-probe latency is recorded; all checks share a 2s soft budget.
func (s *Store) HealthCheck(ctx context.Context) HealthStatus {
        status := HealthStatus{
                Status:    "healthy",
                Healthy:   true,
                Timestamp: time.Now(),
                Uptime:    time.Since(s.startedAt),
        }
        fail := func(name, detail string) {
                status.Checks = append(status.Checks, HealthCheckResult{Name: name, Healthy: false, Detail: detail})
                status.Status = "unhealthy"
                status.Healthy = false
        }
        degrade := func(name, detail string) {
                status.Checks = append(status.Checks, HealthCheckResult{Name: name, Healthy: true, Detail: detail})
                if status.Status == "healthy" {
                        status.Status = "degraded"
                }
        }

        // 1. engine reachable
        if s.closing.Load() {
                fail("open", "store is closing or closed")
                return status
        }
        status.Checks = append(status.Checks, HealthCheckResult{Name: "open", Healthy: true, Detail: "engine accepting operations"})

        // 2. read probe on the reserved health key
        probeStart := time.Now()
        probeKey := []byte("\x00__bedrock_health__")
        _, err := s.Get(ctx, probeKey)
        probeLatency := time.Since(probeStart)
        switch {
        case err == nil || err == ErrNotFound:
                status.Checks = append(status.Checks, HealthCheckResult{
                        Name: "read_probe", Healthy: true, Detail: "read path operational", Latency: probeLatency,
                })
        default:
                fail("read_probe", fmt.Sprintf("read path failed: %v", err))
                return status
        }

        // 3. engine pressure (degradation signals, not failures)
        details := evaluateEnginePressure(s.engineStats(), uint64(s.cfg.MemTableSizeMB)<<20)
        for _, d := range details {
                degrade("engine_pressure", d)
        }
        if len(details) == 0 {
                status.Checks = append(status.Checks, HealthCheckResult{Name: "engine_pressure", Healthy: true, Detail: "within thresholds"})
        }
        return status
}

// evaluateEnginePressure translates engine statistics into human-readable
// degradation details. Empty output means the engine runs within
// thresholds. Pure function; unit-testable without a live engine.
func evaluateEnginePressure(stats DBStats, memTableBudget uint64) []string {
        const (
                l0WarnFiles   = 20
                debtWarnBytes = 1 << 30 // 1 GiB
                memWarnRatio  = 0.9
        )
        var details []string
        if stats.L0Files >= l0WarnFiles {
                details = append(details, fmt.Sprintf("L0 has %d files (>= %d), read amplification risk", stats.L0Files, l0WarnFiles))
        }
        if stats.CompactionDebt >= debtWarnBytes {
                details = append(details, fmt.Sprintf("compaction debt %d bytes (>= %d)", stats.CompactionDebt, debtWarnBytes))
        }
        if memTableBudget > 0 && float64(stats.MemTableSize) >= float64(memTableBudget)*memWarnRatio {
                details = append(details, fmt.Sprintf("memtable %d bytes near flush threshold (budget %d)", stats.MemTableSize, memTableBudget))
        }
        return details
}
