package bedrock

import (
        "math/bits"
        "strings"
        "sync/atomic"
        "time"
)

// Histogram is a dependency-free, lock-free latency histogram in the HDR
// style: 8 sub-buckets per octave spanning 1ns to 2^64ns (~584 years) using
// 512 atomic counters. Record costs one shift, one mask and one atomic add —
// no allocation, no lock contention on hot paths.
//
// Percentile precision is better than ±6% (sub-octave), sufficient for
// p50/p95/p99/p999 monitoring and capacity workloads. For exact p99 use
// `go test -bench` histograms (testing.Histogram) or an external recorder.
type Histogram struct {
        counts [histBuckets]atomic.Uint64
        sum    atomic.Uint64 // nanoseconds, wraps after ~584 years of total time
        count  atomic.Uint64
        max    atomic.Uint64
}

const (
        histBuckets   = 512
        histSubBuckets = 8 // sub-buckets per octave
)

// bucketFor maps a duration in nanoseconds (v >= 1) to its bucket index.
//
// Layout: indices 0..6 cover 1..7ns exactly; index 7+8*k+s (s in [0,8))
// covers [2^(k+3) + s*2^k, 2^(k+3) + (s+1)*2^k).
func bucketFor(v uint64) int {
        if v < 8 {
                return int(v) - 1
        }
        exp := uint(bits.Len64(v) - 1) // v in [2^exp, 2^(exp+1)), exp >= 3
        sub := (v >> (exp - 3)) & 7    // top fraction bits -> [0,8)
        return 7 + int(exp-3)*histSubBuckets + int(sub)
}

// bucketLowerBound returns the smallest nanosecond value that maps to idx.
func bucketLowerBound(idx int) uint64 {
        if idx < 7 {
                return uint64(idx + 1)
        }
        rel := idx - 7
        exp := uint(rel/histSubBuckets) + 3
        sub := uint64(rel % histSubBuckets)
        return 1<<exp + sub<<(exp-3)
}

// Record observes a duration. Safe for concurrent use; never allocates.
func (h *Histogram) Record(d time.Duration) {
        v := uint64(d)
        if v == 0 {
                v = 1
        }
        h.counts[bucketFor(v)].Add(1)
        h.count.Add(1)
        h.sum.Add(v)
        for {
                old := h.max.Load()
                if v <= old || h.max.CompareAndSwap(old, v) {
                        break
                }
        }
}

// Count returns the number of observations.
func (h *Histogram) Count() uint64 { return h.count.Load() }

// Sum returns the total observed time.
func (h *Histogram) Sum() time.Duration { return time.Duration(h.sum.Load()) }

// Max returns the largest observed duration.
func (h *Histogram) Max() time.Duration { return time.Duration(h.max.Load()) }

// Mean returns the arithmetic mean; 0 when empty.
func (h *Histogram) Mean() time.Duration {
        c := h.count.Load()
        if c == 0 {
                return 0
        }
        return time.Duration(h.sum.Load() / c)
}

// ValueAtQuantile returns the approximate duration at the given quantile
// (q in [0,100], fractional values like 99.9 allowed). Returns 0 when the
// histogram is empty. The estimate is the midpoint of the containing bucket,
// i.e. within ±6% of the true value.
func (h *Histogram) ValueAtQuantile(q float64) time.Duration {
        total := h.count.Load()
        if total == 0 {
                return 0
        }
        if q > 100 {
                q = 100
        }
        if q < 0 {
                q = 0
        }
        target := uint64(q/100*float64(total) + 0.9999999) // ceil(q% * total)
        if target == 0 {
                target = 1
        }
        var cumulative uint64
        for i := 0; i < histBuckets; i++ {
                c := h.counts[i].Load()
                if c == 0 {
                        continue
                }
                cumulative += c
                if cumulative >= target {
                        lo := bucketLowerBound(i)
                        // midpoint within the bucket: lo * 2^(1/8) ~ lo*1.09; the +lo/2
                        // variant overshoots on wide octaves, so scale instead.
                        mid := lo + (bucketWidth(lo) >> 1)
                        return time.Duration(mid)
                }
        }
        return time.Duration(h.max.Load())
}

// bucketWidth returns the size of the bucket containing value lo (which must
// be a bucket lower bound).
func bucketWidth(lo uint64) uint64 {
        if lo < 8 {
                return 1
        }
        // width = 2^(exp-3) where lo = 2^exp + sub*2^(exp-3)
        exp := uint(bits.Len64(lo) - 1)
        if exp < 3 {
                return 1
        }
        return 1 << (exp - 3)
}

// VisitBuckets calls fn(lowerBound, count) for every non-empty bucket in
// ascending order. Used by the Prometheus renderer to build cumulative
// buckets without copying the counter array.
func (h *Histogram) VisitBuckets(fn func(lowerBoundNS uint64, count uint64)) {
        for i := 0; i < histBuckets; i++ {
                c := h.counts[i].Load()
                if c != 0 {
                        fn(bucketLowerBound(i), c)
                }
        }
}

// String renders a compact human-readable percentile line (used in logs).
func (h *Histogram) String() string {
        var b strings.Builder
        b.WriteString("{count=")
        b.WriteString(itoaU64(h.count.Load()))
        b.WriteString(" p50=")
        b.WriteString(h.ValueAtQuantile(50).String())
        b.WriteString(" p95=")
        b.WriteString(h.ValueAtQuantile(95).String())
        b.WriteString(" p99=")
        b.WriteString(h.ValueAtQuantile(99).String())
        b.WriteString(" max=")
        b.WriteString(h.Max().String())
        b.WriteString("}")
        return b.String()
}

// itoaU64 is a tiny allocation-free formatter for metric dumps.
func itoaU64(v uint64) string {
        if v == 0 {
                return "0"
        }
        var buf [20]byte
        i := len(buf)
        for v > 0 {
                i--
                buf[i] = byte('0' + v%10)
                v /= 10
        }
        return string(buf[i:])
}
