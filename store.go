package bedrock

import (
        "context"
        "time"
)

// OpType identifies the mutation performed by an Operation inside a batch.
type OpType uint8

const (
        // OpSet stores value at key.
        OpSet OpType = iota
        // OpDelete removes key (idempotent; deleting a missing key succeeds).
        OpDelete
        // OpDeleteRange removes all keys in [Key, End). Key must be < End.
        OpDeleteRange
        // OpMerge appends value to key using Pebble's merge operator. The
        // default merge operator concatenates byte strings; supply a custom
        // Comparer/MergeOperator through engine options for other semantics.
        OpMerge
)

// String implements fmt.Stringer.
func (t OpType) String() string {
        switch t {
        case OpSet:
                return "set"
        case OpDelete:
                return "delete"
        case OpDeleteRange:
                return "delete_range"
        case OpMerge:
                return "merge"
        default:
                return "unknown"
        }
}

// Operation is a single mutation inside an atomic batch. The memory of Key,
// Value and End is owned by the caller; the store copies nothing before the
// pebble batch does, so callers must not mutate these slices concurrently
// with Batch execution.
type Operation struct {
        Type  OpType
        Key   []byte // target key; for OpDeleteRange the inclusive start
        Value []byte // OpSet / OpMerge payload
        End   []byte // OpDeleteRange exclusive upper bound
}

// validate checks the operation for structural correctness.
func (op Operation) validate() error {
        if len(op.Key) == 0 {
                return ErrEmptyKey
        }
        switch op.Type {
        case OpSet, OpMerge:
                return nil // empty values are legal
        case OpDelete:
                return nil
        case OpDeleteRange:
                if len(op.End) == 0 {
                        return ErrInvalidOperation
                }
                return nil
        default:
                return ErrInvalidOperation
        }
}

// Iterator is a stable, forward/backward cursor over a snapshot-consistent
// range. Returned by Scan. The Key and Value slices are owned by the store
// and are only valid until the next positioning call (Next, Prev, SeekGE,
// SeekLT, First, Last) or Close. Copy explicitly to retain.
//
// The iterator must be Closed to release engine resources; leaking iterators
// pins memtables and blocks compaction.
type Iterator interface {
        // Valid reports whether the iterator currently points at an entry.
        Valid() bool
        // Key returns the current key; nil when !Valid.
        Key() []byte
        // Value returns the current value; nil when !Valid.
        Value() []byte
        // Next advances to the next (greater) key. Returns false when exhausted
        // or when an error occurred; check Error afterwards.
        Next() bool
        // Prev steps back to the previous (smaller) key.
        Prev() bool
        // SeekGE positions at the first key >= target.
        SeekGE(key []byte) bool
        // SeekLT positions at the last key < target.
        SeekLT(key []byte) bool
        // First positions at the smallest key in the range.
        First() bool
        // Last positions at the largest key in the range.
        Last() bool
        // Error returns the first error encountered, if any.
        Error() error
        // Close releases engine resources. It is safe to call Close on an
        // iterator whose Error is non-nil; Close returns that error as well.
        Close() error
}

// ScanOptions tunes range scans. The zero value scans the whole keyspace
// with prefetching disabled.
type ScanOptions struct {
        // Prefetch copies up to Prefetch entries ahead of the consumer position
        // into a ring buffer. This hides iterator-return latency and warms the
        // block cache for bulk scans. Default 0 (pass-through, no copying).
        // Values larger than 4096 are clamped.
        Prefetch int
}

// WriteBatch is a mutable, reusable, atomic write buffer. Obtain one with
// ExtendedStore.NewWriteBatch, enqueue mutations, then Commit (all-or-
// nothing) or Reset (discard and reuse). Close always releases pooled
// resources, including after Commit.
type WriteBatch interface {
        Set(key, value []byte) error
        // SetWithTTL pins the hot-cache entry for ttl on commit.
        SetWithTTL(key, value []byte, ttl time.Duration) error
        Delete(key []byte) error
        DeleteRange(start, end []byte) error
        Merge(key, value []byte) error
        Count() int
        Len() int // encoded size in bytes
        Commit(ctx context.Context) error
        Reset()
        Close() error
}

// bedrock is the core synchronous key-value API. All methods are safe
// for concurrent use by multiple goroutines.
//
// Performance contract: methods never spawn goroutines and never allocate
// beyond the returned value; hot-cache hits on Get are allocation-free.
type Bedrock interface {
        // Get retrieves the value for key. Returns ErrNotFound when absent.
        // The returned slice is store-owned and read-only (see package docs).
        Get(ctx context.Context, key []byte) ([]byte, error)

        // Set stores value at key. Empty values are allowed. The write is
        // durable according to Config.SyncWrites (WAL fsync policy).
        Set(ctx context.Context, key, value []byte) error

        // Delete removes key. Deleting a non-existent key succeeds (idempotent).
        Delete(ctx context.Context, key []byte) error

        // Batch applies all operations atomically: either every operation is
        // visible after the call returns, or none is (crash-consistent via the
        // Pebble WAL). Cache invalidation happens only after a successful
        // commit.
        Batch(ctx context.Context, ops []Operation) error

        // Scan returns an iterator over the half-open key range [start, end).
        // nil start scans from the smallest key; nil end scans to the largest.
        // Callers must Close the iterator.
        Scan(ctx context.Context, start, end []byte) (Iterator, error)

        // Close drains in-flight operations, releases pooled resources and
        // closes the engine. Subsequent operations return ErrShuttingDown or
        // ErrClosed. It is safe to call Close multiple times.
        Close() error
}

// ExtendedStore is the full-featured surface implemented by *Store. It is a
// superset of bedrock exposing TTL writes, streaming batches, maintenance
// operations, observability snapshots and health checks.
type ExtendedStore interface {
        Bedrock

        // Name returns the logical instance name.
        Name() string

        // SetWithTTL stores value at key and pins it in the hot cache for ttl
        // (overriding the default TTL). The engine itself has no TTL: expiry
        // applies to the hot cache only.
        SetWithTTL(ctx context.Context, key, value []byte, ttl time.Duration) error

        // Merge appends value to key under Pebble's merge operator.
        Merge(ctx context.Context, key, value []byte) error

        // NewWriteBatch returns a pooled reusable atomic write buffer. Close
        // must be called when done.
        NewWriteBatch() WriteBatch

        // Flush forces a memtable flush to SSTables. Useful before checkpoints
        // and in tests.
        Flush(ctx context.Context) error

        // CompactRange compacts the given key range ([start, end); nil/nil = all
        // keys), merging delete tombstones and reducing read amplification.
        CompactRange(ctx context.Context, start, end []byte) error

        // CreateCheckpoint writes a crash-consistent hardlink snapshot of the
        // database into destDir. The destination must not exist.
        CreateCheckpoint(destDir string) error

        // MetricsSnapshot returns a consistent view of all counters, latency
        // percentiles and engine statistics.
        MetricsSnapshot() MetricsSnapshot

        // PrometheusMetrics renders metrics in Prometheus text exposition
        // format, including mapped engine-level gauges.
        PrometheusMetrics() string

        // HealthCheck probes the store and returns a structured status suitable
        // for Kubernetes readiness/liveness wiring.
        HealthCheck(ctx context.Context) HealthStatus

        // CacheStats returns hot-cache occupancy (0 when disabled).
        CacheStats() (entries int, bytes int64)
}
