package bedrock

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// Sentinel errors returned by the store. All are comparable with errors.Is.
var (
	// ErrNotFound is returned by Get when the key does not exist. Pebble's
	// own ErrNotFound is mapped to this sentinel so callers never depend on
	// the engine type. It is treated as a normal outcome, not a failure.
	ErrNotFound = errors.New("bedrock: key not found")

	// ErrClosed is returned when an operation is attempted on a store that
	// has already been closed.
	ErrClosed = errors.New("bedrock: store is closed")

	// ErrShuttingDown is returned when the store is draining in-flight
	// operations during graceful shutdown and rejects new work.
	ErrShuttingDown = errors.New("bedrock: store is shutting down")

	// ErrCircuitOpen is returned when the circuit breaker is open, i.e. the
	// store has detected sustained failures and is shedding load to allow
	// the backend to recover.
	ErrCircuitOpen = errors.New("bedrock: circuit breaker is open")

	// ErrEmptyKey is returned when a nil or zero-length key is supplied.
	// Empty keys are rejected because Pebble orders by bytewise comparison
	// and empty keys corrupt range semantics (e.g. Scan bounds).
	ErrEmptyKey = errors.New("bedrock: empty key")

	// ErrEmptyBatch is returned by Batch when the operation list is empty.
	ErrEmptyBatch = errors.New("bedrock: empty batch")

	// ErrTooManyOps is returned by Batch when the operation list exceeds
	// Config.MaxBatchOps.
	ErrTooManyOps = errors.New("bedrock: batch exceeds max ops")

	// ErrInvalidOperation is returned when an Operation has an unknown type
	// or is missing required fields for its type.
	ErrInvalidOperation = errors.New("bedrock: invalid operation")

	// ErrStoreNotFound is returned by Repository lookups for a name that has
	// never been opened.
	ErrStoreNotFound = errors.New("bedrock: store not found in repository")

	// ErrRepositoryClosing is returned when opening a store through a
	// repository that is shutting down.
	ErrRepositoryClosing = errors.New("bedrock: repository is closing")

	// ErrCheckpointExists is returned when creating a checkpoint into an
	// existing non-empty destination directory.
	ErrCheckpointExists = errors.New("bedrock: checkpoint destination already exists")

	// ErrInvalidConfig is returned by Config.Validate when configuration
	// values are out of range.
	ErrInvalidConfig = errors.New("bedrock: invalid config")
)

// ErrorClass categorizes an error for retry policies, alerting and circuit
// breaker accounting.
type ErrorClass uint8

const (
	// ClassOK means the error is a normal, expected outcome (ErrNotFound).
	ClassOK ErrorClass = iota
	// ClassTransient errors are retryable (I/O timeouts, breaker open,
	// shutdown in progress).
	ClassTransient
	// ClassPermanent errors are not retryable without operator intervention
	// (corruption, invalid usage).
	ClassPermanent
)

// String implements fmt.Stringer.
func (c ErrorClass) String() string {
	switch c {
	case ClassOK:
		return "ok"
	case ClassTransient:
		return "transient"
	case ClassPermanent:
		return "permanent"
	default:
		return "unknown"
	}
}

// StoreError wraps an underlying error with operation and key context. It is
// returned by every store method that fails at the engine level. Use
// errors.Is/As to inspect: it always unwraps to the cause.
type StoreError struct {
	Op  string // "get", "set", "delete", "batch", "scan", ...
	Key []byte // affected key, may be nil for batch/scan
	Err error  // underlying cause
}

// Error implements the error interface.
func (e *StoreError) Error() string {
	if e.Key == nil {
		return fmt.Sprintf("bedrock: %s: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("bedrock: %s key=%q: %v", e.Op, truncateKey(e.Key), e.Err)
}

// Unwrap exposes the cause for errors.Is / errors.As chains.
func (e *StoreError) Unwrap() error { return e.Err }

// Classify maps an error to its ErrorClass.
func (e *StoreError) Classify() ErrorClass { return Classify(e.Err) }

// Classify inspects any error and returns its class. Expected outcomes such
// as ErrNotFound and ErrEmptyKey classify as ClassOK; shutdown and breaker
// errors as ClassTransient; everything else as ClassPermanent.
func Classify(err error) ErrorClass {
	switch {
	case err == nil:
		return ClassOK
	case errors.Is(err, ErrNotFound), errors.Is(err, ErrEmptyKey):
		return ClassOK
	case errors.Is(err, ErrCircuitOpen),
		errors.Is(err, ErrShuttingDown),
		errors.Is(err, ErrClosed):
		return ClassTransient
	case errors.Is(err, pebble.ErrCorruption),
		errors.Is(err, ErrInvalidConfig),
		errors.Is(err, ErrInvalidOperation):
		return ClassPermanent
	default:
		return ClassTransient
	}
}

// mapEngineError converts a raw pebble error into the module's error
// taxonomy. It returns nil-normalized sentinels where possible so that
// callers can compare with == in the hottest paths.
func mapEngineError(op string, key []byte, err error) error {
	if err == nil {
		return nil
	}
	if err == pebble.ErrNotFound {
		return ErrNotFound
	}
	return &StoreError{Op: op, Key: key, Err: err}
}

func truncateKey(key []byte) string {
	const max = 64
	if len(key) <= max {
		return string(key)
	}
	return string(key[:max]) + "..."
}
