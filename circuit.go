package bedrock

import (
	"sync"
	"time"
)

// CircuitState enumerates the circuit breaker states.
type CircuitState uint8

const (
	// CircuitClosed passes all traffic; failures are counted.
	CircuitClosed CircuitState = iota
	// CircuitOpen sheds load, returning ErrCircuitOpen until the cooldown
	// elapses.
	CircuitOpen
	// CircuitHalfOpen admits a limited number of probe operations to test
	// recovery.
	CircuitHalfOpen
)

// String implements fmt.Stringer.
func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// CircuitConfig configures the built-in circuit breaker. Zero durations are
// replaced by the documented defaults when Enabled is true.
type CircuitConfig struct {
	// Enabled activates the breaker. Default when zero value of the whole
	// CircuitConfig is used: disabled; Open() enables it with defaults when
	// the user passes WithCircuitBreaker without filling fields.
	Enabled bool
	// FailureThreshold is the number of failed operations within
	// FailureWindow that trip the breaker. Default 50.
	FailureThreshold int32
	// FailureWindow is the sliding window for counting failures. Default
	// 30s.
	FailureWindow time.Duration
	// Cooldown is how long an open breaker waits before probing. Default
	// 5s.
	Cooldown time.Duration
	// HalfOpenSuccesses is the number of consecutive successful probes
	// required to close the breaker again. Default 3.
	HalfOpenSuccesses int32
}

// defaults fills zero-valued fields with production-sane defaults.
func (c *CircuitConfig) defaults() {
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 50
	}
	if c.FailureWindow <= 0 {
		c.FailureWindow = 30 * time.Second
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 5 * time.Second
	}
	if c.HalfOpenSuccesses <= 0 {
		c.HalfOpenSuccesses = 3
	}
}

// CircuitBreaker implements a classic closed/open/half-open breaker sized
// for a key-value store: failure classification ignores expected outcomes
// (ErrNotFound, ErrEmptyKey); only genuine engine failures count.
type CircuitBreaker struct {
	cfg CircuitConfig

	mu          sync.Mutex
	state       CircuitState
	failures    int32
	successes   int32
	windowStart time.Time
	openedAt    time.Time

	// OnStateChange, when set, is invoked synchronously on transitions.
	// It is set once before use; reads are data-race-free because the
	// breaker is constructed before publication.
	OnStateChange func(from, to CircuitState)
}

// NewCircuitBreaker builds a breaker from cfg; zero config fields receive
// defaults.
func NewCircuitBreaker(cfg CircuitConfig) *CircuitBreaker {
	cfg.defaults()
	return &CircuitBreaker{cfg: cfg, state: CircuitClosed, windowStart: time.Now()}
}

// State returns the current state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.refresh()
	return cb.state
}

// refresh transitions open->half_open when the cooldown elapsed. Callers
// must hold cb.mu.
func (cb *CircuitBreaker) refresh() {
	if cb.state == CircuitOpen && time.Since(cb.openedAt) >= cb.cfg.Cooldown {
		cb.transition(CircuitHalfOpen)
		cb.successes = 0
	}
}

// transition moves state and fires the hook. Callers must hold cb.mu.
func (cb *CircuitBreaker) transition(to CircuitState) {
	if cb.state == to {
		return
	}
	from := cb.state
	cb.state = to
	if to == CircuitOpen {
		cb.openedAt = time.Now()
		cb.failures = 0
		cb.successes = 0
	}
	if to == CircuitClosed {
		cb.failures = 0
		cb.successes = 0
	}
	if cb.OnStateChange != nil {
		cb.OnStateChange(from, to)
	}
}

// Allow reports whether an operation may proceed. It returns ErrCircuitOpen
// when load must be shed. A successful call in the half-open state reserves
// one probe slot.
func (cb *CircuitBreaker) Allow() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.refresh()
	switch cb.state {
	case CircuitClosed:
		return nil
	case CircuitOpen:
		return ErrCircuitOpen
	default: // half_open: allow probes sequentially
		return nil
	}
}

// Record accounts for one completed operation. Errors classified as ClassOK
// (e.g. ErrNotFound) are recorded as successes; ClassPermanent and
// ClassTransient both count as failures for the breaker.
func (cb *CircuitBreaker) Record(err error) {
	cls := Classify(err)
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		if err == nil || cls == ClassOK {
			return
		}
		now := time.Now()
		if now.Sub(cb.windowStart) > cb.cfg.FailureWindow {
			cb.windowStart = now
			cb.failures = 0
		}
		cb.failures++
		if cb.failures >= cb.cfg.FailureThreshold {
			cb.transition(CircuitOpen)
		}
	case CircuitHalfOpen:
		if err == nil || cls == ClassOK {
			cb.successes++
			if cb.successes >= cb.cfg.HalfOpenSuccesses {
				cb.transition(CircuitClosed)
			}
			return
		}
		cb.transition(CircuitOpen)
	default:
		// Record while open: operation should not have run; ignore.
	}
}
