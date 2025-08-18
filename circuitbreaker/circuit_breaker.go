package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/osamikoyo/leviathan/logger"
	"go.uber.org/zap"
)

var (
	ErrCircuitBreakerOpen     = errors.New("circuit breaker is open")
	ErrCircuitBreakerHalfOpen = errors.New("circuit breaker is half-open")
)

// State represents the state of the circuit breaker
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// Config holds configuration for the circuit breaker
type Config struct {
	// MaxFailures is the maximum number of failures allowed before opening the circuit
	MaxFailures int
	// ResetTimeout is the timeout after which the circuit breaker resets from open to half-open
	ResetTimeout time.Duration
	// MonitoringPeriod is the time window for counting failures
	MonitoringPeriod time.Duration
	// HalfOpenMaxRequests is the maximum number of requests allowed in half-open state
	HalfOpenMaxRequests int
	// SuccessThreshold is the number of consecutive successes required to close the circuit from half-open
	SuccessThreshold int
}

// DefaultConfig returns a default circuit breaker configuration
func DefaultConfig() Config {
	return Config{
		MaxFailures:         5,
		ResetTimeout:        60 * time.Second,
		MonitoringPeriod:    60 * time.Second,
		HalfOpenMaxRequests: 3,
		SuccessThreshold:    2,
	}
}

// CircuitBreaker implements the circuit breaker pattern
type CircuitBreaker struct {
	config          Config
	state           State
	failures        int
	requests        int
	successes       int
	lastFailureTime time.Time
	lastStateChange time.Time
	mu              sync.RWMutex
	logger          *logger.Logger
	name            string
}

// New creates a new circuit breaker with the given configuration
func New(name string, config Config, logger *logger.Logger) *CircuitBreaker {
	return &CircuitBreaker{
		config:          config,
		state:           StateClosed,
		lastStateChange: time.Now(),
		logger:          logger,
		name:            name,
	}
}

// Execute runs the given function if the circuit breaker allows it
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if !cb.canExecute() {
		return cb.getCircuitBreakerError()
	}

	cb.beforeRequest()

	err := fn(ctx)

	cb.afterRequest(err == nil)

	return err
}

// canExecute returns true if the request can be executed
func (cb *CircuitBreaker) canExecute() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(cb.lastStateChange) > cb.config.ResetTimeout {
			// Time to try half-open
			return true
		}
		return false
	case StateHalfOpen:
		return cb.requests < cb.config.HalfOpenMaxRequests
	default:
		return false
	}
}

// beforeRequest is called before executing a request
func (cb *CircuitBreaker) beforeRequest() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateOpen && time.Since(cb.lastStateChange) > cb.config.ResetTimeout {
		cb.setState(StateHalfOpen)
		cb.requests = 0
		cb.successes = 0
	}

	cb.requests++
}

// afterRequest is called after executing a request
func (cb *CircuitBreaker) afterRequest(success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if success {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

// onSuccess handles successful requests
func (cb *CircuitBreaker) onSuccess() {
	cb.failures = 0 // Reset failure count on success

	switch cb.state {
	case StateHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
			cb.requests = 0
			cb.successes = 0
		}
	}
}

// onFailure handles failed requests
func (cb *CircuitBreaker) onFailure() {
	cb.failures++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.shouldOpenCircuit() {
			cb.setState(StateOpen)
		}
	case StateHalfOpen:
		cb.setState(StateOpen)
		cb.requests = 0
		cb.successes = 0
	}
}

// shouldOpenCircuit determines if the circuit should be opened
func (cb *CircuitBreaker) shouldOpenCircuit() bool {
	// Check if we have enough failures within the monitoring period
	if cb.failures >= cb.config.MaxFailures {
		// Check if failures occurred within the monitoring period
		if time.Since(cb.lastFailureTime) <= cb.config.MonitoringPeriod {
			return true
		}
	}
	return false
}

// setState changes the circuit breaker state and logs the change
func (cb *CircuitBreaker) setState(newState State) {
	if cb.state != newState {
		oldState := cb.state
		cb.state = newState
		cb.lastStateChange = time.Now()

		cb.logger.Info("circuit breaker state changed",
			zap.String("name", cb.name),
			zap.String("old_state", oldState.String()),
			zap.String("new_state", newState.String()),
			zap.Int("failures", cb.failures),
			zap.Int("requests", cb.requests),
			zap.Int("successes", cb.successes))
	}
}

// getCircuitBreakerError returns the appropriate error based on current state
func (cb *CircuitBreaker) getCircuitBreakerError() error {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch cb.state {
	case StateOpen:
		return fmt.Errorf("%w: %s", ErrCircuitBreakerOpen, cb.name)
	case StateHalfOpen:
		return fmt.Errorf("%w: %s (max requests exceeded)", ErrCircuitBreakerHalfOpen, cb.name)
	default:
		return fmt.Errorf("circuit breaker %s is in invalid state", cb.name)
	}
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// GetStats returns statistics about the circuit breaker
func (cb *CircuitBreaker) GetStats() map[string]interface{} {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return map[string]interface{}{
		"name":              cb.name,
		"state":             cb.state.String(),
		"failures":          cb.failures,
		"requests":          cb.requests,
		"successes":         cb.successes,
		"last_failure_time": cb.lastFailureTime,
		"last_state_change": cb.lastStateChange,
		"time_since_change": time.Since(cb.lastStateChange),
	}
}

// Reset resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed)
	cb.failures = 0
	cb.requests = 0
	cb.successes = 0
}
