package errors

import (
	"errors"
	"fmt"
	"time"
)

// Common errors used across the application
var (
	// Configuration errors
	ErrNilConfig   = errors.New("config cannot be nil")
	ErrNilLogger   = errors.New("logger cannot be nil")
	ErrEmptyConfig = errors.New("config is empty or invalid")

	// Connection errors
	ErrConnectionFailed  = errors.New("failed to establish connection")
	ErrConnectionClosed  = errors.New("connection is closed")
	ErrConnectionTimeout = errors.New("connection timeout")

	// Request errors
	ErrEmptySQL       = errors.New("sql query cannot be empty")
	ErrNilRequest     = errors.New("request cannot be nil")
	ErrInvalidRequest = errors.New("invalid request format")
	ErrRequestTimeout = errors.New("request timeout")

	// Node errors
	ErrNoNodesAvailable = errors.New("no nodes available for routing")
	ErrNodeUnhealthy    = errors.New("node is unhealthy")
	ErrNilResponse      = errors.New("received nil response from node")

	// Data processing errors
	ErrNilArgument     = errors.New("argument cannot be nil")
	ErrUnsupportedType = errors.New("unsupported argument type")
	ErrMarshalFailed   = errors.New("failed to marshal data")
	ErrUnmarshalFailed = errors.New("failed to unmarshal data")

	// Producer/Consumer errors
	ErrPublishFailed = errors.New("failed to publish message")
	ErrConsumeFailed = errors.New("failed to consume message")
)

// Error types for better error handling
type ErrorType string

const (
	ErrorTypeConnection ErrorType = "connection"
	ErrorTypeRequest    ErrorType = "request"
	ErrorTypeNode       ErrorType = "node"
	ErrorTypeData       ErrorType = "data"
	ErrorTypeTimeout    ErrorType = "timeout"
	ErrorTypeValidation ErrorType = "validation"
	ErrorTypeCircuit    ErrorType = "circuit_breaker"
	ErrorTypeHealth     ErrorType = "health_check"
	ErrorTypeProducer   ErrorType = "producer"
	ErrorTypeConsumer   ErrorType = "consumer"
	ErrorTypeRetry      ErrorType = "retry"
	ErrorTypeConfig     ErrorType = "config"
)

// ApplicationError represents a structured error with type and context
type ApplicationError struct {
	Type    ErrorType
	Message string
	Cause   error
	Context map[string]interface{}
}

func (e *ApplicationError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *ApplicationError) Unwrap() error {
	return e.Cause
}

// NewApplicationError creates a new ApplicationError
func NewApplicationError(errType ErrorType, message string, cause error) *ApplicationError {
	return &ApplicationError{
		Type:    errType,
		Message: message,
		Cause:   cause,
		Context: make(map[string]interface{}),
	}
}

// WithContext adds context to the error
func (e *ApplicationError) WithContext(key string, value interface{}) *ApplicationError {
	e.Context[key] = value
	return e
}

// ErrorSeverity represents the severity of an error
type ErrorSeverity string

const (
	SeverityLow      ErrorSeverity = "low"
	SeverityMedium   ErrorSeverity = "medium"
	SeverityHigh     ErrorSeverity = "high"
	SeverityCritical ErrorSeverity = "critical"
)

// EnhancedError provides detailed error information with metrics support
type EnhancedError struct {
	Type        ErrorType              `json:"type"`
	Severity    ErrorSeverity          `json:"severity"`
	Message     string                 `json:"message"`
	Details     string                 `json:"details,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	Retryable   bool                   `json:"retryable"`
	NodeAddress string                 `json:"node_address,omitempty"`
	RequestID   string                 `json:"request_id,omitempty"`
	Cause       error                  `json:"cause,omitempty"`
	Context     map[string]interface{} `json:"context,omitempty"`
}

// Error implements the error interface
func (e *EnhancedError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("%s [%s]: %s - %s", e.Type, e.Severity, e.Message, e.Details)
	}
	return fmt.Sprintf("%s [%s]: %s", e.Type, e.Severity, e.Message)
}

// Unwrap returns the underlying cause
func (e *EnhancedError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns true if the error is retryable
func (e *EnhancedError) IsRetryable() bool {
	return e.Retryable
}

// GetMetricLabels returns labels for metrics collection
func (e *EnhancedError) GetMetricLabels() map[string]string {
	labels := map[string]string{
		"error_type":     string(e.Type),
		"error_severity": string(e.Severity),
		"retryable":      fmt.Sprintf("%t", e.Retryable),
	}

	if e.NodeAddress != "" {
		labels["node_address"] = e.NodeAddress
	}

	return labels
}

// NewEnhancedError creates a new enhanced error
func NewEnhancedError(errorType ErrorType, severity ErrorSeverity, message string) *EnhancedError {
	return &EnhancedError{
		Type:      errorType,
		Severity:  severity,
		Message:   message,
		Timestamp: time.Now(),
		Retryable: isDefaultRetryable(errorType),
		Context:   make(map[string]interface{}),
	}
}

// NewEnhancedErrorWithDetails creates a new enhanced error with details
func NewEnhancedErrorWithDetails(errorType ErrorType, severity ErrorSeverity, message, details string) *EnhancedError {
	return &EnhancedError{
		Type:      errorType,
		Severity:  severity,
		Message:   message,
		Details:   details,
		Timestamp: time.Now(),
		Retryable: isDefaultRetryable(errorType),
		Context:   make(map[string]interface{}),
	}
}

// WrapEnhancedError wraps an existing error with enhanced information
func WrapEnhancedError(cause error, errorType ErrorType, severity ErrorSeverity, message string) *EnhancedError {
	return &EnhancedError{
		Type:      errorType,
		Severity:  severity,
		Message:   message,
		Timestamp: time.Now(),
		Cause:     cause,
		Retryable: isDefaultRetryable(errorType),
		Context:   make(map[string]interface{}),
	}
}

// WithNodeAddress adds node address information to the error
func (e *EnhancedError) WithNodeAddress(address string) *EnhancedError {
	e.NodeAddress = address
	return e
}

// WithRequestID adds request ID information to the error
func (e *EnhancedError) WithRequestID(requestID string) *EnhancedError {
	e.RequestID = requestID
	return e
}

// WithRetryable sets the retryable flag
func (e *EnhancedError) WithRetryable(retryable bool) *EnhancedError {
	e.Retryable = retryable
	return e
}

// WithContextValue adds context to the error
func (e *EnhancedError) WithContextValue(key string, value interface{}) *EnhancedError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// isDefaultRetryable returns default retryability based on error type
func isDefaultRetryable(errorType ErrorType) bool {
	switch errorType {
	case ErrorTypeConnection, ErrorTypeTimeout, ErrorTypeNode:
		return true
	case ErrorTypeConfig, ErrorTypeValidation:
		return false
	case ErrorTypeCircuit, ErrorTypeRetry:
		return false
	case ErrorTypeHealth:
		return true
	case ErrorTypeProducer, ErrorTypeConsumer:
		return true
	default:
		return true
	}
}

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if enhanced, ok := err.(*EnhancedError); ok {
		return enhanced.IsRetryable()
	}
	if app, ok := err.(*ApplicationError); ok {
		// Check context for retryable flag
		if retryable, exists := app.Context["retryable"]; exists {
			if b, ok := retryable.(bool); ok {
				return b
			}
		}
		return isDefaultRetryable(app.Type)
	}
	return true // Default to retryable for unknown error types
}

// GetErrorType extracts error type from any error
func GetErrorType(err error) ErrorType {
	if enhanced, ok := err.(*EnhancedError); ok {
		return enhanced.Type
	}
	if app, ok := err.(*ApplicationError); ok {
		return app.Type
	}
	return "unknown"
}

// GetErrorSeverity extracts error severity from any error
func GetErrorSeverity(err error) ErrorSeverity {
	if enhanced, ok := err.(*EnhancedError); ok {
		return enhanced.Severity
	}
	return SeverityMedium // Default severity
}
