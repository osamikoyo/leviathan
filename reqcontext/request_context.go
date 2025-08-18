package reqcontext

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// RequestContext holds metadata for request tracing and correlation
type RequestContext struct {
	RequestID     string            `json:"request_id"`
	CorrelationID string            `json:"correlation_id"`
	UserID        string            `json:"user_id,omitempty"`
	Source        string            `json:"source"`
	StartTime     time.Time         `json:"start_time"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

// ContextKey is the type used for context keys
type ContextKey string

const (
	// RequestContextKey is the key for storing RequestContext in context.Context
	RequestContextKey ContextKey = "request_context"
)

// NewRequestContext creates a new RequestContext with generated IDs
func NewRequestContext(source string) *RequestContext {
	return &RequestContext{
		RequestID:     generateID(),
		CorrelationID: generateID(),
		Source:        source,
		StartTime:     time.Now(),
		Metadata:      make(map[string]string),
	}
}

// NewRequestContextWithCorrelationID creates a new RequestContext with existing correlation ID
func NewRequestContextWithCorrelationID(source, correlationID string) *RequestContext {
	return &RequestContext{
		RequestID:     generateID(),
		CorrelationID: correlationID,
		Source:        source,
		StartTime:     time.Now(),
		Metadata:      make(map[string]string),
	}
}

// generateID generates a new unique ID
func generateID() string {
	return uuid.New().String()
}

// WithRequestContext adds RequestContext to context.Context
func WithRequestContext(ctx context.Context, reqCtx *RequestContext) context.Context {
	return context.WithValue(ctx, RequestContextKey, reqCtx)
}

// FromContext extracts RequestContext from context.Context
func FromContext(ctx context.Context) (*RequestContext, bool) {
	reqCtx, ok := ctx.Value(RequestContextKey).(*RequestContext)
	return reqCtx, ok
}

// MustFromContext extracts RequestContext from context.Context, creating one if not found
func MustFromContext(ctx context.Context) *RequestContext {
	if reqCtx, ok := FromContext(ctx); ok {
		return reqCtx
	}
	// Create a new context if one doesn't exist
	return NewRequestContext("unknown")
}

// WithMetadata adds metadata to the request context
func (rc *RequestContext) WithMetadata(key, value string) *RequestContext {
	if rc.Metadata == nil {
		rc.Metadata = make(map[string]string)
	}
	rc.Metadata[key] = value
	return rc
}

// WithUserID sets the user ID for the request
func (rc *RequestContext) WithUserID(userID string) *RequestContext {
	rc.UserID = userID
	return rc
}

// GetMetadata retrieves metadata from the request context
func (rc *RequestContext) GetMetadata(key string) (string, bool) {
	if rc.Metadata == nil {
		return "", false
	}
	value, exists := rc.Metadata[key]
	return value, exists
}

// Duration returns the time elapsed since the request started
func (rc *RequestContext) Duration() time.Duration {
	return time.Since(rc.StartTime)
}

// LogFields returns zap fields for logging
func (rc *RequestContext) LogFields() []zap.Field {
	fields := []zap.Field{
		zap.String("request_id", rc.RequestID),
		zap.String("correlation_id", rc.CorrelationID),
		zap.String("source", rc.Source),
		zap.Duration("duration", rc.Duration()),
	}

	if rc.UserID != "" {
		fields = append(fields, zap.String("user_id", rc.UserID))
	}

	if len(rc.Metadata) > 0 {
		fields = append(fields, zap.Any("metadata", rc.Metadata))
	}

	return fields
}

// MetricLabels returns labels for Prometheus metrics
func (rc *RequestContext) MetricLabels() map[string]string {
	labels := map[string]string{
		"source": rc.Source,
	}

	if rc.UserID != "" {
		labels["user_id"] = rc.UserID
	}

	// Add selected metadata as labels (be careful not to create too many label combinations)
	if nodeAddr, exists := rc.GetMetadata("node_address"); exists {
		labels["node_address"] = nodeAddr
	}

	if requestType, exists := rc.GetMetadata("request_type"); exists {
		labels["request_type"] = requestType
	}

	return labels
}

// String returns a string representation of the request context
func (rc *RequestContext) String() string {
	return fmt.Sprintf("RequestID=%s CorrelationID=%s Source=%s Duration=%v",
		rc.RequestID, rc.CorrelationID, rc.Source, rc.Duration())
}

// Clone creates a copy of the request context with a new request ID but same correlation ID
func (rc *RequestContext) Clone() *RequestContext {
	clone := &RequestContext{
		RequestID:     generateID(),
		CorrelationID: rc.CorrelationID,
		UserID:        rc.UserID,
		Source:        rc.Source,
		StartTime:     time.Now(),
		Metadata:      make(map[string]string),
	}

	// Copy metadata
	for k, v := range rc.Metadata {
		clone.Metadata[k] = v
	}

	return clone
}

// ContextLogger creates a logger with request context fields
func (rc *RequestContext) ContextLogger(logger *zap.Logger) *zap.Logger {
	return logger.With(rc.LogFields()...)
}

// RequestInfo holds basic request information for middleware
type RequestInfo struct {
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers,omitempty"`
	UserAgent string            `json:"user_agent,omitempty"`
	RemoteIP  string            `json:"remote_ip,omitempty"`
}

// ExtendedRequestContext includes HTTP-specific information
type ExtendedRequestContext struct {
	*RequestContext
	RequestInfo *RequestInfo `json:"request_info,omitempty"`
}

// NewExtendedRequestContext creates a new ExtendedRequestContext
func NewExtendedRequestContext(source string, requestInfo *RequestInfo) *ExtendedRequestContext {
	return &ExtendedRequestContext{
		RequestContext: NewRequestContext(source),
		RequestInfo:    requestInfo,
	}
}

// WithRequestInfo adds request information to the context
func (rc *RequestContext) WithRequestInfo(info *RequestInfo) *ExtendedRequestContext {
	return &ExtendedRequestContext{
		RequestContext: rc,
		RequestInfo:    info,
	}
}

// LogFields returns zap fields including request info
func (erc *ExtendedRequestContext) LogFields() []zap.Field {
	fields := erc.RequestContext.LogFields()

	if erc.RequestInfo != nil {
		fields = append(fields,
			zap.String("method", erc.RequestInfo.Method),
			zap.String("path", erc.RequestInfo.Path),
		)

		if erc.RequestInfo.UserAgent != "" {
			fields = append(fields, zap.String("user_agent", erc.RequestInfo.UserAgent))
		}

		if erc.RequestInfo.RemoteIP != "" {
			fields = append(fields, zap.String("remote_ip", erc.RequestInfo.RemoteIP))
		}

		if len(erc.RequestInfo.Headers) > 0 {
			fields = append(fields, zap.Any("headers", erc.RequestInfo.Headers))
		}
	}

	return fields
}

// TraceSpan represents a span for distributed tracing
type TraceSpan struct {
	SpanID    string            `json:"span_id"`
	ParentID  string            `json:"parent_id,omitempty"`
	Operation string            `json:"operation"`
	StartTime time.Time         `json:"start_time"`
	EndTime   *time.Time        `json:"end_time,omitempty"`
	Tags      map[string]string `json:"tags,omitempty"`
	Success   bool              `json:"success"`
	Error     string            `json:"error,omitempty"`
}

// NewTraceSpan creates a new trace span
func NewTraceSpan(operation, parentID string) *TraceSpan {
	return &TraceSpan{
		SpanID:    generateID(),
		ParentID:  parentID,
		Operation: operation,
		StartTime: time.Now(),
		Tags:      make(map[string]string),
		Success:   true,
	}
}

// Finish completes the span
func (ts *TraceSpan) Finish() {
	now := time.Now()
	ts.EndTime = &now
}

// WithTag adds a tag to the span
func (ts *TraceSpan) WithTag(key, value string) *TraceSpan {
	if ts.Tags == nil {
		ts.Tags = make(map[string]string)
	}
	ts.Tags[key] = value
	return ts
}

// WithError marks the span as failed with an error
func (ts *TraceSpan) WithError(err error) *TraceSpan {
	ts.Success = false
	if err != nil {
		ts.Error = err.Error()
	}
	return ts
}

// Duration returns the duration of the span
func (ts *TraceSpan) Duration() time.Duration {
	if ts.EndTime != nil {
		return ts.EndTime.Sub(ts.StartTime)
	}
	return time.Since(ts.StartTime)
}

// LogFields returns zap fields for the span
func (ts *TraceSpan) LogFields() []zap.Field {
	fields := []zap.Field{
		zap.String("span_id", ts.SpanID),
		zap.String("operation", ts.Operation),
		zap.Duration("span_duration", ts.Duration()),
		zap.Bool("success", ts.Success),
	}

	if ts.ParentID != "" {
		fields = append(fields, zap.String("parent_span_id", ts.ParentID))
	}

	if ts.Error != "" {
		fields = append(fields, zap.String("span_error", ts.Error))
	}

	if len(ts.Tags) > 0 {
		fields = append(fields, zap.Any("span_tags", ts.Tags))
	}

	return fields
}
