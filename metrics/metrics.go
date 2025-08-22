package metrics

import (
	"fmt"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// Existing metrics
	RequestCount      *prometheus.CounterVec
	RequestDuration   *prometheus.HistogramVec
	ActiveConnections prometheus.Gauge
	RequestErrors     *prometheus.CounterVec
	NodeHealthStatus  *prometheus.GaugeVec

	// Enhanced metrics
	NodeRequestDuration  *prometheus.HistogramVec
	NodeRequestCount     *prometheus.CounterVec
	CircuitBreakerStatus *prometheus.GaugeVec
	RetryAttempts        *prometheus.CounterVec
	QueueDepth           *prometheus.GaugeVec
	ConnectionPoolStats  *prometheus.GaugeVec
	ErrorsByType         *prometheus.CounterVec
	ErrorsBySeverity     *prometheus.CounterVec
	HealthCheckDuration  *prometheus.HistogramVec
	ProducerMetrics      *prometheus.CounterVec
	ConsumerMetrics      *prometheus.CounterVec

	once sync.Once
)

func InitMetrics() {
	once.Do(func() {
		RequestCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_requests_total",
			Help: "Total number of requests processed by heart service",
		}, []string{"type"})

		RequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "leviathan_request_duration_seconds",
			Help:    "Time to process request in seconds",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0},
		}, []string{"type"})

		ActiveConnections = prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "leviathan_active_connections",
			Help: "Current number of active connections",
		})

		RequestErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_request_errors_total",
			Help: "Total number of request errors",
		}, []string{"type", "error_type"})

		NodeHealthStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "leviathan_node_health_status",
			Help: "Health status of nodes (1=healthy, 0=unhealthy)",
		}, []string{"node_address"})

		// Enhanced metrics
		NodeRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "leviathan_node_request_duration_seconds",
			Help:    "Time to process request on specific node in seconds",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0},
		}, []string{"node_address", "request_type"})

		NodeRequestCount = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_node_requests_total",
			Help: "Total number of requests sent to specific nodes",
		}, []string{"node_address", "request_type", "status"})

		CircuitBreakerStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "leviathan_circuit_breaker_status",
			Help: "Circuit breaker status (0=closed, 1=open, 0.5=half-open)",
		}, []string{"node_address"})

		RetryAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_retry_attempts_total",
			Help: "Total number of retry attempts",
		}, []string{"node_address", "attempt_number"})

		QueueDepth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "leviathan_queue_depth",
			Help: "Current depth of various queues",
		}, []string{"queue_type", "queue_name"})

		ConnectionPoolStats = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "leviathan_connection_pool_stats",
			Help: "Connection pool statistics",
		}, []string{"node_address", "stat_type"})

		ErrorsByType = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_errors_by_type_total",
			Help: "Total number of errors by type",
		}, []string{"error_type", "error_severity", "retryable"})

		ErrorsBySeverity = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_errors_by_severity_total",
			Help: "Total number of errors by severity",
		}, []string{"error_severity", "node_address"})

		HealthCheckDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "leviathan_health_check_duration_seconds",
			Help:    "Time to complete health check in seconds",
			Buckets: []float64{0.001, 0.01, 0.1, 0.5, 1.0, 2.0, 5.0},
		}, []string{"node_address", "status"})

		ProducerMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_producer_operations_total",
			Help: "Total number of producer operations",
		}, []string{"operation", "status"})

		ConsumerMetrics = prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "leviathan_consumer_operations_total",
			Help: "Total number of consumer operations",
		}, []string{"operation", "status"})

		prometheus.MustRegister(
			// Existing metrics
			RequestCount,
			RequestDuration,
			ActiveConnections,
			RequestErrors,
			NodeHealthStatus,
			// Enhanced metrics
			NodeRequestDuration,
			NodeRequestCount,
			CircuitBreakerStatus,
			RetryAttempts,
			QueueDepth,
			ConnectionPoolStats,
			ErrorsByType,
			ErrorsBySeverity,
			HealthCheckDuration,
			ProducerMetrics,
			ConsumerMetrics,
		)
	})
}

// TrackError tracks an error in metrics
func TrackError(errorType, severity, nodeAddress string, retryable bool) {
	if ErrorsByType != nil {
		ErrorsByType.WithLabelValues(
			errorType,
			severity,
			fmt.Sprintf("%t", retryable),
		).Inc()
	}

	if ErrorsBySeverity != nil {
		ErrorsBySeverity.WithLabelValues(
			severity,
			nodeAddress,
		).Inc()
	}
}

// TrackNodeRequest tracks a node request with timing
func TrackNodeRequest(nodeAddress, requestType, status string, duration float64) {
	if NodeRequestCount != nil {
		NodeRequestCount.WithLabelValues(
			nodeAddress,
			requestType,
			status,
		).Inc()
	}

	if NodeRequestDuration != nil {
		NodeRequestDuration.WithLabelValues(
			nodeAddress,
			requestType,
		).Observe(duration)
	}
}

// UpdateCircuitBreakerStatus updates circuit breaker status metrics
func UpdateCircuitBreakerStatus(nodeAddress string, state string) {
	if CircuitBreakerStatus == nil {
		return
	}

	var value float64
	switch state {
	case "closed":
		value = 0
	case "open":
		value = 1
	case "half-open":
		value = 0.5
	default:
		value = -1 // Unknown state
	}

	CircuitBreakerStatus.WithLabelValues(nodeAddress).Set(value)
}

// TrackRetryAttempt tracks a retry attempt
func TrackRetryAttempt(nodeAddress string, attemptNumber int) {
	if RetryAttempts != nil {
		RetryAttempts.WithLabelValues(
			nodeAddress,
			fmt.Sprintf("%d", attemptNumber),
		).Inc()
	}
}

// UpdateConnectionPoolStats updates connection pool statistics
func UpdateConnectionPoolStats(nodeAddress, statType string, value float64) {
	if ConnectionPoolStats != nil {
		ConnectionPoolStats.WithLabelValues(
			nodeAddress,
			statType,
		).Set(value)
	}
}

// TrackHealthCheck tracks a health check with timing
func TrackHealthCheck(nodeAddress, status string, duration float64) {
	if HealthCheckDuration != nil {
		HealthCheckDuration.WithLabelValues(
			nodeAddress,
			status,
		).Observe(duration)
	}
}

// TrackProducerOperation tracks producer operations
func TrackProducerOperation(operation, status string) {
	if ProducerMetrics != nil {
		ProducerMetrics.WithLabelValues(
			operation,
			status,
		).Inc()
	}
}

// TrackConsumerOperation tracks consumer operations
func TrackConsumerOperation(operation, status string) {
	if ConsumerMetrics != nil {
		ConsumerMetrics.WithLabelValues(
			operation,
			status,
		).Inc()
	}
}

// UpdateQueueDepth updates queue depth metrics
func UpdateQueueDepth(queueType, queueName string, depth float64) {
	if QueueDepth != nil {
		QueueDepth.WithLabelValues(
			queueType,
			queueName,
		).Set(depth)
	}
}
