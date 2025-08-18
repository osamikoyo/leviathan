package nodeclient

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/osamikoyo/leviathan/circuitbreaker"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/nodepb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// Config holds configuration for the enhanced node client
type Config struct {
	// Connection settings
	Address           string
	MaxConnections    int
	ConnectionTimeout time.Duration
	RequestTimeout    time.Duration
	KeepAliveTime     time.Duration
	KeepAliveTimeout  time.Duration

	// Retry settings
	MaxRetries        int
	InitialBackoff    time.Duration
	MaxBackoff        time.Duration
	BackoffMultiplier float64

	// Circuit breaker settings
	CircuitBreakerConfig circuitbreaker.Config
}

// DefaultConfig returns a default configuration
func DefaultConfig(address string) Config {
	return Config{
		Address:              address,
		MaxConnections:       5,
		ConnectionTimeout:    10 * time.Second,
		RequestTimeout:       30 * time.Second,
		KeepAliveTime:        30 * time.Second,
		KeepAliveTimeout:     5 * time.Second,
		MaxRetries:           3,
		InitialBackoff:       100 * time.Millisecond,
		MaxBackoff:           5 * time.Second,
		BackoffMultiplier:    2.0,
		CircuitBreakerConfig: circuitbreaker.DefaultConfig(),
	}
}

// EnhancedNodeClient provides an enhanced client for node communication
type EnhancedNodeClient struct {
	config         Config
	connections    chan *grpc.ClientConn
	circuitBreaker *circuitbreaker.CircuitBreaker
	logger         *logger.Logger
	mu             sync.RWMutex
	closed         bool
	address        string
}

// NewEnhancedNodeClient creates a new enhanced node client
func NewEnhancedNodeClient(config Config, logger *logger.Logger) (*EnhancedNodeClient, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	client := &EnhancedNodeClient{
		config:         config,
		connections:    make(chan *grpc.ClientConn, config.MaxConnections),
		circuitBreaker: circuitbreaker.New(config.Address, config.CircuitBreakerConfig, logger),
		logger:         logger,
		address:        config.Address,
	}

	// Initialize connection pool
	if err := client.initializeConnectionPool(); err != nil {
		return nil, fmt.Errorf("failed to initialize connection pool: %w", err)
	}

	logger.Info("enhanced node client created",
		zap.String("address", config.Address),
		zap.Int("max_connections", config.MaxConnections))

	return client, nil
}

// initializeConnectionPool creates initial connections
func (c *EnhancedNodeClient) initializeConnectionPool() error {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  c.config.InitialBackoff,
				Multiplier: c.config.BackoffMultiplier,
				MaxDelay:   c.config.MaxBackoff,
			},
			MinConnectTimeout: c.config.ConnectionTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.config.KeepAliveTime,
			Timeout:             c.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	// Create initial connections
	for i := 0; i < c.config.MaxConnections; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectionTimeout)
		conn, err := grpc.DialContext(ctx, c.config.Address, opts...)
		cancel()

		if err != nil {
			// Clean up any connections created so far
			c.closeAllConnections()
			return fmt.Errorf("failed to create connection %d to %s: %w", i, c.config.Address, err)
		}

		c.connections <- conn
	}

	return nil
}

// getConnection gets a connection from the pool
func (c *EnhancedNodeClient) getConnection() (*grpc.ClientConn, error) {
	c.mu.RLock()
	if c.closed {
		c.mu.RUnlock()
		return nil, fmt.Errorf("client is closed")
	}
	c.mu.RUnlock()

	select {
	case conn := <-c.connections:
		// Check if connection is still healthy
		if c.isConnectionHealthy(conn) {
			return conn, nil
		}

		// Connection is unhealthy, try to create a new one
		c.logger.Warn("connection is unhealthy, creating new one",
			zap.String("address", c.address))

		conn.Close()
		return c.createNewConnection()

	default:
		// No available connections, create a new one temporarily
		return c.createNewConnection()
	}
}

// returnConnection returns a connection to the pool
func (c *EnhancedNodeClient) returnConnection(conn *grpc.ClientConn) {
	if conn == nil || !c.isConnectionHealthy(conn) {
		if conn != nil {
			conn.Close()
		}
		return
	}

	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()

	if closed {
		conn.Close()
		return
	}

	select {
	case c.connections <- conn:
		// Successfully returned to pool
	default:
		// Pool is full, close the connection
		conn.Close()
	}
}

// createNewConnection creates a new gRPC connection
func (c *EnhancedNodeClient) createNewConnection() (*grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  c.config.InitialBackoff,
				Multiplier: c.config.BackoffMultiplier,
				MaxDelay:   c.config.MaxBackoff,
			},
			MinConnectTimeout: c.config.ConnectionTimeout,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.config.KeepAliveTime,
			Timeout:             c.config.KeepAliveTimeout,
			PermitWithoutStream: true,
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectionTimeout)
	defer cancel()

	return grpc.DialContext(ctx, c.config.Address, opts...)
}

// isConnectionHealthy checks if a connection is healthy
func (c *EnhancedNodeClient) isConnectionHealthy(conn *grpc.ClientConn) bool {
	state := conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Route executes a node request with circuit breaker, retry logic, and connection pooling
func (c *EnhancedNodeClient) Route(ctx context.Context, req *nodepb.Request, opts ...grpc.CallOption) (*nodepb.Response, error) {
	var lastErr error
	backoff := c.config.InitialBackoff

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		// Don't sleep on first attempt
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}

			// Increase backoff for next attempt
			backoff = time.Duration(float64(backoff) * c.config.BackoffMultiplier)
			if backoff > c.config.MaxBackoff {
				backoff = c.config.MaxBackoff
			}
		}

		var response *nodepb.Response
		var requestError error

		err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
			conn, err := c.getConnection()
			if err != nil {
				requestError = fmt.Errorf("failed to get connection: %w", err)
				return requestError
			}
			defer c.returnConnection(conn)

			client := nodepb.NewNodeClient(conn)

			// Set request timeout
			reqCtx, cancel := context.WithTimeout(ctx, c.config.RequestTimeout)
			defer cancel()

			resp, err := client.Route(reqCtx, req)
			if err != nil {
				requestError = err
				return err
			}

			response = resp
			return nil
		})

		if err == nil && response != nil {
			return response, nil
		}

		if requestError != nil {
			lastErr = requestError
		} else {
			lastErr = err
		}

		c.logger.Warn("request attempt failed",
			zap.String("address", c.address),
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", c.config.MaxRetries),
			zap.Duration("backoff", backoff),
			zap.Error(lastErr))

		// Check if error is not retryable
		if !c.isRetryableError(lastErr) {
			break
		}
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// isRetryableError determines if an error is retryable
func (c *EnhancedNodeClient) isRetryableError(err error) bool {
	// Circuit breaker errors are not retryable
	if err == circuitbreaker.ErrCircuitBreakerOpen || err == circuitbreaker.ErrCircuitBreakerHalfOpen {
		return false
	}

	// Add more sophisticated error checking here
	// For now, most errors are retryable except context cancellation
	return err != context.Canceled && err != context.DeadlineExceeded
}

// GetStats returns statistics about the client
func (c *EnhancedNodeClient) GetStats() map[string]interface{} {
	c.mu.RLock()
	availableConnections := len(c.connections)
	closed := c.closed
	c.mu.RUnlock()

	stats := map[string]interface{}{
		"address":               c.address,
		"max_connections":       c.config.MaxConnections,
		"available_connections": availableConnections,
		"closed":                closed,
	}

	// Add circuit breaker stats
	cbStats := c.circuitBreaker.GetStats()
	for k, v := range cbStats {
		stats["circuit_breaker_"+k] = v
	}

	return stats
}

// Close closes the client and all connections
func (c *EnhancedNodeClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true
	c.closeAllConnections()

	c.logger.Info("enhanced node client closed",
		zap.String("address", c.address))

	return nil
}

// closeAllConnections closes all connections in the pool
func (c *EnhancedNodeClient) closeAllConnections() {
	close(c.connections)
	for conn := range c.connections {
		conn.Close()
	}
}
