package health

import (
	"context"
	"sync"
	"time"

	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/metrics"
	"github.com/osamikoyo/leviathan/nodepb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NodeHealth represents the health status of a node
type NodeHealth struct {
	Address     string
	IsHealthy   bool
	LastChecked time.Time
	Error       error
}

// HealthChecker manages health checking for nodes
type HealthChecker struct {
	nodes    map[string]nodepb.NodeClient
	statuses map[string]*NodeHealth
	mu       sync.RWMutex
	logger   *logger.Logger
	interval time.Duration
	timeout  time.Duration
	done     chan struct{}
}

// NewHealthChecker creates a new health checker
func NewHealthChecker(
	nodes map[string]nodepb.NodeClient,
	logger *logger.Logger,
	interval time.Duration,
	timeout time.Duration,
) *HealthChecker {
	hc := &HealthChecker{
		nodes:    nodes,
		statuses: make(map[string]*NodeHealth),
		logger:   logger,
		interval: interval,
		timeout:  timeout,
		done:     make(chan struct{}),
	}

	// Initialize statuses
	for addr := range nodes {
		hc.statuses[addr] = &NodeHealth{
			Address:   addr,
			IsHealthy: true, // Start as healthy until proven otherwise
		}
	}

	return hc
}

// Start begins the health checking process
func (hc *HealthChecker) Start(ctx context.Context) {
	hc.logger.Info("starting health checker",
		zap.Duration("interval", hc.interval),
		zap.Duration("timeout", hc.timeout),
		zap.Int("nodes_count", len(hc.nodes)))

	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	// Initial health check
	hc.checkAllNodes(ctx)

	for {
		select {
		case <-ctx.Done():
			hc.logger.Info("health checker stopped")
			return
		case <-hc.done:
			hc.logger.Info("health checker stopped via done channel")
			return
		case <-ticker.C:
			hc.checkAllNodes(ctx)
		}
	}
}

// Stop stops the health checker

// checkAllNodes checks the health of all nodes
func (hc *HealthChecker) checkAllNodes(ctx context.Context) {
	var wg sync.WaitGroup

	for addr, client := range hc.nodes {
		wg.Add(1)
		go func(address string, nodeClient nodepb.NodeClient) {
			defer wg.Done()
			hc.checkNodeHealth(ctx, address, nodeClient)
		}(addr, client)
	}

	wg.Wait()
	hc.updateMetrics()
}

// checkNodeHealth checks the health of a single node
func (hc *HealthChecker) checkNodeHealth(ctx context.Context, address string, client nodepb.NodeClient) {
	checkCtx, cancel := context.WithTimeout(ctx, hc.timeout)
	defer cancel()

	startTime := time.Now()

	// Simple ping/health check - try to make a basic request
	// You might want to implement a specific health check endpoint
	_, err := client.Route(checkCtx, &nodepb.Request{
		Sql: "SELECT 1", // Simple health check query
	})

	duration := time.Since(startTime)
	isHealthy := err == nil || status.Code(err) != codes.Unavailable

	hc.mu.Lock()
	defer hc.mu.Unlock()

	if status, exists := hc.statuses[address]; exists {
		previousHealth := status.IsHealthy
		status.IsHealthy = isHealthy
		status.LastChecked = time.Now()
		status.Error = err

		// Log health status changes
		if previousHealth != isHealthy {
			if isHealthy {
				hc.logger.Info("node became healthy",
					zap.String("address", address),
					zap.Duration("check_duration", duration))
			} else {
				hc.logger.Warn("node became unhealthy",
					zap.String("address", address),
					zap.Error(err),
					zap.Duration("check_duration", duration))
			}
		}

		hc.logger.Debug("node health check completed",
			zap.String("address", address),
			zap.Bool("is_healthy", isHealthy),
			zap.Duration("check_duration", duration))
	}
}

// updateMetrics updates Prometheus metrics with current health statuses
func (hc *HealthChecker) updateMetrics() {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	for addr, status := range hc.statuses {
		healthValue := 0.0
		if status.IsHealthy {
			healthValue = 1.0
		}
		metrics.NodeHealthStatus.WithLabelValues(addr).Set(healthValue)
	}
}

// GetHealthyNodes returns a list of healthy node addresses
func (hc *HealthChecker) GetHealthyNodes() []string {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	var healthyNodes []string
	for addr, status := range hc.statuses {
		if status.IsHealthy {
			healthyNodes = append(healthyNodes, addr)
		}
	}

	return healthyNodes
}

// IsNodeHealthy returns true if the specified node is healthy
func (hc *HealthChecker) IsNodeHealthy(address string) bool {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	if status, exists := hc.statuses[address]; exists {
		return status.IsHealthy
	}
	return false
}

// GetNodeStatus returns the health status of a specific node
func (hc *HealthChecker) GetNodeStatus(address string) (*NodeHealth, bool) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	status, exists := hc.statuses[address]
	if !exists {
		return nil, false
	}

	// Return a copy to avoid race conditions
	return &NodeHealth{
		Address:     status.Address,
		IsHealthy:   status.IsHealthy,
		LastChecked: status.LastChecked,
		Error:       status.Error,
	}, true
}

// GetAllStatuses returns health statuses for all nodes
func (hc *HealthChecker) GetAllStatuses() map[string]*NodeHealth {
	hc.mu.RLock()
	defer hc.mu.RUnlock()

	result := make(map[string]*NodeHealth)
	for addr, status := range hc.statuses {
		result[addr] = &NodeHealth{
			Address:     status.Address,
			IsHealthy:   status.IsHealthy,
			LastChecked: status.LastChecked,
			Error:       status.Error,
		}
	}

	return result
}
