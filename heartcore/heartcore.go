package heartcore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/bytedance/sonic"
	"github.com/osamikoyo/leviathan/config"
	"github.com/osamikoyo/leviathan/health"
	"github.com/osamikoyo/leviathan/logger"
	"github.com/osamikoyo/leviathan/metrics"
	"github.com/osamikoyo/leviathan/models"
	"github.com/osamikoyo/leviathan/nodeclient"
	"github.com/osamikoyo/leviathan/nodepb"
	"github.com/osamikoyo/leviathan/producer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

var (
	ErrEmptySQL         = errors.New("sql query cannot be empty")
	ErrNoNodesAvailable = errors.New("no nodes available for routing")
	ErrNilResponse      = errors.New("received nil response from node")
	ErrNilArgument      = errors.New("argument cannot be nil")
	ErrUnsupportedType  = errors.New("unsupported argument type")
)

type EnhancedNode struct {
	client         *nodeclient.EnhancedNodeClient
	address        string
	requestInRoute int
	lastUsed       time.Time
}

func newEnhancedNode(address string, client *nodeclient.EnhancedNodeClient) *EnhancedNode {
	return &EnhancedNode{
		client:   client,
		address:  address,
		lastUsed: time.Now(),
	}
}

type HeartCore struct {
	mux           *sync.RWMutex
	nodes         []*EnhancedNode
	health        *health.HealthChecker
	producer      *producer.Producer
	logger        *logger.Logger
	cfg           *config.HeartConfig
	healthStarted bool
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewHeartCore(
	logger *logger.Logger,
	cfg *config.HeartConfig,
	producer *producer.Producer,
) (*HeartCore, error) {
	if cfg == nil {
		return nil, errors.New("config cannot be nil")
	}
	if producer == nil {
		return nil, errors.New("producer cannot be nil")
	}
	if len(cfg.Nodes) == 0 {
		return nil, ErrNoNodesAvailable
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create enhanced node clients
	nodes := make([]*EnhancedNode, 0, len(cfg.Nodes))
	nodeClientMap := make(map[string]nodepb.NodeClient)

	for _, address := range cfg.Nodes {
		config := nodeclient.DefaultConfig(address)
		// Override config with heart service settings
		config.RequestTimeout = cfg.RequestTimeout
		config.MaxRetries = cfg.MaxRetries

		client, err := nodeclient.NewEnhancedNodeClient(config, logger)
		if err != nil {
			logger.Warn("failed to create enhanced node client, skipping node",
				zap.String("address", address),
				zap.Error(err))
			continue
		}

		nodes = append(nodes, newEnhancedNode(address, client))
		nodeClientMap[address] = client
	}

	if len(nodes) == 0 {
		cancel()
		return nil, ErrNoNodesAvailable
	}

	// Create health checker
	healthChecker := health.NewHealthChecker(
		nodeClientMap,
		logger,
		cfg.HealthCheckInterval,
		cfg.HealthCheckTimeout,
	)

	return &HeartCore{
		mux:      &sync.RWMutex{},
		nodes:    nodes,
		health:   healthChecker,
		logger:   logger,
		cfg:      cfg,
		producer: producer,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

// Start starts the health checker
func (h *HeartCore) Start(ctx context.Context) {
	h.mux.Lock()
	defer h.mux.Unlock()

	if h.healthStarted {
		return
	}

	h.healthStarted = true
	go h.health.Start(ctx)

	h.logger.Info("heart core started with health checking",
		zap.Int("node_count", len(h.nodes)))
}

// Stop stops the heart core and all node clients
func (h *HeartCore) Stop() error {
	h.mux.Lock()
	defer h.mux.Unlock()

	if h.cancel != nil {
		h.cancel()
	}

	h.health.Stop()

	// Close all node clients
	for _, node := range h.nodes {
		if err := node.client.Close(); err != nil {
			h.logger.Warn("failed to close node client",
				zap.String("address", node.address),
				zap.Error(err))
		}
	}

	h.logger.Info("heart core stopped")
	return nil
}

func (h *HeartCore) getGoodNode() (int, error) {
	h.mux.RLock()
	defer h.mux.RUnlock()

	if len(h.nodes) == 0 {
		return -1, ErrNoNodesAvailable
	}

	// First, filter healthy nodes
	healthyNodes := make([]int, 0)
	for i, node := range h.nodes {
		if h.health.IsNodeHealthy(node.address) {
			healthyNodes = append(healthyNodes, i)
		}
	}

	// If no healthy nodes, fall back to all nodes (circuit breakers will handle failures)
	candidates := healthyNodes
	if len(candidates) == 0 {
		h.logger.Warn("no healthy nodes available, falling back to all nodes")
		candidates = make([]int, len(h.nodes))
		for i := range h.nodes {
			candidates[i] = i
		}
	}

	// Find node with minimum load among candidates
	minindex := candidates[0]
	m := h.nodes[minindex].requestInRoute

	for _, i := range candidates {
		node := h.nodes[i]
		if node.requestInRoute < m {
			m = node.requestInRoute
			minindex = i
		}
	}

	h.logger.Debug("selected node for routing",
		zap.Int("node_index", minindex),
		zap.String("node_address", h.nodes[minindex].address),
		zap.Int("requests_in_route", m),
		zap.Int("healthy_nodes_count", len(healthyNodes)),
		zap.Bool("is_healthy", h.health.IsNodeHealthy(h.nodes[minindex].address)))

	return minindex, nil
}

// GetHealthyNodeCount returns the number of currently healthy nodes
func (h *HeartCore) GetHealthyNodeCount() int {
	h.mux.RLock()
	defer h.mux.RUnlock()

	count := 0
	for _, node := range h.nodes {
		if h.health.IsNodeHealthy(node.address) {
			count++
		}
	}
	return count
}

// GetNodeStats returns statistics about all nodes
func (h *HeartCore) GetNodeStats() map[string]interface{} {
	h.mux.RLock()
	defer h.mux.RUnlock()

	stats := make(map[string]interface{})
	healthyCount := 0
	totalRequests := 0

	for i, node := range h.nodes {
		isHealthy := h.health.IsNodeHealthy(node.address)
		if isHealthy {
			healthyCount++
		}
		totalRequests += node.requestInRoute

		// Add per-node stats
		nodeStats := map[string]interface{}{
			"address":           node.address,
			"healthy":           isHealthy,
			"requests_in_route": node.requestInRoute,
			"last_used":         node.lastUsed,
		}

		// Add circuit breaker stats if available
		if clientStats := node.client.GetStats(); clientStats != nil {
			nodeStats["client_stats"] = clientStats
		}

		stats[fmt.Sprintf("node_%d", i)] = nodeStats
	}

	stats["summary"] = map[string]interface{}{
		"total_nodes":    len(h.nodes),
		"healthy_nodes":  healthyCount,
		"total_requests": totalRequests,
	}

	return stats
}

func packAny(value any) (*anypb.Any, error) {
	if value == nil {
		return nil, ErrNilArgument
	}

	switch v := value.(type) {
	case string:
		return anypb.New(wrapperspb.String(v))
	case bool:
		return anypb.New(wrapperspb.Bool(v))
	case int:
		return anypb.New(wrapperspb.Int64(int64(v)))
	case int32:
		return anypb.New(wrapperspb.Int32(v))
	case int64:
		return anypb.New(wrapperspb.Int64(v))
	case uint32:
		return anypb.New(wrapperspb.UInt32(v))
	case uint64:
		return anypb.New(wrapperspb.UInt64(v))
	case float32:
		return anypb.New(wrapperspb.Float(v))
	case float64:
		return anypb.New(wrapperspb.Double(v))
	case []byte:
		return anypb.New(wrapperspb.Bytes(v))
	default:
		return nil, fmt.Errorf("%w: %v", ErrUnsupportedType, reflect.TypeOf(value))
	}
}

func (h *HeartCore) RouteReadRequest(ctx context.Context, sql string, args ...any) ([]map[string]interface{}, error) {
	metrics.RequestCount.WithLabelValues("read").Inc()

	then := time.Now()

	if len(sql) == 0 {
		return nil, ErrEmptySQL
	}

	h.logger.Debug("routing read request",
		zap.String("sql", sql),
		zap.Int("args_count", len(args)))

	// Pack arguments into protobuf Any types
	argsPb := make([]*anypb.Any, len(args))
	for i, arg := range args {
		argPb, err := packAny(arg)
		if err != nil {
			h.logger.Error("failed to pack argument",
				zap.Int("arg_index", i),
				zap.Any("arg_value", arg),
				zap.Error(err))
			return nil, fmt.Errorf("failed to pack argument at index %d: %w", i, err)
		}
		argsPb[i] = argPb
	}

	// Select best node for routing
	index, err := h.getGoodNode()
	if err != nil {
		h.logger.Error("failed to select node for routing", zap.Error(err))
		return nil, fmt.Errorf("failed to select node: %w", err)
	}

	// Increment request counter atomically
	h.incrementNodeCounter(index)
	// Ensure counter is decremented on function exit
	defer h.decrementNodeCounter(index)

	// Make gRPC call to selected node
	resp, err := h.nodes[index].client.Route(ctx, &nodepb.Request{
		Sql:  sql,
		Args: argsPb,
	})

	if err != nil {
		h.logger.Error("node request failed",
			zap.Int("node_index", index),
			zap.String("sql", sql),
			zap.Error(err))
		return nil, fmt.Errorf("node request failed: %w", err)
	}

	if resp == nil {
		h.logger.Error("received nil response from node",
			zap.Int("node_index", index))
		return nil, ErrNilResponse
	}

	// Unmarshal response payload
	var result []map[string]interface{}
	if err = sonic.Unmarshal([]byte(resp.Payload), &result); err != nil {
		h.logger.Error("failed to unmarshal response payload",
			zap.Int("node_index", index),
			zap.String("payload", resp.Payload),
			zap.Error(err))
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	h.logger.Info("read request completed successfully",
		zap.Int("node_index", index),
		zap.Int("result_count", len(result)))

	metrics.RequestDuration.WithLabelValues("read").Observe(float64(time.Since(then)))

	return result, nil
}

// incrementNodeCounter safely increments the request counter for a node
func (h *HeartCore) incrementNodeCounter(index int) {
	h.mux.Lock()
	defer h.mux.Unlock()
	if index >= 0 && index < len(h.nodes) {
		h.nodes[index].requestInRoute++
	}
}

// decrementNodeCounter safely decrements the request counter for a node
func (h *HeartCore) decrementNodeCounter(index int) {
	h.mux.Lock()
	defer h.mux.Unlock()
	if index >= 0 && index < len(h.nodes) && h.nodes[index].requestInRoute > 0 {
		h.nodes[index].requestInRoute--
	}
}

func (h *HeartCore) RouteWriteRequest(ctx context.Context, sql string, args ...any) error {
	metrics.RequestCount.WithLabelValues("write").Inc()

	then := time.Now()

	if len(sql) == 0 {
		return ErrEmptySQL
	}

	h.logger.Debug("routing write request",
		zap.String("sql", sql),
		zap.Int("args_count", len(args)))

	// Select node for load balancing (even for async writes)
	index, err := h.getGoodNode()
	if err != nil {
		h.logger.Error("failed to select node for load balancing", zap.Error(err))
		return fmt.Errorf("failed to select node: %w", err)
	}

	// Increment request counter for load balancing
	h.incrementNodeCounter(index)
	// Ensure counter is decremented on function exit
	defer h.decrementNodeCounter(index)

	// Publish write request to queue
	err = h.producer.Publish(ctx, models.NewRequest(sql, args, true))
	if err != nil {
		h.logger.Error("failed to publish write request",
			zap.String("sql", sql),
			zap.Any("args", args),
			zap.Error(err))
		return fmt.Errorf("failed to publish write request: %w", err)
	}

	h.logger.Info("write request published successfully",
		zap.String("sql", sql))

	metrics.RequestDuration.WithLabelValues("write").Observe(float64(time.Since(then)))

	return nil
}
