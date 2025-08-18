package degradation

import (
	"context"
	"sync"
	"time"

	"github.com/osamikoyo/leviathan/errors"
	"github.com/osamikoyo/leviathan/logger"
	"go.uber.org/zap"
)

// DegradationLevel represents the level of service degradation
type DegradationLevel int

const (
	// LevelNormal - full service availability
	LevelNormal DegradationLevel = iota
	// LevelPartial - some features disabled, core functionality available
	LevelPartial
	// LevelReadOnly - only read operations allowed
	LevelReadOnly
	// LevelMinimal - only basic health checks and status endpoints
	LevelMinimal
	// LevelUnavailable - service is completely unavailable
	LevelUnavailable
)

func (dl DegradationLevel) String() string {
	switch dl {
	case LevelNormal:
		return "normal"
	case LevelPartial:
		return "partial"
	case LevelReadOnly:
		return "read_only"
	case LevelMinimal:
		return "minimal"
	case LevelUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

// DegradationStrategy defines how to handle different degradation scenarios
type DegradationStrategy struct {
	// AllowWrites determines if write operations are allowed
	AllowWrites bool
	// AllowReads determines if read operations are allowed
	AllowReads bool
	// FallbackToCache determines if cached responses should be used
	FallbackToCache bool
	// CacheMaxAge is the maximum age for cached responses
	CacheMaxAge time.Duration
	// ReturnStaleData determines if stale data is acceptable
	ReturnStaleData bool
	// StaleDataMaxAge is the maximum age for stale data
	StaleDataMaxAge time.Duration
	// ErrorMessage is the message to return when operation is not allowed
	ErrorMessage string
}

// DefaultStrategies provides default degradation strategies for each level
var DefaultStrategies = map[DegradationLevel]DegradationStrategy{
	LevelNormal: {
		AllowWrites:     true,
		AllowReads:      true,
		FallbackToCache: false,
		ReturnStaleData: false,
		ErrorMessage:    "",
	},
	LevelPartial: {
		AllowWrites:     true,
		AllowReads:      true,
		FallbackToCache: true,
		CacheMaxAge:     5 * time.Minute,
		ReturnStaleData: true,
		StaleDataMaxAge: 10 * time.Minute,
		ErrorMessage:    "Service is running in partial mode",
	},
	LevelReadOnly: {
		AllowWrites:     false,
		AllowReads:      true,
		FallbackToCache: true,
		CacheMaxAge:     15 * time.Minute,
		ReturnStaleData: true,
		StaleDataMaxAge: 30 * time.Minute,
		ErrorMessage:    "Service is in read-only mode",
	},
	LevelMinimal: {
		AllowWrites:     false,
		AllowReads:      false,
		FallbackToCache: true,
		CacheMaxAge:     30 * time.Minute,
		ReturnStaleData: true,
		StaleDataMaxAge: 60 * time.Minute,
		ErrorMessage:    "Service is in minimal mode",
	},
	LevelUnavailable: {
		AllowWrites:     false,
		AllowReads:      false,
		FallbackToCache: false,
		ReturnStaleData: false,
		ErrorMessage:    "Service is currently unavailable",
	},
}

// HealthMetric represents a health metric for degradation decision making
type HealthMetric struct {
	Name      string
	Value     float64
	Threshold float64
	Critical  bool
	LastCheck time.Time
}

// IsHealthy returns true if the metric is within healthy bounds
func (hm *HealthMetric) IsHealthy() bool {
	return hm.Value <= hm.Threshold
}

// DegradationManager manages service degradation based on health metrics
type DegradationManager struct {
	mu               sync.RWMutex
	currentLevel     DegradationLevel
	strategies       map[DegradationLevel]DegradationStrategy
	metrics          map[string]*HealthMetric
	logger           *logger.Logger
	checkInterval    time.Duration
	lastCheck        time.Time
	degradationRules []DegradationRule
	forceLevel       *DegradationLevel // For manual override
	ctx              context.Context
	cancel           context.CancelFunc
}

// DegradationRule defines conditions for triggering degradation
type DegradationRule struct {
	Name        string
	Level       DegradationLevel
	Condition   func(metrics map[string]*HealthMetric) bool
	Priority    int // Higher priority rules are evaluated first
	Description string
}

// NewDegradationManager creates a new degradation manager
func NewDegradationManager(logger *logger.Logger, checkInterval time.Duration) *DegradationManager {
	ctx, cancel := context.WithCancel(context.Background())

	dm := &DegradationManager{
		currentLevel:     LevelNormal,
		strategies:       make(map[DegradationLevel]DegradationStrategy),
		metrics:          make(map[string]*HealthMetric),
		logger:           logger,
		checkInterval:    checkInterval,
		lastCheck:        time.Now(),
		degradationRules: make([]DegradationRule, 0),
		ctx:              ctx,
		cancel:           cancel,
	}

	// Set default strategies
	for level, strategy := range DefaultStrategies {
		dm.strategies[level] = strategy
	}

	// Add default rules
	dm.addDefaultRules()

	return dm
}

// addDefaultRules adds default degradation rules
func (dm *DegradationManager) addDefaultRules() {
	// Critical system failures
	dm.AddRule(DegradationRule{
		Name:        "critical_failures",
		Level:       LevelUnavailable,
		Priority:    100,
		Description: "Critical system failures detected",
		Condition: func(metrics map[string]*HealthMetric) bool {
			if metric, exists := metrics["critical_errors"]; exists {
				return metric.Critical && !metric.IsHealthy()
			}
			return false
		},
	})

	// No healthy nodes available
	dm.AddRule(DegradationRule{
		Name:        "no_healthy_nodes",
		Level:       LevelReadOnly,
		Priority:    90,
		Description: "No healthy nodes available for writes",
		Condition: func(metrics map[string]*HealthMetric) bool {
			if metric, exists := metrics["healthy_nodes"]; exists {
				return metric.Value == 0
			}
			return false
		},
	})

	// High error rate
	dm.AddRule(DegradationRule{
		Name:        "high_error_rate",
		Level:       LevelPartial,
		Priority:    80,
		Description: "High error rate detected",
		Condition: func(metrics map[string]*HealthMetric) bool {
			if metric, exists := metrics["error_rate"]; exists {
				return !metric.IsHealthy() && metric.Value > 0.1 // 10% error rate
			}
			return false
		},
	})

	// High latency
	dm.AddRule(DegradationRule{
		Name:        "high_latency",
		Level:       LevelPartial,
		Priority:    70,
		Description: "High response latency detected",
		Condition: func(metrics map[string]*HealthMetric) bool {
			if metric, exists := metrics["avg_latency"]; exists {
				return !metric.IsHealthy() && metric.Value > 5000 // 5 seconds
			}
			return false
		},
	})

	// Low node availability
	dm.AddRule(DegradationRule{
		Name:        "low_node_availability",
		Level:       LevelPartial,
		Priority:    60,
		Description: "Low node availability",
		Condition: func(metrics map[string]*HealthMetric) bool {
			if metric, exists := metrics["node_availability"]; exists {
				return metric.Value < 0.5 // Less than 50% nodes available
			}
			return false
		},
	})
}

// AddRule adds a degradation rule
func (dm *DegradationManager) AddRule(rule DegradationRule) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.degradationRules = append(dm.degradationRules, rule)

	// Sort rules by priority (highest first)
	for i := len(dm.degradationRules) - 1; i > 0; i-- {
		if dm.degradationRules[i].Priority > dm.degradationRules[i-1].Priority {
			dm.degradationRules[i], dm.degradationRules[i-1] = dm.degradationRules[i-1], dm.degradationRules[i]
		} else {
			break
		}
	}
}

// UpdateMetric updates a health metric
func (dm *DegradationManager) UpdateMetric(name string, value, threshold float64, critical bool) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.metrics[name] = &HealthMetric{
		Name:      name,
		Value:     value,
		Threshold: threshold,
		Critical:  critical,
		LastCheck: time.Now(),
	}
}

// GetCurrentLevel returns the current degradation level
func (dm *DegradationManager) GetCurrentLevel() DegradationLevel {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if dm.forceLevel != nil {
		return *dm.forceLevel
	}

	return dm.currentLevel
}

// GetStrategy returns the strategy for the current degradation level
func (dm *DegradationManager) GetStrategy() DegradationStrategy {
	level := dm.GetCurrentLevel()

	dm.mu.RLock()
	defer dm.mu.RUnlock()

	if strategy, exists := dm.strategies[level]; exists {
		return strategy
	}

	// Fallback to minimal strategy
	return DefaultStrategies[LevelMinimal]
}

// SetStrategy sets a custom strategy for a degradation level
func (dm *DegradationManager) SetStrategy(level DegradationLevel, strategy DegradationStrategy) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.strategies[level] = strategy
}

// ForceLevel manually sets the degradation level (for maintenance, testing, etc.)
func (dm *DegradationManager) ForceLevel(level DegradationLevel) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.forceLevel = &level
	dm.logger.Warn("degradation level manually forced",
		zap.String("level", level.String()))
}

// ClearForcedLevel removes manual override and returns to automatic detection
func (dm *DegradationManager) ClearForcedLevel() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	dm.forceLevel = nil
	dm.logger.Info("degradation level override cleared")
}

// CheckDegradation evaluates current metrics and updates degradation level
func (dm *DegradationManager) CheckDegradation() {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	if dm.forceLevel != nil {
		// Don't change level if it's manually forced
		return
	}

	oldLevel := dm.currentLevel
	newLevel := dm.evaluateDegradationLevel()

	if newLevel != oldLevel {
		dm.currentLevel = newLevel
		dm.logger.Warn("degradation level changed",
			zap.String("old_level", oldLevel.String()),
			zap.String("new_level", newLevel.String()))

		// Log which rule triggered the change
		for _, rule := range dm.degradationRules {
			if rule.Level == newLevel && rule.Condition(dm.metrics) {
				dm.logger.Info("degradation triggered by rule",
					zap.String("rule_name", rule.Name),
					zap.String("description", rule.Description))
				break
			}
		}
	}

	dm.lastCheck = time.Now()
}

// evaluateDegradationLevel evaluates rules and returns appropriate degradation level
func (dm *DegradationManager) evaluateDegradationLevel() DegradationLevel {
	// Check rules in priority order
	for _, rule := range dm.degradationRules {
		if rule.Condition(dm.metrics) {
			return rule.Level
		}
	}

	// If no rules triggered, return to normal
	return LevelNormal
}

// Start starts the degradation manager monitoring
func (dm *DegradationManager) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(dm.checkInterval)
		defer ticker.Stop()

		dm.logger.Info("degradation manager started",
			zap.Duration("check_interval", dm.checkInterval))

		for {
			select {
			case <-ctx.Done():
				dm.logger.Info("degradation manager stopped")
				return
			case <-dm.ctx.Done():
				dm.logger.Info("degradation manager stopped via internal context")
				return
			case <-ticker.C:
				dm.CheckDegradation()
			}
		}
	}()
}

// Stop stops the degradation manager
func (dm *DegradationManager) Stop() {
	dm.cancel()
}

// CanWrite returns true if write operations are allowed
func (dm *DegradationManager) CanWrite() bool {
	strategy := dm.GetStrategy()
	return strategy.AllowWrites
}

// CanRead returns true if read operations are allowed
func (dm *DegradationManager) CanRead() bool {
	strategy := dm.GetStrategy()
	return strategy.AllowReads
}

// ShouldFallbackToCache returns true if cache fallback is recommended
func (dm *DegradationManager) ShouldFallbackToCache() bool {
	strategy := dm.GetStrategy()
	return strategy.FallbackToCache
}

// GetCacheMaxAge returns the maximum age for cached responses
func (dm *DegradationManager) GetCacheMaxAge() time.Duration {
	strategy := dm.GetStrategy()
	return strategy.CacheMaxAge
}

// CanReturnStaleData returns true if stale data is acceptable
func (dm *DegradationManager) CanReturnStaleData() bool {
	strategy := dm.GetStrategy()
	return strategy.ReturnStaleData
}

// GetStaleDataMaxAge returns the maximum age for stale data
func (dm *DegradationManager) GetStaleDataMaxAge() time.Duration {
	strategy := dm.GetStrategy()
	return strategy.StaleDataMaxAge
}

// CheckWrite returns an error if write operations are not allowed
func (dm *DegradationManager) CheckWrite() error {
	if !dm.CanWrite() {
		strategy := dm.GetStrategy()
		return errors.NewEnhancedError(
			errors.ErrorTypeValidation,
			errors.SeverityHigh,
			strategy.ErrorMessage,
		).WithRetryable(false)
	}
	return nil
}

// CheckRead returns an error if read operations are not allowed
func (dm *DegradationManager) CheckRead() error {
	if !dm.CanRead() {
		strategy := dm.GetStrategy()
		return errors.NewEnhancedError(
			errors.ErrorTypeValidation,
			errors.SeverityHigh,
			strategy.ErrorMessage,
		).WithRetryable(false)
	}
	return nil
}

// GetStatus returns the current status of the degradation manager
func (dm *DegradationManager) GetStatus() map[string]interface{} {
	dm.mu.RLock()
	defer dm.mu.RUnlock()

	status := map[string]interface{}{
		"current_level":  dm.currentLevel.String(),
		"forced_level":   dm.forceLevel != nil,
		"last_check":     dm.lastCheck,
		"check_interval": dm.checkInterval,
		"strategy":       dm.strategies[dm.currentLevel],
	}

	if dm.forceLevel != nil {
		status["forced_level_value"] = dm.forceLevel.String()
	}

	// Add metrics summary
	metrics := make(map[string]interface{})
	for name, metric := range dm.metrics {
		metrics[name] = map[string]interface{}{
			"value":      metric.Value,
			"threshold":  metric.Threshold,
			"healthy":    metric.IsHealthy(),
			"critical":   metric.Critical,
			"last_check": metric.LastCheck,
		}
	}
	status["metrics"] = metrics

	// Add rules summary
	rules := make([]map[string]interface{}, len(dm.degradationRules))
	for i, rule := range dm.degradationRules {
		rules[i] = map[string]interface{}{
			"name":        rule.Name,
			"level":       rule.Level.String(),
			"priority":    rule.Priority,
			"description": rule.Description,
			"triggered":   rule.Condition(dm.metrics),
		}
	}
	status["rules"] = rules

	return status
}

// CacheEntry represents a cached response
type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
	TTL       time.Duration
}

// IsValid returns true if the cache entry is still valid
func (ce *CacheEntry) IsValid() bool {
	return time.Since(ce.Timestamp) < ce.TTL
}

// IsStale returns true if the cache entry is stale but within acceptable limits
func (ce *CacheEntry) IsStale(maxAge time.Duration) bool {
	age := time.Since(ce.Timestamp)
	return age >= ce.TTL && age < maxAge
}

// SimpleCache provides a basic in-memory cache for degradation scenarios
type SimpleCache struct {
	mu      sync.RWMutex
	entries map[string]*CacheEntry
}

// NewSimpleCache creates a new simple cache
func NewSimpleCache() *SimpleCache {
	return &SimpleCache{
		entries: make(map[string]*CacheEntry),
	}
}

// Set stores a value in the cache
func (sc *SimpleCache) Set(key string, value interface{}, ttl time.Duration) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.entries[key] = &CacheEntry{
		Data:      value,
		Timestamp: time.Now(),
		TTL:       ttl,
	}
}

// Get retrieves a value from the cache
func (sc *SimpleCache) Get(key string) (interface{}, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	entry, exists := sc.entries[key]
	if !exists {
		return nil, false
	}

	if entry.IsValid() {
		return entry.Data, true
	}

	return nil, false
}

// GetStale retrieves a value from cache even if stale, within limits
func (sc *SimpleCache) GetStale(key string, maxAge time.Duration) (interface{}, bool, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	entry, exists := sc.entries[key]
	if !exists {
		return nil, false, false
	}

	if entry.IsValid() {
		return entry.Data, true, false // Valid, not stale
	}

	if entry.IsStale(maxAge) {
		return entry.Data, true, true // Available but stale
	}

	return nil, false, false // Too old
}

// Delete removes a value from the cache
func (sc *SimpleCache) Delete(key string) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.entries, key)
}

// Clear removes all entries from the cache
func (sc *SimpleCache) Clear() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.entries = make(map[string]*CacheEntry)
}

// Cleanup removes expired entries from the cache
func (sc *SimpleCache) Cleanup() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	for key, entry := range sc.entries {
		if !entry.IsValid() && !entry.IsStale(24*time.Hour) { // Remove entries older than 24h
			delete(sc.entries, key)
		}
	}
}
