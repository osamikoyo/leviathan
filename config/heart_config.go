package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type HeartConfig struct {
	ExchangeName        string        `yaml:"exchange"`
	QueueName           string        `yaml:"queue_name"`
	RabbitmqUrl         string        `yaml:"rabbitmq_url"`
	Addr                string        `yaml:"addr"`
	Nodes               []string      `yaml:"nodes"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
	HealthCheckTimeout  time.Duration `yaml:"health_check_timeout"`
	MaxRetries          int           `yaml:"max_retries"`
	RequestTimeout      time.Duration `yaml:"request_timeout"`
}

func NewHeartConfig(path string) (*HeartConfig, error) {
	if path == "default" {
		return &HeartConfig{
			Addr:  "localhost:8080",
			Nodes: nil,
		}, nil
	}

	cfg := HeartConfig{}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed open config file: %v", err)
	}

	if err = yaml.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed decode config file: %v", err)
	}

	// Set defaults for optional fields
	cfg.setDefaults()

	// Validate configuration
	if err = cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %v", err)
	}

	return &cfg, nil
}

// setDefaults sets default values for optional configuration fields
func (c *HeartConfig) setDefaults() {
	if c.ExchangeName == "" {
		c.ExchangeName = "leviathan_writes"
	}
	if c.QueueName == "" {
		c.QueueName = "write_requests"
	}
	if c.HealthCheckInterval == 0 {
		c.HealthCheckInterval = 30 * time.Second
	}
	if c.HealthCheckTimeout == 0 {
		c.HealthCheckTimeout = 5 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RequestTimeout == 0 {
		c.RequestTimeout = 30 * time.Second
	}
}

// validate validates the configuration
func (c *HeartConfig) validate() error {
	if c.Addr == "" {
		return fmt.Errorf("addr is required")
	}

	if c.RabbitmqUrl != "" {
		if _, err := url.Parse(c.RabbitmqUrl); err != nil {
			return fmt.Errorf("invalid rabbitmq_url: %v", err)
		}
	}

	for i, node := range c.Nodes {
		if node == "" {
			return fmt.Errorf("node at index %d is empty", i)
		}
		if !strings.Contains(node, ":") {
			return fmt.Errorf("node at index %d missing port: %s", i, node)
		}
	}

	if c.HealthCheckInterval < time.Second {
		return fmt.Errorf("health_check_interval must be at least 1 second")
	}

	if c.HealthCheckTimeout < 100*time.Millisecond {
		return fmt.Errorf("health_check_timeout must be at least 100ms")
	}

	if c.HealthCheckTimeout >= c.HealthCheckInterval {
		return fmt.Errorf("health_check_timeout must be less than health_check_interval")
	}

	if c.MaxRetries < 1 || c.MaxRetries > 10 {
		return fmt.Errorf("max_retries must be between 1 and 10")
	}

	if c.RequestTimeout < time.Second {
		return fmt.Errorf("request_timeout must be at least 1 second")
	}

	return nil
}
