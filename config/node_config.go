package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	NodeID       uint32 `yaml:"node_id"`
	Addr         string `yaml:"addr"`
	ExchangeName string `yaml:"exchange_name"`
	RabbitmqURL  string `yaml:"rabbitmq_url"`
	QueueName    string `yaml:"queue_name"`
	SqlitePath   string `yaml:"sqlite_path"`
}

func NewNodeConfig(path string) (*NodeConfig, error) {
	cfg := NodeConfig{}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed open config file: %v", err)
	}

	if err = yaml.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed decode config: %v", err)
	}

	return &cfg, nil
}
