package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type NodeConfig struct {
	Addr       string `yaml:"addr"`
	SqlitePath string `yaml:"sqlite_path"`
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
