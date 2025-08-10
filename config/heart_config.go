package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type HeartConfig struct {
	Addr  string   `yaml:"addr"`
	Nodes []string `yaml:"nodes"`
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

	return &cfg, nil
}
