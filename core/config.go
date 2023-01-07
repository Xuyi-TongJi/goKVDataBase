package core

import (
	"encoding/json"
	"io"
	"os"
)

type Config struct {
	Port           int   `json:"port"`
	MaxConnection  int32 `json:"maxConnection"`
	MaxQueryLength int32 `json:"maxQueryLength"`
}

const (
	DefaultMaxConnection int32 = 1024
	DefaultPort          int   = 6379
	MaxMaxConnection     int32 = 4096
	MaxMaxQueryLength    int32 = 4096
)

// LoadConfig
// if read from path errors, load default config
func LoadConfig(path string) *Config {
	config, err := loadConfigFile(path)
	if err != nil {
		return &Config{
			Port:           DefaultPort,
			MaxConnection:  DefaultMaxConnection,
			MaxQueryLength: MaxMaxQueryLength,
		}
	}
	if config.MaxConnection > MaxMaxConnection {
		config.MaxConnection = MaxMaxConnection
	}
	if config.MaxQueryLength > MaxMaxQueryLength {
		config.MaxQueryLength = MaxMaxQueryLength
	}
	return config
}

func loadConfigFile(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// read
	buffer, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err = json.Unmarshal(buffer, config); err != nil {
		return nil, err
	}
	return config, nil
}
