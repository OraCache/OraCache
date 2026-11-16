package cpconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// MaxConfigFileBytes bounds the development bootstrap file. Configurations are
// metadata, not cache payload.
const MaxConfigFileBytes = 16 << 20

// LoadFile reads a JSON configuration snapshot, rejects unknown fields and
// validates all invariants. The format uses the struct tags of Config; region
// roles are "master"/"slave".
func LoadFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	if len(data) > MaxConfigFileBytes {
		return nil, fmt.Errorf("config %q exceeds %d bytes", path, MaxConfigFileBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config %q: %w", path, err)
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode config %q: trailing JSON value", path)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config %q: %w", path, err)
	}
	return &cfg, nil
}
