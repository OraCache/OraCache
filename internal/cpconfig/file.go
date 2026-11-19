package cpconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
)

// MaxConfigFileBytes ограничивает размер bootstrap-файла для разработки.
// Конфигурация — это metadata, а не payload кэша.
const MaxConfigFileBytes = 16 << 20

// LoadFile читает JSON snapshot конфигурации, отклоняет неизвестные поля и
// проверяет все инварианты. Формат использует struct-теги Config; роли регионов —
// "master"/"slave".
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
