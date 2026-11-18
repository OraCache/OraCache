package cache

import (
	"encoding/json"
	"fmt"
)

type checkpoint struct {
	Format int                        `json:"format"`
	Keys   map[string]checkpointEntry `json:"keys"`
}

type checkpointEntry struct {
	Sequence uint64            `json:"sequence"`
	Versions map[uint64][]byte `json:"versions"`
}

// Checkpoint includes counters of deleted keys, so restore never reuses revisions.
func (s *Server) Checkpoint() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := checkpoint{Format: 1, Keys: make(map[string]checkpointEntry, len(s.keys))}
	for k, e := range s.keys {
		c.Keys[k] = checkpointEntry{Sequence: e.sequence, Versions: e.versions}
	}
	return json.Marshal(c)
}

// Restore validates into temporary state and replaces the entire cache atomically.
func (s *Server) Restore(data []byte) error {
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("decode checkpoint: %w", err)
	}
	if c.Format != 1 || c.Keys == nil {
		return fmt.Errorf("unsupported or incomplete checkpoint")
	}
	keys := make(map[string]*entry, len(c.Keys))
	for k, e := range c.Keys {
		if k == "" || e.Sequence == 0 {
			return fmt.Errorf("invalid checkpoint key or sequence")
		}
		for rev, value := range e.Versions {
			if rev == 0 || rev > e.Sequence || len(value) > MaxPayload {
				return fmt.Errorf("invalid checkpoint revision or payload")
			}
		}
		if e.Versions == nil {
			e.Versions = make(map[uint64][]byte)
		}
		keys[k] = &entry{sequence: e.Sequence, versions: e.Versions}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys = keys
	return nil
}
