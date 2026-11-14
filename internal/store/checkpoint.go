package store

import (
	"container/list"
	"encoding/json"
	"fmt"
)

const checkpointFormat = 1

type checkpoint struct {
	Format int    `json:"format"`
	Config Config `json:"config"`
	// Seq is the global revision counter; it also covers deleted revisions.
	Seq uint64 `json:"seq"`
	// Keys are ordered from most to least recently written (LRU front to back).
	Keys []checkpointKey `json:"keys"`
}

type checkpointKey struct {
	Key      string              `json:"key"`
	Versions []checkpointVersion `json:"versions"`
}

type checkpointVersion struct {
	Rev  uint64 `json:"rev"`
	Data []byte `json:"data"`
}

// Checkpoint serializes one consistent in-memory state: configuration, the
// global revision sequence, every retained version and the LRU order.
func (s *Store) Checkpoint() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := checkpoint{Format: checkpointFormat, Config: s.cfg, Seq: s.seq, Keys: make([]checkpointKey, 0, len(s.entries))}
	for el := s.lru.Front(); el != nil; el = el.Next() {
		e := el.Value.(*entry)
		k := checkpointKey{Key: e.key, Versions: make([]checkpointVersion, len(e.versions))}
		for i, v := range e.versions {
			k.Versions[i] = checkpointVersion{Rev: v.Rev, Data: v.Data}
		}
		c.Keys = append(c.Keys, k)
	}
	return json.Marshal(c)
}

// Restore validates data completely and then replaces the whole state. On any
// error the current state is untouched. The checkpoint must have been taken
// with the same Config, otherwise eviction would no longer be deterministic.
func (s *Store) Restore(data []byte) error {
	var c checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return fmt.Errorf("store: decode checkpoint: %w", err)
	}
	if c.Format != checkpointFormat {
		return fmt.Errorf("store: unsupported checkpoint format %d", c.Format)
	}
	if c.Config != s.cfg {
		return fmt.Errorf("store: checkpoint config %+v differs from store config %+v", c.Config, s.cfg)
	}
	entries := make(map[string]*entry, len(c.Keys))
	lru := list.New()
	var used int64
	for _, k := range c.Keys {
		if k.Key == "" || entries[k.Key] != nil {
			return fmt.Errorf("store: checkpoint has an empty or duplicate key")
		}
		if n := len(k.Versions); n == 0 || n > s.cfg.MaxVersions {
			return fmt.Errorf("store: checkpoint key %q has %d versions", k.Key, n)
		}
		e := &entry{key: k.Key}
		var prev uint64
		for _, v := range k.Versions {
			if v.Rev <= prev || v.Rev > c.Seq || len(v.Data) > s.cfg.MaxValueBytes {
				return fmt.Errorf("store: checkpoint key %q has an invalid version", k.Key)
			}
			prev = v.Rev
			e.versions = append(e.versions, Version{Rev: v.Rev, Data: clone(v.Data)})
		}
		e.size = e.computeSize()
		used += e.size
		e.elem = lru.PushBack(e)
		entries[k.Key] = e
	}
	if used > s.cfg.MaxBytes && !(len(entries) == 1 && len(c.Keys[0].Versions) == 1) {
		return fmt.Errorf("store: checkpoint uses %d bytes, over the %d limit", used, s.cfg.MaxBytes)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq, s.entries, s.lru, s.used = c.Seq, entries, lru, used
	return nil
}
