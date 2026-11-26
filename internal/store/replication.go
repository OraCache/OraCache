package store

import "fmt"

func (s *Store) Versions(key string) []Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Version
	if e := s.entries[key]; e != nil {
		for _, v := range e.versions {
			out = append(out, Version{Rev: v.Rev, Data: clone(v.Data)})
		}
	}
	return out
}

func (s *Store) ReplaceVersions(key string, versions []Version) error {
	if key == "" || len(versions) > s.cfg.MaxVersions {
		return fmt.Errorf("store: invalid replacement key or version count")
	}
	var prev uint64
	e := &entry{key: key}
	for _, v := range versions {
		if v.Rev <= prev || len(v.Data) > s.cfg.MaxValueBytes {
			return fmt.Errorf("store: invalid replacement version")
		}
		prev = v.Rev
		e.versions = append(e.versions, Version{Rev: v.Rev, Data: clone(v.Data)})
	}
	if len(versions) > 1 && e.computeSize() > s.cfg.MaxBytes {
		return fmt.Errorf("store: replacement exceeds memory budget")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old := s.entries[key]; old != nil {
		s.remove(old)
	}
	if len(versions) == 0 {
		return nil
	}
	e.elem = s.lru.PushFront(e)
	s.entries[key] = e
	s.resize(e)
	s.evict(e)
	if prev > s.seq {
		s.seq = prev
	}
	return nil
}
