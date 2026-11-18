package store

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"sync"
)

const (
	DefaultMaxVersions   = 3
	DefaultMaxBytes      = 256 << 20
	DefaultMaxValueBytes = 10 << 10
)

const (
	keyOverhead     = 64
	versionOverhead = 32
)

var ErrRevisionsExhausted = errors.New("store: revision sequence exhausted")

type Config struct {
	MaxVersions   int
	MaxBytes      int64
	MaxValueBytes int
}

func (c Config) withDefaults() Config {
	if c.MaxVersions == 0 {
		c.MaxVersions = DefaultMaxVersions
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = DefaultMaxBytes
	}
	if c.MaxValueBytes == 0 {
		c.MaxValueBytes = DefaultMaxValueBytes
	}
	return c
}

type Version struct {
	Rev  uint64
	Data []byte
}

type GetStatus int

const (
	GetMiss GetStatus = iota
	GetHit
	GetVersionNotFound
)

type GetResult struct {
	Status    GetStatus
	Version   Version
	LatestRev uint64
}

type PutStatus int

const (
	PutOK PutStatus = iota
	PutConflict
	PutTooLarge
)

type PutResult struct {
	Status PutStatus
	Rev    uint64
}

type Stats struct {
	Keys  int
	Bytes int64
}

type entry struct {
	key      string
	versions []Version
	size     int64
	elem     *list.Element
}

func (e *entry) latest() uint64 { return e.versions[len(e.versions)-1].Rev }

func (e *entry) computeSize() int64 {
	size := int64(keyOverhead + len(e.key))
	for _, v := range e.versions {
		size += int64(versionOverhead + len(v.Data))
	}
	return size
}

type Store struct {
	mu      sync.Mutex
	cfg     Config
	seq     uint64
	entries map[string]*entry
	lru     *list.List
	used    int64
}

func New(cfg Config) (*Store, error) {
	cfg = cfg.withDefaults()
	switch {
	case cfg.MaxVersions < 1:
		return nil, fmt.Errorf("store: MaxVersions must be >= 1, got %d", cfg.MaxVersions)
	case cfg.MaxValueBytes < 1:
		return nil, fmt.Errorf("store: MaxValueBytes must be >= 1, got %d", cfg.MaxValueBytes)
	case cfg.MaxBytes < int64(keyOverhead+versionOverhead+cfg.MaxValueBytes):

		return nil, fmt.Errorf("store: MaxBytes %d is too small for a %d-byte value", cfg.MaxBytes, cfg.MaxValueBytes)
	}
	return &Store{
		cfg:     cfg,
		entries: make(map[string]*entry),
		lru:     list.New(),
	}, nil
}

func (s *Store) Config() Config { return s.cfg }

// Get returns a version and promotes the key in the LRU order.
func (s *Store) Get(key string, rev uint64) GetResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookup(key, rev, true)
}

// Peek is Get without the LRU promotion. Replicated state machines use it so
// that reads, which are served by one replica only, never change eviction order.
func (s *Store) Peek(key string, rev uint64) GetResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookup(key, rev, false)
}

func (s *Store) lookup(key string, rev uint64, touch bool) GetResult {
	e := s.entries[key]
	if e == nil {
		return GetResult{Status: GetMiss}
	}
	if touch {
		s.lru.MoveToFront(e.elem)
	}
	latest := e.latest()
	if rev == 0 {
		rev = latest
	}
	for _, v := range e.versions {
		if v.Rev == rev {
			return GetResult{Status: GetHit, Version: Version{Rev: v.Rev, Data: clone(v.Data)}, LatestRev: latest}
		}
	}
	return GetResult{Status: GetVersionNotFound, LatestRev: latest}
}

func (s *Store) Put(key string, data []byte, expectedRev *uint64) (PutResult, error) {
	if len(data) > s.cfg.MaxValueBytes {
		return PutResult{Status: PutTooLarge}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.entries[key]
	var latest uint64
	if e != nil {
		latest = e.latest()
	}
	if expectedRev != nil && *expectedRev != latest {
		return PutResult{Status: PutConflict, Rev: latest}, nil
	}
	if s.seq == math.MaxUint64 {
		return PutResult{}, ErrRevisionsExhausted
	}
	s.seq++
	rev := s.seq

	if e == nil {
		e = &entry{key: key}
		e.elem = s.lru.PushFront(e)
		s.entries[key] = e
	} else {
		s.lru.MoveToFront(e.elem)
	}
	e.versions = append(e.versions, Version{Rev: rev, Data: clone(data)})
	if extra := len(e.versions) - s.cfg.MaxVersions; extra > 0 {
		e.versions = append([]Version(nil), e.versions[extra:]...)
	}
	s.resize(e)
	s.evict(e)
	return PutResult{Status: PutOK, Rev: rev}, nil
}

func (s *Store) Delete(key string, rev uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.entries[key]
	if e == nil {
		return false
	}
	if rev == 0 {
		s.remove(e)
		return true
	}
	for i, v := range e.versions {
		if v.Rev == rev {
			e.versions = append(e.versions[:i:i], e.versions[i+1:]...)
			if len(e.versions) == 0 {
				s.remove(e)
			} else {
				s.resize(e)
			}
			return true
		}
	}
	return false
}

func (s *Store) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Keys: len(s.entries), Bytes: s.used}
}

func (s *Store) resize(e *entry) {
	size := e.computeSize()
	s.used += size - e.size
	e.size = size
}

func (s *Store) remove(e *entry) {
	s.used -= e.size
	s.lru.Remove(e.elem)
	delete(s.entries, e.key)
}

func (s *Store) evict(keep *entry) {
	for s.used > s.cfg.MaxBytes {
		oldest := s.lru.Back().Value.(*entry)
		if oldest != keep {
			s.remove(oldest)
			continue
		}
		if len(keep.versions) == 1 {
			return
		}
		keep.versions = append([]Version(nil), keep.versions[1:]...)
		s.resize(keep)
	}
}

func clone(b []byte) []byte { return append([]byte(nil), b...) }
