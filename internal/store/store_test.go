package store

import (
	"bytes"
	"fmt"
	"math"
	"sync"
	"testing"
)

func ptr(v uint64) *uint64 { return &v }

func newStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func put(t *testing.T, s *Store, key, data string, expected *uint64) PutResult {
	t.Helper()
	r, err := s.Put(key, []byte(data), expected)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func checkInvariants(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var used int64
	if s.lru.Len() != len(s.entries) {
		t.Fatalf("lru has %d keys, map has %d", s.lru.Len(), len(s.entries))
	}
	for el := s.lru.Front(); el != nil; el = el.Next() {
		e := el.Value.(*entry)
		if s.entries[e.key] != e {
			t.Fatalf("lru key %q is not in map", e.key)
		}
		if len(e.versions) == 0 || len(e.versions) > s.cfg.MaxVersions {
			t.Fatalf("key %q has %d versions", e.key, len(e.versions))
		}
		for i := 1; i < len(e.versions); i++ {
			if e.versions[i-1].Rev >= e.versions[i].Rev {
				t.Fatalf("key %q versions out of order", e.key)
			}
		}
		if e.size != e.computeSize() {
			t.Fatalf("key %q size %d, want %d", e.key, e.size, e.computeSize())
		}
		used += e.size
	}
	if used != s.used {
		t.Fatalf("used %d, want %d", s.used, used)
	}
	if s.used > s.cfg.MaxBytes {
		t.Fatalf("used %d exceeds MaxBytes %d", s.used, s.cfg.MaxBytes)
	}
}

func TestGetPutDelete(t *testing.T) {
	s := newStore(t, Config{})

	if r := s.Get("k", 0); r.Status != GetMiss || r.LatestRev != 0 {
		t.Fatalf("absent key: %+v", r)
	}
	if r := put(t, s, "k", "v1", nil); r.Status != PutOK || r.Rev != 1 {
		t.Fatalf("first put: %+v", r)
	}
	put(t, s, "k", "v2", nil)

	r := s.Get("k", 0)
	if r.Status != GetHit || r.Version.Rev != 2 || string(r.Version.Data) != "v2" || r.LatestRev != 2 {
		t.Fatalf("latest: %+v", r)
	}
	r = s.Get("k", 1)
	if r.Status != GetHit || string(r.Version.Data) != "v1" || r.LatestRev != 2 {
		t.Fatalf("previous: %+v", r)
	}
	if r := s.Get("k", 99); r.Status != GetVersionNotFound || r.LatestRev != 2 {
		t.Fatalf("unknown version: %+v", r)
	}

	if !s.Delete("k", 2) {
		t.Fatal("delete latest")
	}
	if r := s.Get("k", 0); r.Version.Rev != 1 || r.LatestRev != 1 {
		t.Fatalf("after delete latest: %+v", r)
	}
	if s.Delete("k", 2) {
		t.Fatal("repeated delete must report not found")
	}

	if !s.Delete("k", 1) {
		t.Fatal("delete last version")
	}
	if r := s.Get("k", 0); r.Status != GetMiss {
		t.Fatalf("key must be gone: %+v", r)
	}
	if st := s.Stats(); st.Keys != 0 || st.Bytes != 0 {
		t.Fatalf("empty store stats: %+v", st)
	}
	if s.Delete("missing", 0) {
		t.Fatal("delete missing key")
	}
	checkInvariants(t, s)
}

func TestDeleteAll(t *testing.T) {
	s := newStore(t, Config{})
	put(t, s, "k", "v1", nil)
	put(t, s, "k", "v2", nil)
	if !s.Delete("k", 0) {
		t.Fatal("delete all")
	}
	if r := s.Get("k", 1); r.Status != GetMiss {
		t.Fatalf("all versions must be gone: %+v", r)
	}
	checkInvariants(t, s)
}

func TestVersionLimit(t *testing.T) {
	s := newStore(t, Config{MaxVersions: 3})
	for i := 1; i <= 5; i++ {
		put(t, s, "k", fmt.Sprintf("v%d", i), nil)
	}
	for rev := uint64(1); rev <= 2; rev++ {
		if r := s.Get("k", rev); r.Status != GetVersionNotFound || r.LatestRev != 5 {
			t.Fatalf("rev %d must be dropped by the limit: %+v", rev, r)
		}
	}
	for rev := uint64(3); rev <= 5; rev++ {
		if r := s.Get("k", rev); r.Status != GetHit || string(r.Version.Data) != fmt.Sprintf("v%d", rev) {
			t.Fatalf("rev %d must be kept: %+v", rev, r)
		}
	}
	checkInvariants(t, s)
}

func TestRevisionsAreNeverReused(t *testing.T) {
	s := newStore(t, Config{})
	put(t, s, "a", "1", nil)
	put(t, s, "b", "1", nil)
	put(t, s, "a", "2", nil)
	s.Delete("a", 3)
	if r := put(t, s, "a", "3", nil); r.Rev != 4 {
		t.Fatalf("deleted revision reused: %+v", r)
	}
	s.Delete("a", 0)
	if r := put(t, s, "a", "4", nil); r.Rev != 5 {
		t.Fatalf("revision reused after delete all: %+v", r)
	}
}

func TestExpectedRev(t *testing.T) {
	s := newStore(t, Config{})
	if r := put(t, s, "k", "x", ptr(5)); r.Status != PutConflict || r.Rev != 0 {
		t.Fatalf("expected 5 on absent key: %+v", r)
	}
	if r := put(t, s, "k", "x", ptr(0)); r.Status != PutOK || r.Rev != 1 {
		t.Fatalf("create if absent: %+v", r)
	}
	if r := put(t, s, "k", "x", ptr(0)); r.Status != PutConflict || r.Rev != 1 {
		t.Fatalf("second create must conflict: %+v", r)
	}
	if r := put(t, s, "k", "y", ptr(1)); r.Status != PutOK || r.Rev != 2 {
		t.Fatalf("matching expected: %+v", r)
	}
	if r := put(t, s, "k", "z", ptr(1)); r.Status != PutConflict || r.Rev != 2 {
		t.Fatalf("stale expected: %+v", r)
	}

	if r := put(t, s, "k", "z", nil); r.Rev != 3 {
		t.Fatalf("conflict consumed a revision: %+v", r)
	}
}

func TestTooLarge(t *testing.T) {
	s := newStore(t, Config{MaxValueBytes: 4})
	r, err := s.Put("k", []byte("12345"), nil)
	if err != nil || r.Status != PutTooLarge {
		t.Fatalf("oversize: %+v %v", r, err)
	}
	if r := put(t, s, "k", "1234", nil); r.Status != PutOK || r.Rev != 1 {
		t.Fatalf("oversize consumed a revision: %+v", r)
	}
}

func TestDataIsCopied(t *testing.T) {
	s := newStore(t, Config{})
	in := []byte("abc")
	if _, err := s.Put("k", in, nil); err != nil {
		t.Fatal(err)
	}
	in[0] = 'X'
	out := s.Get("k", 0).Version.Data
	if string(out) != "abc" {
		t.Fatalf("input aliased: %q", out)
	}
	out[0] = 'Y'
	if got := s.Get("k", 0).Version.Data; string(got) != "abc" {
		t.Fatalf("output aliased: %q", got)
	}
}

func twoKeyStore(t *testing.T) *Store {
	t.Helper()
	perKey := int64(keyOverhead + 1 + versionOverhead + 100)
	return newStore(t, Config{MaxBytes: 2 * perKey, MaxValueBytes: 100})
}

func TestLRUEvictsLeastRecentlyUsedKey(t *testing.T) {
	s := twoKeyStore(t)
	v := string(bytes.Repeat([]byte("x"), 100))
	put(t, s, "a", v, nil)
	put(t, s, "b", v, nil)
	s.Get("a", 0)
	put(t, s, "c", v, nil)

	if r := s.Get("b", 0); r.Status != GetMiss {
		t.Fatalf("b must be evicted: %+v", r)
	}
	for _, k := range []string{"a", "c"} {
		if r := s.Get(k, 0); r.Status != GetHit {
			t.Fatalf("%s must stay: %+v", k, r)
		}
	}
	checkInvariants(t, s)
}

func TestPeekDoesNotChangeEvictionOrder(t *testing.T) {
	s := twoKeyStore(t)
	v := string(bytes.Repeat([]byte("x"), 100))
	put(t, s, "a", v, nil)
	put(t, s, "b", v, nil)
	if r := s.Peek("a", 0); r.Status != GetHit || r.Version.Rev != 1 {
		t.Fatalf("peek a: %+v", r)
	}
	if r := s.Peek("a", 99); r.Status != GetVersionNotFound || r.LatestRev != 1 {
		t.Fatalf("peek missing version: %+v", r)
	}
	if r := s.Peek("zz", 0); r.Status != GetMiss {
		t.Fatalf("peek miss: %+v", r)
	}
	put(t, s, "c", v, nil)
	// a stayed least recently written despite the peek, so it is evicted.
	if r := s.Peek("a", 0); r.Status != GetMiss {
		t.Fatalf("a must be evicted: %+v", r)
	}
	checkInvariants(t, s)
}

func TestPutCountsAsUse(t *testing.T) {
	s := twoKeyStore(t)
	v := string(bytes.Repeat([]byte("x"), 100))
	put(t, s, "a", v, nil)
	put(t, s, "b", v, nil)
	s.Delete("a", 1)
	put(t, s, "a", v, nil)
	put(t, s, "c", v, nil)
	if r := s.Get("b", 0); r.Status != GetMiss {
		t.Fatalf("b must be evicted: %+v", r)
	}
	checkInvariants(t, s)
}

func TestSingleKeyKeepsNewestVersionsThatFit(t *testing.T) {

	capacity := int64(keyOverhead + 1 + 2*(versionOverhead+100))
	s := newStore(t, Config{MaxBytes: capacity, MaxValueBytes: 100, MaxVersions: 3})
	v := string(bytes.Repeat([]byte("x"), 100))
	for i := 0; i < 3; i++ {
		put(t, s, "k", v, nil)
	}
	if r := s.Get("k", 1); r.Status != GetVersionNotFound {
		t.Fatalf("oldest version must be dropped to fit: %+v", r)
	}
	if r := s.Get("k", 0); r.Status != GetHit || r.Version.Rev != 3 {
		t.Fatalf("newest version must stay: %+v", r)
	}
	checkInvariants(t, s)
}

func TestConfigValidation(t *testing.T) {
	for _, cfg := range []Config{
		{MaxVersions: -1},
		{MaxValueBytes: -1},
		{MaxBytes: 100, MaxValueBytes: 100},
	} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("config %+v must be rejected", cfg)
		}
	}
	s := newStore(t, Config{})
	if c := s.Config(); c.MaxVersions != DefaultMaxVersions || c.MaxBytes != DefaultMaxBytes || c.MaxValueBytes != DefaultMaxValueBytes {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestRevisionExhaustion(t *testing.T) {
	s := newStore(t, Config{})
	s.seq = math.MaxUint64 - 1
	if r := put(t, s, "k", "x", nil); r.Rev != math.MaxUint64 {
		t.Fatalf("last revision: %+v", r)
	}
	if _, err := s.Put("k", nil, nil); err != ErrRevisionsExhausted {
		t.Fatalf("want ErrRevisionsExhausted, got %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := newStore(t, Config{MaxBytes: 4 << 10, MaxValueBytes: 256})
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := fmt.Sprintf("k%d", (g+i)%10)
				switch i % 4 {
				case 0, 1:
					if _, err := s.Put(key, bytes.Repeat([]byte{byte(i)}, i%200), nil); err != nil {
						t.Error(err)
					}
				case 2:
					s.Get(key, 0)
				case 3:
					s.Delete(key, uint64(i))
				}
			}
		}(g)
	}
	wg.Wait()
	checkInvariants(t, s)
}
