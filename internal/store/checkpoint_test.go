package store

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCheckpointRestorePreservesEverything(t *testing.T) {
	cfg := Config{MaxVersions: 2, MaxValueBytes: 100, MaxBytes: 3 * (keyOverhead + 1 + 2*(versionOverhead+100))}
	src := newStore(t, cfg)
	v := string(bytes.Repeat([]byte("x"), 100))
	for _, k := range []string{"a", "b", "c", "a", "a"} {
		put(t, src, k, v, nil)
	}
	src.Delete("b", 0) // seq keeps counting past deleted revisions

	data, err := src.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	dst := newStore(t, cfg)
	put(t, dst, "junk", "junk", nil)
	if err := dst.Restore(data); err != nil {
		t.Fatal(err)
	}
	checkInvariants(t, dst)
	again, _ := dst.Checkpoint()
	if !bytes.Equal(data, again) {
		t.Fatalf("restore is not lossless:\n%s\n%s", data, again)
	}
	if dst.Peek("junk", 0).Status != GetMiss {
		t.Fatal("restore must replace old state")
	}
	if r := put(t, dst, "b", "new", nil); r.Rev != 6 {
		t.Fatalf("revision must continue after the deleted b (seq=5): %+v", r)
	}
	put(t, src, "b", "new", nil)

	// Identical follow-up writes evict identically: the same LRU order was restored.
	for _, k := range []string{"d", "e", "f"} {
		put(t, src, k, v, nil)
		put(t, dst, k, v, nil)
	}
	a, _ := src.Checkpoint()
	b, _ := dst.Checkpoint()
	if !bytes.Equal(a, b) {
		t.Fatalf("replicas diverged:\n%s\n%s", a, b)
	}
}

func TestRestoreRejectsBadCheckpointsAndKeepsState(t *testing.T) {
	s := newStore(t, Config{MaxVersions: 2})
	put(t, s, "k", "v", nil)
	good, _ := s.Checkpoint()
	other := newStore(t, Config{MaxVersions: 3})
	otherData, _ := other.Checkpoint()

	hdr := `"format":1,"config":{"MaxVersions":2,"MaxBytes":268435456,"MaxValueBytes":10240}`
	bad := map[string]string{
		"garbage":      "{",
		"format":       `{"format":9}`,
		"config":       string(otherData),
		"empty key":    fmt.Sprintf(`{%s,"seq":1,"keys":[{"key":"","versions":[{"rev":1}]}]}`, hdr),
		"rev over seq": fmt.Sprintf(`{%s,"seq":1,"keys":[{"key":"k","versions":[{"rev":2}]}]}`, hdr),
		"unordered":    fmt.Sprintf(`{%s,"seq":3,"keys":[{"key":"k","versions":[{"rev":3},{"rev":2}]}]}`, hdr),
		"too many":     fmt.Sprintf(`{%s,"seq":3,"keys":[{"key":"k","versions":[{"rev":1},{"rev":2},{"rev":3}]}]}`, hdr),
	}
	for name, data := range bad {
		if err := s.Restore([]byte(data)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	after, _ := s.Checkpoint()
	if !bytes.Equal(good, after) {
		t.Fatal("failed restore modified the store")
	}
}
