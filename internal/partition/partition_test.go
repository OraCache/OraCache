package partition_test

import (
	"fmt"
	"testing"

	"github.com/OraCache/oracache/internal/partition"
)

func TestSum64GoldenVectors(t *testing.T) {
	t.Parallel()
	// Fixed xxhash64 vectors (seed 0). Changing these is a partition migration.
	vectors := []struct {
		key  string
		want uint64
	}{
		{key: "", want: 0xef46db3751d8e999},
		{key: "a", want: 0xd24ec4f1a98c6e5b},
		{key: "abc", want: 0x44bc2cf5ad770999},
		{key: "hello", want: 0x26c7827d889f6da3},
		{key: "profile:42", want: 0xc53112598aca1913},
	}
	for _, tt := range vectors {
		if got := partition.Sum64(tt.key); got != tt.want {
			t.Errorf("Sum64(%q) = %#x, want %#x", tt.key, got, tt.want)
		}
	}
}

func TestOfIsStable(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"", "a", "abc", "hello", "profile:42"} {
		first := partition.Of(key, partition.DefaultCount)
		for i := 0; i < 100; i++ {
			if got := partition.Of(key, partition.DefaultCount); got != first {
				t.Fatalf("Of(%q) changed from %d to %d", key, first, got)
			}
		}
	}
}

func TestOfGoldenPartitionVectors(t *testing.T) {
	t.Parallel()
	vectors := []struct {
		key  string
		want uint32
	}{
		{key: "", want: 25},
		{key: "a", want: 91},
		{key: "abc", want: 25},
		{key: "hello", want: 35},
		{key: "profile:42", want: 19},
	}
	for _, tt := range vectors {
		if got := partition.Of(tt.key, partition.DefaultCount); got != tt.want {
			t.Errorf("Of(%q,128) = %d, want %d", tt.key, got, tt.want)
		}
	}
}

func TestAllDefaultPartitionsAreReachable(t *testing.T) {
	t.Parallel()
	seen := make(map[uint32]bool, partition.DefaultCount)
	for i := 0; i < 100_000 && len(seen) < int(partition.DefaultCount); i++ {
		seen[partition.Of(fmt.Sprintf("key:%d", i), partition.DefaultCount)] = true
	}
	if len(seen) != int(partition.DefaultCount) {
		t.Fatalf("reachable partitions = %d, want %d", len(seen), partition.DefaultCount)
	}
}

func TestOfRejectsZeroCount(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("Of with zero count did not panic")
		}
	}()
	partition.Of("key", 0)
}
