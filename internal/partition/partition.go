// Package partition implements the fixed OraCache partition mapping.
//
// partition_id = xxhash64(key) % partition_count. The hash is stable between
// processes and releases; maphash and randomized hashes are forbidden. There is
// no consistent hashing and no ring.
package partition

import "github.com/cespare/xxhash/v2"

// DefaultCount is the fixed number of partitions in an OraCache cluster.
const DefaultCount uint32 = 128

// Sum64 returns the stable xxhash64 of a key.
func Sum64(key string) uint64 { return xxhash.Sum64String(key) }

// Of maps a key to a partition. It panics when count is zero, because a
// configuration with no partitions is invalid rather than an empty cluster.
func Of(key string, count uint32) uint32 {
	if count == 0 {
		panic("oracache/partition: partition count must be positive")
	}
	return uint32(Sum64(key) % uint64(count))
}
