// Package partition реализует фиксированное отображение ключей в партиции OraCache.
//
// partition_id = xxhash64(key) % partition_count. Хэш стабилен между процессами
// и релизами; maphash и рандомизированные хэши запрещены. Consistent hashing
// и кольцо не используются.
package partition

import "github.com/cespare/xxhash/v2"

// DefaultCount — фиксированное число партиций в кластере OraCache.
const DefaultCount uint32 = 128

// Sum64 возвращает стабильный xxhash64 ключа.
func Sum64(key string) uint64 { return xxhash.Sum64String(key) }

// Of отображает ключ в партицию. Паникует при count == 0, потому что
// конфигурация без партиций невалидна, а не означает пустой кластер.
func Of(key string, count uint32) uint32 {
	if count == 0 {
		panic("oracache/partition: partition count must be positive")
	}
	return uint32(Sum64(key) % uint64(count))
}
