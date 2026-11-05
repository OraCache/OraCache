package cpconfig

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Holder хранит последний успешный неизменяемый snapshot конфигурации. Apply —
// единственный mutator: он валидирует перед заменой, отклоняет устаревшие revision
// и запрещает менять число партиций. Читатели используют Current без блокировки.
//
// Holder нельзя копировать после первого использования.
type Holder struct {
	mu  sync.Mutex
	cur atomic.Pointer[Config]
}

// NewHolder возвращает пустой holder. Current возвращает nil, пока не применён
// валидный snapshot.
func NewHolder() *Holder { return &Holder{} }

// Apply валидирует next и атомарно заменяет текущий snapshot. При любой ошибке
// текущий snapshot сохраняется.
func (h *Holder) Apply(next *Config) error {
	if err := next.Validate(); err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	old := h.cur.Load()
	if old != nil {
		if next.Revision <= old.Revision {
			return fmt.Errorf("%w: have %d, got %d", ErrStaleRevision, old.Revision, next.Revision)
		}
		if next.PartitionCount != old.PartitionCount {
			return fmt.Errorf("%w: have %d, got %d", ErrPartitionCountChanged, old.PartitionCount, next.PartitionCount)
		}
	}
	h.cur.Store(next.Clone())
	return nil
}

// Current возвращает текущий неизменяемый snapshot или nil, если он не применён.
func (h *Holder) Current() *Config { return h.cur.Load() }

// Revision возвращает текущую revision конфигурации или 0, если snapshot не применён.
func (h *Holder) Revision() uint64 {
	if cur := h.cur.Load(); cur != nil {
		return cur.Revision
	}
	return 0
}
