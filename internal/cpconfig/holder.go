package cpconfig

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Holder stores the last-known-good immutable configuration snapshot. Apply is
// the only mutator: it validates before swapping, rejects stale revisions and
// forbids changing the partition count. Readers use Current lock-free.
//
// Holder must not be copied after first use.
type Holder struct {
	mu  sync.Mutex
	cur atomic.Pointer[Config]
}

// NewHolder returns an empty holder. Current returns nil until a valid snapshot
// is applied.
func NewHolder() *Holder { return &Holder{} }

// Apply validates next, then atomically replaces the current snapshot. On any
// error the current snapshot is preserved.
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

// Current returns the current immutable snapshot or nil if none was applied.
func (h *Holder) Current() *Config { return h.cur.Load() }

// Revision returns the current config revision, or 0 if none was applied.
func (h *Holder) Revision() uint64 {
	if cur := h.cur.Load(); cur != nil {
		return cur.Revision
	}
	return 0
}
