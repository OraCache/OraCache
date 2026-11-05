// Package gateway реализует целевой stateless слой маршрутизации API Gateway.
//
// Он намеренно отделён от legacy-кэша в internal/cache. Router хранит только
// last-known-good snapshot Control Plane: вычисляет фиксированную партицию для
// ключа и выбирает локального лидера региона (GET) или лидера master-региона
// (PUT). Данные кэша он не хранит.
package gateway

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/partition"
)

// Ошибки маршрутизации. Вызывающий код использует errors.Is, а не текст ошибки.
var (
	ErrNoConfig              = errors.New("oracache/gateway: no routing config")
	ErrPartitionOutOfRange   = errors.New("oracache/gateway: partition out of range")
	ErrUnknownRegion         = errors.New("oracache/gateway: unknown local region")
	ErrNoLeader              = errors.New("oracache/gateway: no leader for partition")
	ErrStaleRevision         = cpconfig.ErrStaleRevision
	ErrPartitionCountChanged = cpconfig.ErrPartitionCountChanged
)

// Leader — выбранный локальный Raft-лидер. Region включён для диагностики и
// должен совпадать с регионом assignment, из которого получен маршрут.
type Leader struct {
	NodeID  string
	Address string
	Region  string
}

type snapshot struct {
	revision       uint64
	partitionCount uint32
	masterRegion   string
	local          map[uint32]Leader
	write          map[uint32]Leader
}

// Router — lock-free читатель неизменяемого routing snapshot.
//
// Router нельзя копировать после первого использования.
type Router struct {
	region string
	mu     sync.Mutex
	cur    atomic.Pointer[snapshot]
}

// NewRouter создаёт router для одного локального региона. Пустой регион недопустим.
func NewRouter(region string) (*Router, error) {
	if region == "" {
		return nil, fmt.Errorf("%w: empty region", ErrUnknownRegion)
	}
	return &Router{region: region}, nil
}

// Region возвращает локальный регион, который обслуживает router.
func (r *Router) Region() string { return r.region }

// Apply валидирует cfg и атомарно заменяет routing snapshot. Устаревшие revision
// и изменение числа партиций отклоняются; при ошибке предыдущий snapshot
// сохраняется.
func (r *Router) Apply(cfg *cpconfig.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if _, ok := cfg.RegionByID(r.region); !ok {
		return fmt.Errorf("%w: %q", ErrUnknownRegion, r.region)
	}
	next, err := buildSnapshot(r.region, cfg)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	if old != nil {
		if cfg.Revision <= old.revision {
			return fmt.Errorf("%w: have %d, got %d", ErrStaleRevision, old.revision, cfg.Revision)
		}
		if cfg.PartitionCount != old.partitionCount {
			return fmt.Errorf("%w: have %d, got %d", ErrPartitionCountChanged, old.partitionCount, cfg.PartitionCount)
		}
	}
	r.cur.Store(next)
	return nil
}

func buildSnapshot(localRegion string, cfg *cpconfig.Config) (*snapshot, error) {
	masterRegion := cfg.MasterRegion()
	if masterRegion == "" {
		return nil, fmt.Errorf("%w: config has no master region", ErrUnknownRegion)
	}
	s := &snapshot{
		revision:       cfg.Revision,
		partitionCount: cfg.PartitionCount,
		masterRegion:   masterRegion,
		local:          make(map[uint32]Leader, cfg.PartitionCount),
		write:          make(map[uint32]Leader, cfg.PartitionCount),
	}
	for p := uint32(0); p < cfg.PartitionCount; p++ {
		localAssignment, ok := cfg.Assignment(localRegion, p)
		if !ok {
			return nil, fmt.Errorf("%w: local region %q has no assignment for partition %d", ErrNoLeader, localRegion, p)
		}
		localLeader, err := resolveLeader(cfg, localAssignment)
		if err != nil {
			return nil, err
		}
		s.local[p] = localLeader

		writeAssignment, ok := cfg.Assignment(masterRegion, p)
		if !ok {
			return nil, fmt.Errorf("%w: master region %q has no assignment for partition %d", ErrNoLeader, masterRegion, p)
		}
		writeLeader, err := resolveLeader(cfg, writeAssignment)
		if err != nil {
			return nil, err
		}
		s.write[p] = writeLeader
	}
	return s, nil
}

func resolveLeader(cfg *cpconfig.Config, assignment cpconfig.PartitionAssignment) (Leader, error) {
	node, ok := cfg.NodeByID(assignment.LeaderNodeID)
	if !ok {
		return Leader{}, fmt.Errorf("%w: partition %d references unknown leader %q", ErrNoLeader, assignment.PartitionID, assignment.LeaderNodeID)
	}
	if node.Region != assignment.Region {
		return Leader{}, fmt.Errorf("%w: leader %q is outside region %q", ErrNoLeader, node.ID, assignment.Region)
	}
	return Leader{NodeID: node.ID, Address: node.Address, Region: node.Region}, nil
}

// Revision возвращает текущую revision конфигурации или 0, если snapshot не применён.
func (r *Router) Revision() uint64 {
	if cur := r.cur.Load(); cur != nil {
		return cur.revision
	}
	return 0
}

// PartitionCount возвращает текущее число партиций или 0 без snapshot.
func (r *Router) PartitionCount() uint32 {
	if cur := r.cur.Load(); cur != nil {
		return cur.partitionCount
	}
	return 0
}

// MasterRegion возвращает регион, принимающий клиентские записи, или "" без
// snapshot.
func (r *Router) MasterRegion() string {
	if cur := r.cur.Load(); cur != nil {
		return cur.masterRegion
	}
	return ""
}

// Partition возвращает фиксированную партицию для ключа.
func (r *Router) Partition(key string) (uint32, error) {
	cur := r.cur.Load()
	if cur == nil {
		return 0, ErrNoConfig
	}
	return partition.Of(key, cur.partitionCount), nil
}

// GetLeader выбирает локального лидера региона для партиции. Это путь GET:
// строгие чтения всегда идут к лидеру региона вызывающего.
func (r *Router) GetLeader(partitionID uint32) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.local, partitionID, cur.partitionCount)
}

// GetLeaderForKey хэширует ключ и выбирает локального GET-лидера региона.
func (r *Router) GetLeaderForKey(key string) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.local, partition.Of(key, cur.partitionCount), cur.partitionCount)
}

// WriteLeader выбирает лидера master-региона для партиции. В slave-регионе это
// цель проксирования клиентских PUT.
func (r *Router) WriteLeader(partitionID uint32) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.write, partitionID, cur.partitionCount)
}

// WriteLeaderForKey хэширует ключ и выбирает PUT-лидера master-региона.
func (r *Router) WriteLeaderForKey(key string) (Leader, error) {
	cur := r.cur.Load()
	if cur == nil {
		return Leader{}, ErrNoConfig
	}
	return routeFor(cur.write, partition.Of(key, cur.partitionCount), cur.partitionCount)
}

func routeFor(routes map[uint32]Leader, partitionID, partitionCount uint32) (Leader, error) {
	if partitionID >= partitionCount {
		return Leader{}, fmt.Errorf("%w: %d >= %d", ErrPartitionOutOfRange, partitionID, partitionCount)
	}
	leader, ok := routes[partitionID]
	if !ok {
		return Leader{}, fmt.Errorf("%w: partition %d", ErrNoLeader, partitionID)
	}
	return leader, nil
}
