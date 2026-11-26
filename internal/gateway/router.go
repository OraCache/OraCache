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
	local          map[uint32]partitionRoute
	write          map[uint32]partitionRoute
	admin          map[adminKey]adminRoute
}

type adminKey struct {
	group     string
	region    string
	partition uint32
}

type adminRoute struct {
	addresses []string
}

type partitionRoute struct {
	region      string
	leader      Leader
	replicas    []Leader
	observation leaderObservation
}

type leaderObservation struct {
	term     uint64
	nodeID   string
	conflict bool
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

// ApplyView применяет live-view с тем же revision, что и текущая конфигурация.
// Это позволяет обновлять liveness и лидеров без изменения revision топологии.
// Более старая revision и изменение числа партиций отклоняются.
func (r *Router) ApplyView(cfg *cpconfig.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if _, ok := cfg.RegionByID(r.region); !ok {
		return fmt.Errorf("%w: %q", ErrUnknownRegion, r.region)
	}
	next, err := buildSnapshotMode(r.region, cfg, true)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.cur.Load()
	if old != nil {
		if cfg.Revision < old.revision {
			return fmt.Errorf("%w: have %d, got %d", ErrStaleRevision, old.revision, cfg.Revision)
		}
		if cfg.PartitionCount != old.partitionCount {
			return fmt.Errorf("%w: have %d, got %d", ErrPartitionCountChanged, old.partitionCount, cfg.PartitionCount)
		}
		keepLeaderObservations(old.local, next.local)
		keepLeaderObservations(old.write, next.write)
	}
	r.cur.Store(next)
	return nil
}

func buildSnapshot(localRegion string, cfg *cpconfig.Config) (*snapshot, error) {
	return buildSnapshotMode(localRegion, cfg, false)
}

func buildSnapshotMode(localRegion string, cfg *cpconfig.Config, live bool) (*snapshot, error) {
	masterRegion := cfg.MasterRegion()
	if masterRegion == "" {
		return nil, fmt.Errorf("%w: config has no master region", ErrUnknownRegion)
	}
	s := &snapshot{
		revision:       cfg.Revision,
		partitionCount: cfg.PartitionCount,
		masterRegion:   masterRegion,
		local:          make(map[uint32]partitionRoute, cfg.PartitionCount),
		write:          make(map[uint32]partitionRoute, cfg.PartitionCount),
		admin:          make(map[adminKey]adminRoute),
	}
	for _, assignment := range cfg.Partitions {
		if assignment.Region != localRegion && assignment.Region != masterRegion {
			continue
		}
		route, err := buildPartitionRoute(cfg, assignment, live)
		if err != nil {
			return nil, err
		}
		if assignment.Region == localRegion {
			s.local[assignment.PartitionID] = route.forOperation(live && assignment.Ready && !assignment.Readable)
		}
		if assignment.Region == masterRegion {
			s.write[assignment.PartitionID] = route.forOperation(live && assignment.Ready && !assignment.Writable)
		}
	}
	for _, m := range cfg.Memberships {
		route := adminRoute{}
		var leaderAddress string
		for _, a := range cfg.Partitions {
			if a.Region == m.Region && a.PartitionID == m.PartitionID {
				for _, peer := range m.Members {
					if peer.NodeID == a.LeaderNodeID && (peer.Voter || peer.OutgoingVoter) {
						leaderAddress = peer.Address
						break
					}
				}
				break
			}
		}
		if leaderAddress != "" {
			route.addresses = append(route.addresses, leaderAddress)
		}
		for _, peer := range m.Members {
			if (peer.Voter || peer.OutgoingVoter) && peer.Address != leaderAddress {
				route.addresses = append(route.addresses, peer.Address)
			}
		}
		s.admin[adminKey{m.GroupID, m.Region, m.PartitionID}] = route
	}
	return s, nil
}

// AdminCandidates returns only voter addresses from the observed Raft group.
func (r *Router) AdminCandidates(groupID, region string, partition uint32) ([]string, error) {
	s := r.cur.Load()
	if s == nil {
		return nil, ErrNoConfig
	}
	admin, ok := s.admin[adminKey{groupID, region, partition}]
	if !ok {
		return nil, ErrNoLeader
	}
	return append([]string(nil), admin.addresses...), nil
}

func (route partitionRoute) forOperation(disabled bool) partitionRoute {
	if disabled {
		route.leader, route.replicas = Leader{}, nil
	}
	return route
}

func buildPartitionRoute(cfg *cpconfig.Config, assignment cpconfig.PartitionAssignment, live bool) (partitionRoute, error) {
	route := partitionRoute{region: assignment.Region}
	if live {
		route.observation = leaderObservation{term: assignment.ObservedTerm, conflict: assignment.LeaderConflict}
	}
	if !live || assignment.Ready {
		leader, err := resolveReplica(cfg, assignment, assignment.LeaderNodeID)
		if err != nil && !live {
			return partitionRoute{}, err
		}
		route.leader = leader
		route.observation.nodeID = leader.NodeID
	}
	if route.observation.conflict {
		return route, nil
	}

	candidates := assignment.ReplicaNodeIDs
	if live {
		candidates = assignment.ReadyReplicaNodeIDs
		// A server using the previous contract only reports the ready leader.
		if len(candidates) == 0 && route.leader.NodeID != "" {
			candidates = []string{route.leader.NodeID}
		}
	}
	for _, id := range candidates {
		if replica, err := resolveReplica(cfg, assignment, id); err == nil {
			route.replicas = append(route.replicas, replica)
		}
	}
	return route, nil
}

// keepLeaderObservations retains the highest known election term for this
// region and partition without retaining the liveness of an expired leader.
func keepLeaderObservations(old, next map[uint32]partitionRoute) {
	for partitionID, route := range next {
		previous := old[partitionID]
		if previous.region != route.region || previous.observation.term == 0 {
			continue
		}
		switch {
		case route.observation.term < previous.observation.term:
			route.observation = previous.observation
			route.leader, route.replicas = Leader{}, nil
		case route.observation.term == previous.observation.term:
			last, incoming := previous.observation, route.observation
			if incoming.nodeID == "" {
				incoming.nodeID = last.nodeID
			}
			incoming.conflict = incoming.conflict || last.conflict || (last.nodeID != "" && incoming.nodeID != last.nodeID)
			route.observation = incoming
			if incoming.conflict {
				route.leader, route.replicas = Leader{}, nil
			}
		}
		next[partitionID] = route
	}
}

func resolveReplica(cfg *cpconfig.Config, assignment cpconfig.PartitionAssignment, nodeID string) (Leader, error) {
	node, ok := cfg.NodeByID(nodeID)
	if !ok {
		return Leader{}, fmt.Errorf("%w: partition %d references unknown replica %q", ErrNoLeader, assignment.PartitionID, nodeID)
	}
	if node.Region != assignment.Region {
		return Leader{}, fmt.Errorf("%w: leader %q is outside region %q", ErrNoLeader, node.ID, assignment.Region)
	}
	if node.State != "" && node.State != cpconfig.NodeStateReady {
		return Leader{}, fmt.Errorf("%w: leader %q is not ready (state %q)", ErrNoLeader, node.ID, node.State)
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

func routeFor(routes map[uint32]partitionRoute, partitionID, partitionCount uint32) (Leader, error) {
	if partitionID >= partitionCount {
		return Leader{}, fmt.Errorf("%w: %d >= %d", ErrPartitionOutOfRange, partitionID, partitionCount)
	}
	leader := routes[partitionID].leader
	if leader.NodeID == "" {
		return Leader{}, fmt.Errorf("%w: partition %d", ErrNoLeader, partitionID)
	}
	return leader, nil
}
