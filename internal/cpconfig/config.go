// Package cpconfig определяет неизменяемый версионированный snapshot
// конфигурации Control Plane, общий для CP API, CP Agent и Gateway.
//
// Snapshot содержит регион-скоупные assignments: (region, partition_id)
// идентифицирует локальную Raft-группу, поэтому master и slave регионы имеют
// независимых лидеров для одной партиции. После применения snapshot неизменяем:
// вызывающий код не должен менять структуру или её slices.
package cpconfig

import (
	"errors"
	"fmt"
)

// DefaultPartitionCount — фиксированное число партиций кластера OraCache.
// Оно входит в snapshot и не должно меняться между revision.
const DefaultPartitionCount uint32 = 128

// Ошибки валидации. Вызывающий код сравнивает их через errors.Is и не парсит текст.
var (
	ErrInvalidConfig         = errors.New("oracache/cpconfig: invalid config")
	ErrStaleRevision         = errors.New("oracache/cpconfig: revision must increase")
	ErrPartitionCountChanged = errors.New("oracache/cpconfig: partition count is immutable")
)

// RegionRole определяет, где принимаются клиентские записи. Это свойство
// региона, а не Raft-лидера.
type RegionRole uint8

const (
	RegionRoleUnspecified RegionRole = iota
	RegionRoleMaster
	RegionRoleSlave
)

func (r RegionRole) String() string {
	switch r {
	case RegionRoleMaster:
		return "master"
	case RegionRoleSlave:
		return "slave"
	default:
		return "unspecified"
	}
}

// ParseRegionRole разбирает текстовую форму, используемую в файлах конфигурации.
func ParseRegionRole(s string) (RegionRole, error) {
	switch s {
	case "master":
		return RegionRoleMaster, nil
	case "slave":
		return RegionRoleSlave, nil
	default:
		return RegionRoleUnspecified, fmt.Errorf("%w: unknown region role %q", ErrInvalidConfig, s)
	}
}

// MarshalText реализует encoding.TextMarshaler для файлов конфигурации.
func (r RegionRole) MarshalText() ([]byte, error) {
	if !r.Valid() {
		return nil, fmt.Errorf("%w: unknown region role %d", ErrInvalidConfig, r)
	}
	return []byte(r.String()), nil
}

// UnmarshalText реализует encoding.TextUnmarshaler для файлов конфигурации.
func (r *RegionRole) UnmarshalText(text []byte) error {
	parsed, err := ParseRegionRole(string(text))
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

func (r RegionRole) Valid() bool {
	return r == RegionRoleMaster || r == RegionRoleSlave
}

// NodeState повторяет Node.state из protobuf-контракта. Пустое значение
// допускается для ноды, которая ещё не присылала heartbeat.
const (
	NodeStateStarting = "starting"
	NodeStateReady    = "ready"
	NodeStateDraining = "draining"
	NodeStateDead     = "dead"
)

// Config — одна неизменяемая revision конфигурации кластера.
type Config struct {
	Revision       uint64                `json:"revision"`
	PartitionCount uint32                `json:"partition_count"` // в production используется DefaultPartitionCount
	Regions        []Region              `json:"regions"`
	Nodes          []Node                `json:"nodes"`
	Partitions     []PartitionAssignment `json:"partitions"`
	Memberships    []GroupMembership     `json:"memberships,omitempty"`
}

// GroupMembership is a leader-reported applied Raft configuration. It never
// changes the static ReplicaNodeIDs assignment.
type GroupMembership struct {
	GroupID     string        `json:"group_id"`
	Region      string        `json:"region"`
	PartitionID uint32        `json:"partition_id"`
	ReporterID  uint64        `json:"reporter_peer_id"`
	Term        uint64        `json:"term"`
	Index       uint64        `json:"config_index"`
	Joint       bool          `json:"joint"`
	Members     []GroupMember `json:"members"`
	RemovedIDs  []uint64      `json:"removed_ids,omitempty"`
	Confirmed   bool          `json:"confirmed,omitempty"`
}

type GroupMember struct {
	PeerID        uint64 `json:"peer_id"`
	NodeID        string `json:"node_id"`
	Address       string `json:"address"`
	RaftAddress   string `json:"raft_address"`
	Voter         bool   `json:"voter"`
	Learner       bool   `json:"learner"`
	OutgoingVoter bool   `json:"outgoing_voter"`
}

// Region — домен отказа и размещения.
type Region struct {
	ID               string     `json:"id"`
	Role             RegionRole `json:"role"`
	GatewayEndpoints []string   `json:"gateway_endpoints,omitempty"`
}

// Node — процесс Storage Node внутри региона.
type Node struct {
	ID            string `json:"id"`
	Region        string `json:"region"`
	Address       string `json:"address"`
	State         string `json:"state,omitempty"`
	CapacityBytes uint64 `json:"capacity_bytes,omitempty"`
	Zone          string `json:"zone,omitempty"`
}

// PartitionAssignment — локальная Raft-группа одной партиции в одном регионе.
//
// Ready/Readable/Writable заполняются live-view по heartbeat. В статическом
// bootstrap snapshot они могут быть false.
type PartitionAssignment struct {
	PartitionID         uint32   `json:"partition_id"`
	Region              string   `json:"region"`
	GroupID             string   `json:"group_id,omitempty"`
	LeaderNodeID        string   `json:"leader_node_id"`
	ReplicaNodeIDs      []string `json:"replica_node_ids"`
	Ready               bool     `json:"ready,omitempty"`
	Readable            bool     `json:"readable,omitempty"`
	Writable            bool     `json:"writable,omitempty"`
	ObservedTerm        uint64   `json:"observed_term,omitempty"`
	ReadyReplicaNodeIDs []string `json:"ready_replica_node_ids,omitempty"`
	LeaderConflict      bool     `json:"leader_conflict,omitempty"`
}

// Validate проверяет инварианты конфигурации Control Plane. Вызывается перед
// сохранением или применением snapshot, поэтому невалидная конфигурация никогда
// не заменяет last-known-good.
func (c *Config) Validate() error {
	if c == nil {
		return fmt.Errorf("%w: nil snapshot", ErrInvalidConfig)
	}
	if c.Revision == 0 {
		return fmt.Errorf("%w: revision must be positive", ErrInvalidConfig)
	}
	if c.PartitionCount == 0 {
		return fmt.Errorf("%w: partition count must be positive", ErrInvalidConfig)
	}
	if c.PartitionCount > DefaultPartitionCount {
		return fmt.Errorf("%w: partition count %d exceeds the fixed maximum %d", ErrInvalidConfig, c.PartitionCount, DefaultPartitionCount)
	}
	if len(c.Regions) == 0 {
		return fmt.Errorf("%w: at least one region is required", ErrInvalidConfig)
	}

	regions := make(map[string]Region, len(c.Regions))
	masters := 0
	for i := range c.Regions {
		r := c.Regions[i]
		if r.ID == "" {
			return fmt.Errorf("%w: region id must not be empty", ErrInvalidConfig)
		}
		if _, ok := regions[r.ID]; ok {
			return fmt.Errorf("%w: duplicate region %q", ErrInvalidConfig, r.ID)
		}
		if !r.Role.Valid() {
			return fmt.Errorf("%w: region %q has no master/slave role", ErrInvalidConfig, r.ID)
		}
		if r.Role == RegionRoleMaster {
			masters++
		}
		regions[r.ID] = r
	}
	if masters != 1 {
		return fmt.Errorf("%w: exactly one master region is required, got %d", ErrInvalidConfig, masters)
	}

	nodes := make(map[string]Node, len(c.Nodes))
	for i := range c.Nodes {
		n := c.Nodes[i]
		if n.ID == "" {
			return fmt.Errorf("%w: node id must not be empty", ErrInvalidConfig)
		}
		if _, ok := nodes[n.ID]; ok {
			return fmt.Errorf("%w: duplicate node %q", ErrInvalidConfig, n.ID)
		}
		if _, ok := regions[n.Region]; !ok {
			return fmt.Errorf("%w: node %q references unknown region %q", ErrInvalidConfig, n.ID, n.Region)
		}
		if n.Address == "" {
			return fmt.Errorf("%w: node %q has empty address", ErrInvalidConfig, n.ID)
		}
		if !validNodeState(n.State) {
			return fmt.Errorf("%w: node %q has unknown state %q", ErrInvalidConfig, n.ID, n.State)
		}
		nodes[n.ID] = n
	}

	type assignmentKey struct {
		region    string
		partition uint32
	}
	seen := make(map[assignmentKey]struct{}, len(c.Partitions))
	coverage := make(map[string][]bool, len(regions))
	for id := range regions {
		coverage[id] = make([]bool, c.PartitionCount)
	}
	for i := range c.Partitions {
		a := c.Partitions[i]
		if a.PartitionID >= c.PartitionCount {
			return fmt.Errorf("%w: partition %d is outside [0,%d)", ErrInvalidConfig, a.PartitionID, c.PartitionCount)
		}
		if _, ok := regions[a.Region]; !ok {
			return fmt.Errorf("%w: assignment references unknown region %q", ErrInvalidConfig, a.Region)
		}
		key := assignmentKey{region: a.Region, partition: a.PartitionID}
		if _, ok := seen[key]; ok {
			return fmt.Errorf("%w: duplicate assignment (%s,%d)", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		seen[key] = struct{}{}
		if len(a.ReplicaNodeIDs) == 0 {
			return fmt.Errorf("%w: assignment (%s,%d) has no replicas", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		if a.LeaderNodeID == "" {
			return fmt.Errorf("%w: assignment (%s,%d) has no leader", ErrInvalidConfig, a.Region, a.PartitionID)
		}

		replicas := make(map[string]struct{}, len(a.ReplicaNodeIDs))
		for _, nodeID := range a.ReplicaNodeIDs {
			node, ok := nodes[nodeID]
			if !ok {
				return fmt.Errorf("%w: assignment (%s,%d) references unknown node %q", ErrInvalidConfig, a.Region, a.PartitionID, nodeID)
			}
			if node.Region != a.Region {
				return fmt.Errorf("%w: replica %q is not in assignment region %q", ErrInvalidConfig, nodeID, a.Region)
			}
			if _, ok := replicas[nodeID]; ok {
				return fmt.Errorf("%w: assignment (%s,%d) repeats replica %q", ErrInvalidConfig, a.Region, a.PartitionID, nodeID)
			}
			replicas[nodeID] = struct{}{}
		}
		if _, ok := replicas[a.LeaderNodeID]; !ok && !c.memberVoter(a.Region, a.PartitionID, a.LeaderNodeID) {
			return fmt.Errorf("%w: leader %q is not a replica in assignment (%s,%d)", ErrInvalidConfig, a.LeaderNodeID, a.Region, a.PartitionID)
		}
		leaderReady := false
		for _, nodeID := range a.ReadyReplicaNodeIDs {
			if _, ok := replicas[nodeID]; !ok && !c.memberVoter(a.Region, a.PartitionID, nodeID) {
				return fmt.Errorf("%w: ready node %q is not a replica in assignment (%s,%d)", ErrInvalidConfig, nodeID, a.Region, a.PartitionID)
			}
			leaderReady = leaderReady || nodeID == a.LeaderNodeID
		}
		if a.Ready && len(a.ReadyReplicaNodeIDs) != 0 && !leaderReady {
			return fmt.Errorf("%w: ready assignment (%s,%d) has no ready leader", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		if a.LeaderConflict && a.Ready {
			return fmt.Errorf("%w: assignment (%s,%d) is ready despite a leader conflict", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		if a.Readable && !a.Ready {
			return fmt.Errorf("%w: assignment (%s,%d) is readable but not ready", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		if a.Writable && (!a.Ready || !a.Readable) {
			return fmt.Errorf("%w: assignment (%s,%d) is writable but not ready/readable", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		if a.Writable && regions[a.Region].Role != RegionRoleMaster {
			return fmt.Errorf("%w: assignment (%s,%d) is writable outside master region", ErrInvalidConfig, a.Region, a.PartitionID)
		}
		coverage[a.Region][a.PartitionID] = true
	}
	seenGroups := map[assignmentKey]bool{}
	for _, m := range c.Memberships {
		if m.GroupID == "" || m.Index == 0 || m.PartitionID >= c.PartitionCount {
			return fmt.Errorf("%w: invalid observed membership", ErrInvalidConfig)
		}
		if _, ok := seen[assignmentKey{m.Region, m.PartitionID}]; !ok {
			return fmt.Errorf("%w: membership has no assignment", ErrInvalidConfig)
		}
		if a, ok := c.Assignment(m.Region, m.PartitionID); ok && a.GroupID != "" && a.GroupID != m.GroupID {
			return fmt.Errorf("%w: membership group_id does not match assignment", ErrInvalidConfig)
		}
		key := assignmentKey{m.Region, m.PartitionID}
		if seenGroups[key] {
			return fmt.Errorf("%w: duplicate membership for %s/%d", ErrInvalidConfig, m.Region, m.PartitionID)
		}
		seenGroups[key] = true
		ids, nodeIDs := map[uint64]bool{}, map[string]bool{}
		outgoingIDs := map[uint64]bool{}
		reporter := false
		for _, peer := range m.Members {
			if peer.PeerID == 0 || peer.NodeID == "" || peer.Address == "" || peer.RaftAddress == "" || ids[peer.PeerID] || nodeIDs[peer.NodeID] || (peer.Voter && peer.Learner) || (!peer.Voter && !peer.Learner && !peer.OutgoingVoter) {
				return fmt.Errorf("%w: invalid observed member", ErrInvalidConfig)
			}
			ids[peer.PeerID] = true
			nodeIDs[peer.NodeID] = true
			outgoingIDs[peer.PeerID] = peer.OutgoingVoter
			reporter = reporter || (peer.PeerID == m.ReporterID && peer.Voter)
		}
		if !reporter {
			return fmt.Errorf("%w: reporter is not a voter", ErrInvalidConfig)
		}
		removedIDs := map[uint64]bool{}
		for _, id := range m.RemovedIDs {
			if id == 0 || (ids[id] && (!m.Joint || !outgoingIDs[id])) || removedIDs[id] {
				return fmt.Errorf("%w: invalid removed peer", ErrInvalidConfig)
			}
			removedIDs[id] = true
		}
	}

	for regionID, parts := range coverage {
		for partitionID, covered := range parts {
			if !covered {
				return fmt.Errorf("%w: region %q is missing assignment for partition %d", ErrInvalidConfig, regionID, partitionID)
			}
		}
	}
	return nil
}

func validNodeState(state string) bool {
	switch state {
	case "", NodeStateStarting, NodeStateReady, NodeStateDraining, NodeStateDead:
		return true
	default:
		return false
	}
}

// MasterRegion возвращает единственный регион, принимающий клиентские записи.
func (c *Config) MasterRegion() string {
	if c == nil {
		return ""
	}
	for i := range c.Regions {
		if c.Regions[i].Role == RegionRoleMaster {
			return c.Regions[i].ID
		}
	}
	return ""
}

// RegionByID возвращает регион по id.
func (c *Config) RegionByID(id string) (Region, bool) {
	if c == nil {
		return Region{}, false
	}
	for i := range c.Regions {
		if c.Regions[i].ID == id {
			return c.Regions[i], true
		}
	}
	return Region{}, false
}

// NodeByID возвращает ноду по id.
func (c *Config) NodeByID(id string) (Node, bool) {
	if c == nil {
		return Node{}, false
	}
	for i := range c.Nodes {
		if c.Nodes[i].ID == id {
			return c.Nodes[i], true
		}
	}
	return Node{}, false
}

// Assignment возвращает регион-скоупный assignment партиции.
func (c *Config) Assignment(region string, partitionID uint32) (PartitionAssignment, bool) {
	if c == nil {
		return PartitionAssignment{}, false
	}
	for i := range c.Partitions {
		a := c.Partitions[i]
		if a.Region == region && a.PartitionID == partitionID {
			return a, true
		}
	}
	return PartitionAssignment{}, false
}

// Clone возвращает глубокую копию. Неизменяемые snapshot всё равно пересекают
// границы пакетов и goroutine, поэтому владелец входящих данных копирует их
// перед сохранением.
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}
	out := *c
	out.Regions = make([]Region, len(c.Regions))
	for i := range c.Regions {
		out.Regions[i] = c.Regions[i]
		out.Regions[i].GatewayEndpoints = append([]string(nil), c.Regions[i].GatewayEndpoints...)
	}
	out.Nodes = append([]Node(nil), c.Nodes...)
	out.Memberships = make([]GroupMembership, len(c.Memberships))
	for i := range c.Memberships {
		out.Memberships[i] = c.Memberships[i]
		out.Memberships[i].Members = append([]GroupMember(nil), c.Memberships[i].Members...)
		out.Memberships[i].RemovedIDs = append([]uint64(nil), c.Memberships[i].RemovedIDs...)
	}
	out.Partitions = make([]PartitionAssignment, len(c.Partitions))
	for i := range c.Partitions {
		out.Partitions[i] = c.Partitions[i]
		out.Partitions[i].ReplicaNodeIDs = append([]string(nil), c.Partitions[i].ReplicaNodeIDs...)
		out.Partitions[i].ReadyReplicaNodeIDs = append([]string(nil), c.Partitions[i].ReadyReplicaNodeIDs...)
	}
	return &out
}

func (c *Config) memberVoter(region string, partition uint32, nodeID string) bool {
	for _, m := range c.Memberships {
		if m.Region == region && m.PartitionID == partition {
			for _, peer := range m.Members {
				if peer.NodeID == nodeID && peer.Voter {
					return true
				}
			}
		}
	}
	return false
}
