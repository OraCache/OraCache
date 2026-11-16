// Package cpconfig defines the immutable, versioned Control Plane configuration
// snapshot shared by CP API, CP Agent and Gateway.
//
// The snapshot carries region-scoped assignments: (region, partition_id)
// identifies one local Raft group, so master and slave regions have independent
// leaders for the same partition. A snapshot is immutable after it is applied:
// callers must not mutate the struct or its slices.
package cpconfig

import (
	"errors"
	"fmt"
)

// DefaultPartitionCount is the fixed partition count of an OraCache cluster.
// It is carried in the snapshot and must not change between revisions.
const DefaultPartitionCount uint32 = 128

// Validation errors. Callers compare them with errors.Is and do not parse text.
var (
	ErrInvalidConfig         = errors.New("oracache/cpconfig: invalid config")
	ErrStaleRevision         = errors.New("oracache/cpconfig: revision must increase")
	ErrPartitionCountChanged = errors.New("oracache/cpconfig: partition count is immutable")
)

// RegionRole decides where client writes are accepted. It is a property of a
// region, not of a Raft leader.
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

// ParseRegionRole parses the textual form used in configuration files.
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

// MarshalText implements encoding.TextMarshaler for configuration files.
func (r RegionRole) MarshalText() ([]byte, error) {
	if !r.Valid() {
		return nil, fmt.Errorf("%w: unknown region role %d", ErrInvalidConfig, r)
	}
	return []byte(r.String()), nil
}

// UnmarshalText implements encoding.TextUnmarshaler for configuration files.
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

// NodeState mirrors Node.state in the protobuf contract. Empty is allowed for a
// node that has not reported yet.
const (
	NodeStateStarting = "starting"
	NodeStateReady    = "ready"
	NodeStateDraining = "draining"
	NodeStateDead     = "dead"
)

// Config is one immutable revision of the cluster configuration.
type Config struct {
	Revision       uint64                `json:"revision"`
	PartitionCount uint32                `json:"partition_count"` // production uses DefaultPartitionCount
	Regions        []Region              `json:"regions"`
	Nodes          []Node                `json:"nodes"`
	Partitions     []PartitionAssignment `json:"partitions"`
}

// Region is a failure and placement domain.
type Region struct {
	ID               string     `json:"id"`
	Role             RegionRole `json:"role"`
	GatewayEndpoints []string   `json:"gateway_endpoints,omitempty"`
}

// Node is a Storage Node process inside a region.
type Node struct {
	ID            string `json:"id"`
	Region        string `json:"region"`
	Address       string `json:"address"`
	State         string `json:"state,omitempty"`
	CapacityBytes uint64 `json:"capacity_bytes,omitempty"`
	Zone          string `json:"zone,omitempty"`
}

// PartitionAssignment is the local Raft group for one partition in one region.
type PartitionAssignment struct {
	PartitionID    uint32   `json:"partition_id"`
	Region         string   `json:"region"`
	LeaderNodeID   string   `json:"leader_node_id"`
	ReplicaNodeIDs []string `json:"replica_node_ids"`
}

// Validate enforces the Control Plane configuration invariants. It is called
// before a snapshot is stored or applied, so invalid configurations never
// replace last-known-good state.
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
		if _, ok := replicas[a.LeaderNodeID]; !ok {
			return fmt.Errorf("%w: leader %q is not a replica in assignment (%s,%d)", ErrInvalidConfig, a.LeaderNodeID, a.Region, a.PartitionID)
		}
		coverage[a.Region][a.PartitionID] = true
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

// MasterRegion returns the single region allowed to accept client writes.
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

// RegionByID returns a region by id.
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

// NodeByID returns a node by id.
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

// Assignment returns the region-scoped assignment for a partition.
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

// Clone returns a deep copy. Immutable snapshots still cross package and
// goroutine boundaries, so callers that own ingress data clone before storing.
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
	out.Partitions = make([]PartitionAssignment, len(c.Partitions))
	for i := range c.Partitions {
		out.Partitions[i] = c.Partitions[i]
		out.Partitions[i].ReplicaNodeIDs = append([]string(nil), c.Partitions[i].ReplicaNodeIDs...)
	}
	return &out
}
