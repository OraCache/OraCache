// Package scheduler computes static placement without observing or changing Raft.
package scheduler

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"

	"github.com/OraCache/oracache/internal/cpconfig"
)

type Policy struct {
	RF            uint32            `json:"rf"`
	DistinctZones bool              `json:"distinct_zones,omitempty"`
	ReplicaBytes  uint64            `json:"replica_bytes,omitempty"`
	MaxReplicas   map[string]uint32 `json:"max_replicas,omitempty"`
}

type Input struct {
	Regions []cpconfig.Region `json:"regions,omitempty"`
	Nodes   []cpconfig.Node   `json:"nodes,omitempty"`
	Policy  map[string]Policy `json:"policy"`
}

type Change struct {
	Region    string   `json:"region"`
	Partition uint32   `json:"partition"`
	Before    []string `json:"before,omitempty"`
	After     []string `json:"after,omitempty"`
	Reason    string   `json:"reason"`
}

type Constraint struct {
	Region    string `json:"region"`
	Partition uint32 `json:"partition"`
	RF        uint32 `json:"rf"`
	Placed    int    `json:"placed"`
	Reason    string `json:"reason"`
}

type Plan struct {
	Policy        map[string]Policy `json:"policy"`
	BaseRevision  uint64            `json:"base_revision"`
	Proposed      *cpconfig.Config  `json:"proposed"`
	Changes       []Change          `json:"changes"`
	StaticChanges []string          `json:"static_changes"`
	Unsatisfied   []Constraint      `json:"unsatisfied"`
}

// Calculate maximizes feasible assignments first, then retained replicas.
func Calculate(current *cpconfig.Config, input Input) (Plan, error) {
	if err := current.Validate(); err != nil {
		return Plan{}, err
	}
	if current.Revision == math.MaxUint64 {
		return Plan{}, errors.New("configuration revision exhausted")
	}
	if input.Nodes == nil {
		input.Nodes = current.Nodes
	}
	if input.Regions == nil {
		input.Regions = current.Regions
	}
	proposed := (&cpconfig.Config{
		Revision: current.Revision + 1, PartitionCount: current.PartitionCount,
		Nodes: input.Nodes, Regions: input.Regions,
	}).Clone()
	slices.SortFunc(proposed.Regions, func(a, b cpconfig.Region) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(proposed.Nodes, func(a, b cpconfig.Node) int { return cmp.Compare(a.ID, b.ID) })
	plan := Plan{BaseRevision: current.Revision, Proposed: proposed, Policy: maps.Clone(input.Policy)}
	for region, policy := range plan.Policy {
		policy.MaxReplicas = maps.Clone(policy.MaxReplicas)
		plan.Policy[region] = policy
	}
	if err := validateInventory(proposed, input.Policy); err != nil {
		return Plan{}, err
	}
	if !reflect.DeepEqual(proposed.Regions, sortedRegions(current.Regions)) {
		plan.StaticChanges = append(plan.StaticChanges, "update registered regions")
	}
	if !reflect.DeepEqual(proposed.Nodes, sortedNodes(current.Nodes)) {
		plan.StaticChanges = append(plan.StaticChanges, "update registered nodes")
	}
	for _, region := range proposed.Regions {
		nodes := slices.DeleteFunc(slices.Clone(proposed.Nodes), func(n cpconfig.Node) bool {
			return n.Region != region.ID || n.State == cpconfig.NodeStateDraining
		})
		policy := input.Policy[region.ID]
		previous := make([]cpconfig.PartitionAssignment, proposed.PartitionCount)
		for partition := range previous {
			previous[partition], _ = current.Assignment(region.ID, uint32(partition))
		}
		placements, reason := place(nodes, previous, policy)
		for i, replicas := range placements {
			partition := uint32(i)
			if len(replicas) != int(policy.RF) {
				plan.Unsatisfied = append(plan.Unsatisfied, Constraint{Region: region.ID, Partition: partition, RF: policy.RF, Placed: len(replicas), Reason: reason})
			}
			leader := previous[i].LeaderNodeID
			if !slices.Contains(replicas, leader) && len(replicas) > 0 {
				leader = replicas[0]
			}
			proposed.Partitions = append(proposed.Partitions, cpconfig.PartitionAssignment{
				Region: region.ID, PartitionID: partition, LeaderNodeID: leader, ReplicaNodeIDs: replicas,
			})
			before := slices.Clone(previous[i].ReplicaNodeIDs)
			slices.Sort(before)
			if !slices.Equal(before, replicas) {
				explanation := "satisfy replication factor and placement constraints"
				if len(replicas) != int(policy.RF) {
					explanation = reason
				}
				plan.Changes = append(plan.Changes, Change{Region: region.ID, Partition: partition, Before: before, After: replicas, Reason: explanation})
			}
		}
	}
	for _, a := range current.Partitions {
		if _, found := proposed.RegionByID(a.Region); !found {
			before := slices.Clone(a.ReplicaNodeIDs)
			slices.Sort(before)
			plan.Changes = append(plan.Changes, Change{Region: a.Region, Partition: a.PartitionID, Before: before, Reason: "region removed from inventory"})
		}
	}
	slices.SortFunc(plan.Changes, func(a, b Change) int {
		if order := cmp.Compare(a.Region, b.Region); order != 0 {
			return order
		}
		return cmp.Compare(a.Partition, b.Partition)
	})
	if len(plan.Unsatisfied) == 0 {
		if err := proposed.Validate(); err != nil {
			return Plan{}, err
		}
	}
	return plan, nil
}

func sortedRegions(input []cpconfig.Region) []cpconfig.Region {
	result := slices.Clone(input)
	slices.SortFunc(result, func(a, b cpconfig.Region) int { return cmp.Compare(a.ID, b.ID) })
	return result
}

func sortedNodes(input []cpconfig.Node) []cpconfig.Node {
	result := slices.Clone(input)
	slices.SortFunc(result, func(a, b cpconfig.Node) int { return cmp.Compare(a.ID, b.ID) })
	return result
}

func validateInventory(config *cpconfig.Config, policies map[string]Policy) error {
	// The full snapshot validator also requires placement. Validate the inventory
	// here, including empty regions, so the planner can report missing capacity.
	masters := 0
	seenRegions := make(map[string]bool)
	for _, region := range config.Regions {
		if region.ID == "" || seenRegions[region.ID] || !region.Role.Valid() {
			return fmt.Errorf("invalid or duplicate region %q", region.ID)
		}
		seenRegions[region.ID] = true
		if region.Role == cpconfig.RegionRoleMaster {
			masters++
		}
		if policies[region.ID].RF == 0 {
			return fmt.Errorf("region %q requires a positive RF", region.ID)
		}
	}
	if masters != 1 || len(policies) != len(seenRegions) {
		return errors.New("inventory requires one master and one policy per region")
	}
	seenNodes := make(map[string]bool)
	for _, node := range config.Nodes {
		if node.ID == "" || seenNodes[node.ID] || !seenRegions[node.Region] || node.Address == "" {
			return fmt.Errorf("invalid or duplicate node %q", node.ID)
		}
		switch node.State {
		case "", cpconfig.NodeStateStarting, cpconfig.NodeStateReady, cpconfig.NodeStateDead, cpconfig.NodeStateDraining:
		default:
			return fmt.Errorf("node %q has invalid state", node.ID)
		}
		seenNodes[node.ID] = true
	}
	for region, policy := range policies {
		for node := range policy.MaxReplicas {
			if registered, ok := config.NodeByID(node); !ok || registered.Region != region {
				return fmt.Errorf("capacity limit for unregistered node %q in %q", node, region)
			}
		}
	}
	return nil
}
