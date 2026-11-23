// Package controller reconciles explicit static topology changes under an etcd lease.
package controller

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/scheduler"
	"google.golang.org/protobuf/proto"
)

// Validate checks whether the proposal can be published while keeping the
// provisioned Raft identities, voter sets and regional write authority.
func Validate(current *cpconfig.Config, plan scheduler.Plan) (bool, error) {
	if err := current.Validate(); err != nil {
		return false, err
	}
	if len(plan.Unsatisfied) > 0 {
		issue := plan.Unsatisfied[0]
		return false, fmt.Errorf("%s partition %d: %s (%d/%d replicas)", issue.Region, issue.Partition, issue.Reason, issue.Placed, issue.RF)
	}
	next := plan.Proposed
	if err := next.Validate(); err != nil {
		return false, err
	}
	if plan.BaseRevision == math.MaxUint64 || next.Revision != plan.BaseRevision+1 {
		return false, errors.New("proposed revision must follow the plan base revision")
	}
	if next.PartitionCount != current.PartitionCount {
		return false, cpconfig.ErrPartitionCountChanged
	}
	if next.MasterRegion() != current.MasterRegion() {
		return false, errors.New("changing the master role requires an agreed promotion and write fencing procedure")
	}
	for _, currentGroup := range current.Partitions {
		candidate, found := next.Assignment(currentGroup.Region, currentGroup.PartitionID)
		if !found || !sameMembers(currentGroup.ReplicaNodeIDs, candidate.ReplicaNodeIDs) ||
			candidate.LeaderNodeID != currentGroup.LeaderNodeID {
			return false, membershipRequired(currentGroup)
		}
		for _, id := range candidate.ReplicaNodeIDs {
			was, _ := current.NodeByID(id)
			will, _ := next.NodeByID(id)
			if was.Address != will.Address {
				return false, fmt.Errorf("%s partition %d node %s: endpoint change requires confirmation of Raft identity",
					candidate.Region, candidate.PartitionID, id)
			}
		}
	}
	for _, candidate := range next.Partitions {
		if _, found := current.Assignment(candidate.Region, candidate.PartitionID); !found {
			return false, membershipRequired(candidate)
		}
	}
	if sameStaticConfig(current, next) {
		return false, nil
	}
	if current.Revision != plan.BaseRevision {
		return false, fmt.Errorf("%w: plan base %d, published %d", cpconfig.ErrStaleRevision, plan.BaseRevision, current.Revision)
	}
	return true, nil
}

func membershipRequired(a cpconfig.PartitionAssignment) error {
	return fmt.Errorf("%s partition %d: apply requires confirmed Raft membership and readiness (#73/#80)", a.Region, a.PartitionID)
}

func sameMembers(a, b []string) bool {
	first, second := slices.Clone(a), slices.Clone(b)
	slices.Sort(first)
	slices.Sort(second)
	return slices.Equal(first, second)
}

func sameStaticConfig(a, b *cpconfig.Config) bool {
	return proto.Equal(staticConfig(a).ToProto(), staticConfig(b).ToProto())
}

func staticConfig(config *cpconfig.Config) *cpconfig.Config {
	c := config.Clone()
	c.Revision = 0
	slices.SortFunc(c.Regions, func(a, b cpconfig.Region) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(c.Nodes, func(a, b cpconfig.Node) int { return cmp.Compare(a.ID, b.ID) })
	slices.SortFunc(c.Partitions, func(a, b cpconfig.PartitionAssignment) int {
		if order := cmp.Compare(a.Region, b.Region); order != 0 {
			return order
		}
		return cmp.Compare(a.PartitionID, b.PartitionID)
	})
	for i := range c.Partitions {
		a := &c.Partitions[i]
		slices.Sort(a.ReplicaNodeIDs)
		a.Ready, a.Readable, a.Writable, a.LeaderConflict = false, false, false, false
		a.ObservedTerm = 0
		a.ReadyReplicaNodeIDs = nil
	}
	return c
}
