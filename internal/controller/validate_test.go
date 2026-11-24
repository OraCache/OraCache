package controller

import (
	"strings"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/scheduler"
)

func TestValidatesStaticPublication(t *testing.T) {
	current := cpconfigtest.Config(40)
	for _, test := range []struct {
		name, want string
		edit       func(*cpconfig.Config)
	}{
		{"metadata", "", func(next *cpconfig.Config) { next.Nodes[0].CapacityBytes = 4096 }},
		{"membership", "confirmed Raft membership", func(next *cpconfig.Config) {
			next.Partitions[0].ReplicaNodeIDs = []string{"eu-1"}
			next.Partitions[0].ReadyReplicaNodeIDs = nil
		}},
		{"master", "master role", func(next *cpconfig.Config) {
			next.Regions[0].Role, next.Regions[1].Role = next.Regions[1].Role, next.Regions[0].Role
			for i := range next.Partitions {
				next.Partitions[i].Writable = false
			}
		}},
		{"endpoint", "Raft identity", func(next *cpconfig.Config) { next.Nodes[0].Address = "other:8080" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			next := current.Clone()
			next.Revision++
			test.edit(next)
			changed, err := Validate(current, scheduler.Plan{BaseRevision: current.Revision, Proposed: next, Policy: fixturePolicy()})
			if test.want == "" {
				if err != nil || !changed {
					t.Fatalf("validate change = %v, %v", changed, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("expected %s, got %v", test.want, err)
			}
		})
	}
}

func TestIdenticalIntentSkipsRevisionAfterSuccess(t *testing.T) {
	current := cpconfigtest.Config(40)
	next := current.Clone()
	next.Revision++
	next.Nodes[0].CapacityBytes = 20
	request := scheduler.Plan{BaseRevision: current.Revision, Proposed: next, Policy: fixturePolicy()}
	if changed, err := Validate(next, request); changed || err != nil {
		t.Fatalf("repeat = %v, %v", changed, err)
	}
	stale := current.Clone()
	stale.Revision = 42
	if _, err := Validate(stale, request); err == nil {
		t.Fatal("stale edit accepted")
	}
}

func fixturePolicy() map[string]scheduler.Policy {
	return map[string]scheduler.Policy{"eu": {RF: 2}, "us": {RF: 2}}
}

func TestValidateRechecksPlacementPolicyAndProtectsAssignedVoters(t *testing.T) {
	current := cpconfigtest.Config(3)
	for _, scenario := range []struct {
		name, want string
		edit       func(*scheduler.Plan)
	}{
		{"zone", "insufficient_zones", func(plan *scheduler.Plan) {
			plan.Policy["eu"] = scheduler.Policy{RF: 2, DistinctZones: true}
			plan.Proposed.Nodes[1].Zone = plan.Proposed.Nodes[0].Zone
		}},
		{"capacity", "insufficient_capacity", func(plan *scheduler.Plan) {
			plan.Policy["eu"] = scheduler.Policy{RF: 2, MaxReplicas: map[string]uint32{"eu-1": 1}}
		}},
		{"drain", "changing drain status", func(plan *scheduler.Plan) { plan.Proposed.Nodes[0].State = cpconfig.NodeStateDraining }},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			next := current.Clone()
			next.Revision++
			plan := scheduler.Plan{BaseRevision: current.Revision, Proposed: next, Policy: fixturePolicy()}
			scenario.edit(&plan)
			if _, err := Validate(current, plan); err == nil || !strings.Contains(err.Error(), scenario.want) {
				t.Fatalf("unsafe change accepted or wrong reason: %v", err)
			}
		})
	}
}
