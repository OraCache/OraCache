package scheduler_test

import (
	"reflect"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
	"github.com/OraCache/oracache/internal/scheduler"
)

func TestPlanPreservesFeasiblePlacement(t *testing.T) {
	current := cpconfigtest.Config(7)
	input := scheduler.Input{Regions: current.Regions, Nodes: current.Nodes,
		Policy: map[string]scheduler.Policy{"eu": {RF: 2, DistinctZones: true}, "us": {RF: 2, DistinctZones: true}}}
	first, err := scheduler.Calculate(current, input)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Changes) != 0 || len(first.Unsatisfied) != 0 {
		t.Fatalf("unexpected placement change: %+v", first)
	}
	input.Nodes = append([]cpconfig.Node(nil), input.Nodes...)
	input.Nodes[0], input.Nodes[3] = input.Nodes[3], input.Nodes[0]
	second, err := scheduler.Calculate(current, input)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatalf("plan changed with inventory order: %v", err)
	}
}

func TestPlanReportsRegionAndPartitionWhenConstraintsCannotBeMet(t *testing.T) {
	current := cpconfigtest.Config(7)
	tests := []struct {
		name, reason string
		policy       scheduler.Policy
		sameZone     bool
	}{
		{"nodes", "insufficient_nodes", scheduler.Policy{RF: 3}, false},
		{"zones", "insufficient_zones", scheduler.Policy{RF: 2, DistinctZones: true}, true},
		{"capacity", "insufficient_capacity", scheduler.Policy{RF: 2, MaxReplicas: map[string]uint32{"eu-1": 1}}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := scheduler.Input{Regions: current.Regions, Nodes: append([]cpconfig.Node(nil), current.Nodes...),
				Policy: map[string]scheduler.Policy{"eu": test.policy, "us": {RF: 2}}}
			if test.sameZone {
				input.Nodes[1].Zone = input.Nodes[0].Zone
			}
			plan, err := scheduler.Calculate(current, input)
			if err != nil {
				t.Fatal(err)
			}
			if len(plan.Unsatisfied) == 0 {
				t.Fatal("plan unexpectedly feasible")
			}
			for _, failure := range plan.Unsatisfied {
				if failure.Region != "eu" || failure.Reason != test.reason || failure.Partition >= current.PartitionCount {
					t.Fatalf("unexpected failure: %+v", failure)
				}
			}
		})
	}
}
