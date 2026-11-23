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
			changed, err := Validate(current, scheduler.Plan{BaseRevision: current.Revision, Proposed: next})
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
	request := scheduler.Plan{BaseRevision: current.Revision, Proposed: next}
	if changed, err := Validate(next, request); changed || err != nil {
		t.Fatalf("repeat = %v, %v", changed, err)
	}
	stale := current.Clone()
	stale.Revision = 42
	if _, err := Validate(stale, request); err == nil {
		t.Fatal("stale edit accepted")
	}
}
