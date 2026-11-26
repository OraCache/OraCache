package node

import (
	"github.com/OraCache/oracache/internal/replication"
	"reflect"
	"strings"
	"testing"

	"github.com/OraCache/oracache/internal/cpagent"
	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/cpconfig/cpconfigtest"
)

func TestReplicationRoutesFollowCPPlacement(t *testing.T) {
	holder := cpconfig.NewHolder()
	n := &Node{cfg: Config{NodeID: "eu-1", Region: "eu", ShardID: 0, Replication: replication.Config{TargetRegion: "us"}}}
	var err error
	n.agent, err = cpagent.New(cpagent.Options{NodeID: "eu-1", Region: "eu", Client: noControlPlaneIO{}, Statuses: n, Holder: holder})
	if err != nil {
		t.Fatal(err)
	}
	if got := n.replicationAddresses(); len(got) != 0 {
		t.Fatal("must wait for CP", got)
	}
	c := cpconfigtest.Config(1)
	c.Nodes = append(c.Nodes, cpconfig.Node{ID: "us-other", Region: "us", Address: "other-shard:1234"})
	c.Partitions[3].ReplicaNodeIDs = append(c.Partitions[3].ReplicaNodeIDs, "us-other") // shard 1 only
	if err := holder.Apply(c); err != nil {
		t.Fatal(err)
	}
	if got, want := n.replicationAddresses(), []string{"10.0.1.1:7000", "10.0.1.2:7000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("routes %v, want %v", got, want)
	}
	next := c.Clone()
	next.Revision++
	next.Nodes[3].Address = "new-us-2:8000"
	next.Partitions[1].LeaderNodeID = "us-2"
	if err := holder.Apply(next); err != nil {
		t.Fatal(err)
	}
	if got, want := n.replicationAddresses(), []string{"new-us-2:8000", "10.0.1.1:7000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updated routes %v, want %v", got, want)
	}
	next = next.Clone()
	next.Revision++
	next.Nodes[3].State = cpconfig.NodeStateDraining
	if err := holder.Apply(next); err != nil {
		t.Fatal(err)
	}
	if got, want := n.replicationAddresses(), []string{"10.0.1.1:7000"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("draining routes %v, want %v", got, want)
	}
	n.cfg.ShardID = 99
	if got := n.replicationAddresses(); len(got) != 0 {
		t.Fatal("must not use another shard", got)
	}
	n.cfg.ShardID = 0
	next = next.Clone()
	next.Revision++
	next.Regions[0].Role, next.Regions[1].Role = cpconfig.RegionRoleSlave, cpconfig.RegionRoleMaster
	for i := range next.Partitions {
		next.Partitions[i].Writable = false
	}
	if err := holder.Apply(next); err != nil {
		t.Fatal(err)
	}
	if got := n.replicationAddresses(); len(got) != 0 {
		t.Fatal("old master must stop sending", got)
	}
}

func TestOutgoingReplicationRequiresControlPlane(t *testing.T) {
	_, err := New(Config{
		NodeID: "a-1", Region: "a", GroupID: "a/0/test",
		Replication: replication.Config{TargetRegion: "b"},
	})
	if err == nil || !strings.Contains(err.Error(), "outgoing replication requires Control Plane") {
		t.Fatalf("expected missing CP error, got %v", err)
	}
}
