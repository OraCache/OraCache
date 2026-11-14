package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/OraCache/oracache/internal/cpconfig"
	"github.com/OraCache/oracache/internal/statestore"
)

func TestStatusHandlerShowsLastHeartbeats(t *testing.T) {
	ctx := context.Background()
	store := statestore.NewMemory()
	cfg := &cpconfig.Config{
		Revision: 1, PartitionCount: 1,
		Regions:    []cpconfig.Region{{ID: "r", Role: cpconfig.RegionRoleMaster}},
		Nodes:      []cpconfig.Node{{ID: "n1", Region: "r", Address: "a"}, {ID: "n2", Region: "r", Address: "b"}},
		Partitions: []cpconfig.PartitionAssignment{{PartitionID: 0, Region: "r", LeaderNodeID: "n1", ReplicaNodeIDs: []string{"n1", "n2"}}},
	}
	if err := store.ApplyConfig(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertHeartbeat(ctx, statestore.Heartbeat{NodeID: "n2", Region: "r", Partitions: []statestore.PartitionStatus{
		{PartitionID: 0, Role: statestore.RaftRoleLeader, Term: 3, LeaderID: "n2", Ready: true},
	}}); err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	statusHandler(store).ServeHTTP(rec, httptest.NewRequest("GET", "/heartbeats", strings.NewReader("")))
	var got struct {
		ConfigRevision uint64 `json:"config_revision"`
		Nodes          []struct {
			NodeID     string `json:"node_id"`
			Partitions []struct {
				Role   string
				Leader string `json:"leader_id"`
				Term   uint64
			}
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("%v: %s", err, rec.Body)
	}
	if got.ConfigRevision != 1 || len(got.Nodes) != 1 || got.Nodes[0].NodeID != "n2" ||
		got.Nodes[0].Partitions[0].Role != "leader" || got.Nodes[0].Partitions[0].Leader != "n2" || got.Nodes[0].Partitions[0].Term != 3 {
		t.Fatalf("unexpected view: %s", rec.Body)
	}
}
