package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	controlplanev1 "github.com/OraCache/oracache/gen/controlplane/v1"
	"github.com/OraCache/oracache/internal/partition"
)

func testProvider(t *testing.T, partitions, leaders string, count uint32) statusProvider {
	t.Helper()
	held, err := partition.ParseSpec(partitions, count)
	if err != nil {
		t.Fatalf("ParseSpec(%q): %v", partitions, err)
	}
	led, err := partition.ParseSpec(leaders, count)
	if err != nil {
		t.Fatalf("ParseSpec(%q): %v", leaders, err)
	}
	ledSet := make(map[uint32]struct{}, len(led))
	for _, id := range led {
		ledSet[id] = struct{}{}
	}
	return statusProvider{nodeID: "eu-1", region: "eu", held: held, led: ledSet, term: 4}
}

func TestStatusProviderReportsRegionAndShards(t *testing.T) {
	t.Parallel()
	provider := testProvider(t, "even", "all", 4)
	statuses, err := provider.PartitionStatuses(context.Background())
	if err != nil {
		t.Fatalf("PartitionStatuses: %v", err)
	}
	if len(statuses) != 2 || statuses[0].GetPartitionId() != 0 || statuses[1].GetPartitionId() != 2 {
		t.Fatalf("partitions = %v", statuses)
	}
	for _, status := range statuses {
		if status.GetRole() != controlplanev1.RaftRole_RAFT_ROLE_LEADER {
			t.Errorf("partition %d role = %v, want leader", status.GetPartitionId(), status.GetRole())
		}
		if status.GetLeaderId() != "eu-1" {
			t.Errorf("partition %d leader = %q, want eu-1", status.GetPartitionId(), status.GetLeaderId())
		}
		if !status.GetReady() || status.GetTerm() != 4 {
			t.Errorf("partition %d ready=%v term=%d", status.GetPartitionId(), status.GetReady(), status.GetTerm())
		}
	}
}

func TestStatusProviderFollowerSubset(t *testing.T) {
	t.Parallel()
	provider := testProvider(t, "all", "even", 4)
	statuses, err := provider.PartitionStatuses(context.Background())
	if err != nil {
		t.Fatalf("PartitionStatuses: %v", err)
	}
	roles := map[uint32]controlplanev1.RaftRole{}
	for _, status := range statuses {
		roles[status.GetPartitionId()] = status.GetRole()
	}
	if roles[0] != controlplanev1.RaftRole_RAFT_ROLE_LEADER {
		t.Errorf("partition 0 role = %v, want leader", roles[0])
	}
	if roles[1] != controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER {
		t.Errorf("partition 1 role = %v, want follower", roles[1])
	}
	for _, status := range statuses {
		if status.GetRole() == controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER && status.GetLeaderId() != "" {
			t.Errorf("follower partition %d must not claim leadership", status.GetPartitionId())
		}
	}
}

func TestServeStatusDisabled(t *testing.T) {
	t.Parallel()
	provider := testProvider(t, "all", "all", 4)
	stop := serveStatus("none", provider, "eu-1", "eu", slog.New(slog.NewTextHandler(io.Discard, nil)))
	stop() // повторный вызов тоже безопасен
}

func TestServeStatusEndpoints(t *testing.T) {
	t.Parallel()
	provider := testProvider(t, "all", "odd", 4)
	mux := http.NewServeMux()
	mux.HandleFunc("/partitions", func(w http.ResponseWriter, r *http.Request) {
		statuses, err := provider.PartitionStatuses(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"node_id": "eu-1", "region": "eu", "partitions": statuses})
	})

	request := httptest.NewRequest(http.MethodGet, "/partitions", nil)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	var payload struct {
		NodeID     string `json:"node_id"`
		Region     string `json:"region"`
		Partitions []struct {
			PartitionID uint32                  `json:"partition_id"`
			Role        controlplanev1.RaftRole `json:"role"`
			LeaderID    string                  `json:"leader_id"`
			Ready       bool                    `json:"ready"`
		} `json:"partitions"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.NodeID != "eu-1" || payload.Region != "eu" {
		t.Fatalf("identity = %s/%s, want eu-1/eu", payload.NodeID, payload.Region)
	}
	if len(payload.Partitions) != 4 {
		t.Fatalf("partitions = %d, want 4", len(payload.Partitions))
	}
	for _, status := range payload.Partitions {
		want := controlplanev1.RaftRole_RAFT_ROLE_FOLLOWER
		if status.PartitionID%2 == 1 {
			want = controlplanev1.RaftRole_RAFT_ROLE_LEADER
		}
		if status.Role != want {
			t.Errorf("partition %d role = %v, want %v", status.PartitionID, status.Role, want)
		}
		if !status.Ready {
			t.Errorf("partition %d must be ready", status.PartitionID)
		}
	}
}
