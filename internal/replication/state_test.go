package replication

import (
	"fmt"
	pb "github.com/OraCache/oracache/gen/replication/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"math"
	"testing"
)

func TestNewState(t *testing.T) {
	for _, tt := range []struct {
		name      string
		rc        Config
		wantError bool
	}{
		{name: "disabled"},
		{name: "source defaults", rc: Config{TargetRegion: "b"}},
		{name: "source explicit limit", rc: Config{TargetRegion: "b", OutboxLimit: 2}},
		{name: "destination", rc: Config{SourceRegion: "b", SourceGroupID: "b/0/1"}},
		{name: "negative limit", rc: Config{OutboxLimit: -1}, wantError: true},
		{name: "target is own region", rc: Config{TargetRegion: "a"}, wantError: true},
		{name: "both directions", rc: Config{TargetRegion: "b", SourceRegion: "c", SourceGroupID: "c/0/1"}, wantError: true},
		{name: "outgoing with source group", rc: Config{TargetRegion: "b", SourceGroupID: "b/0/1"}, wantError: true},
		{name: "missing source group", rc: Config{SourceRegion: "b"}, wantError: true},
		{name: "missing source region", rc: Config{SourceGroupID: "b/0/1"}, wantError: true},
		{name: "source is own region", rc: Config{SourceRegion: "a", SourceGroupID: "a/0/other"}, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := StateOptions{Region: "a", ShardID: 3, GroupID: "a/3/1", Config: tt.rc}
			state, err := NewState(cfg)
			if (err != nil) != tt.wantError {
				t.Fatalf("unexpected validation result: %v", err)
			}
			if tt.wantError {
				return
			}
			wantConfig := tt.rc
			if wantConfig.OutboxLimit == 0 {
				wantConfig.OutboxLimit = 10000
			}
			if state.data.Identity.Config != wantConfig {
				t.Fatalf("config = %+v, want %+v", state.data.Identity.Config, wantConfig)
			}
			if state.data.NextID != 0 || state.data.AckedID != 0 || state.data.AppliedID != 0 || len(state.data.Outbox) != 0 {
				t.Fatal("new replication state is not empty")
			}
			if tt.rc.TargetRegion != "" || tt.rc.SourceRegion != "" {
				if state.data.Identity.Region != cfg.Region || state.data.Identity.ShardID != cfg.ShardID || state.data.Identity.GroupID != cfg.GroupID {
					t.Fatalf("wrong identity: %+v", state.data.Identity)
				}
			}
		})
	}
}

func TestStateQueueOwnsEventsAndChecksCapacity(t *testing.T) {
	s, err := NewState(StateOptions{Region: "a", GroupID: "a/0/1", Config: Config{TargetRegion: "b", OutboxLimit: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Enqueue("k"); err != nil {
		t.Fatal(err)
	}
	entry, err := s.Peek()
	if err != nil {
		t.Fatal(err)
	}
	entry.Id, entry.Key = 99, "changed"
	if err := s.Enqueue("overflow"); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if err := s.Acknowledge(2); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
	entry, err = s.Peek()
	if err != nil || entry.Id != 1 || entry.Key != "k" {
		t.Fatal(entry, err)
	}
	data, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := s.Restored(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Acknowledge(1); err != nil {
		t.Fatal(err)
	}
	if err := restored.Acknowledge(1); err != nil {
		t.Fatal(err)
	}
	entry, err = s.Peek()
	if err != nil || entry == nil {
		t.Fatal("restored state shares queue with original", err)
	}
	if err := restored.Enqueue("next"); err != nil {
		t.Fatal(err)
	}
	entry, err = restored.Peek()
	if err != nil || entry.Id != 2 {
		t.Fatal(entry, err)
	}
	// Even an empty, fully acknowledged queue cannot wrap its sequence to zero.
	if err := restored.Acknowledge(2); err != nil {
		t.Fatal(err)
	}
	restored.data.NextID = math.MaxUint64
	restored.data.AckedID = math.MaxUint64
	if err := restored.Enqueue("wrap"); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
}

func TestStateApplyAdvancesOnlyAfterSuccessfulCallback(t *testing.T) {
	s, err := NewState(StateOptions{Region: "b", GroupID: "b/0/1", Config: Config{SourceRegion: "a", SourceGroupID: "a/0/1"}})
	if err != nil {
		t.Fatal(err)
	}
	req := &pb.ReplicateRequest{SourceRegion: "a", TargetRegion: "b", StreamId: "a/0/1", Entry: &pb.Entry{Id: 1, Key: "k"}}
	before, err := s.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(req, func(*pb.Entry) error { return fmt.Errorf("store refused replacement") }); err == nil {
		t.Fatal("lost callback error")
	}
	after, err := s.Checkpoint()
	if err != nil || string(before) != string(after) {
		t.Fatal("failed apply changed position", err)
	}
	calls := 0
	apply := func(*pb.Entry) error { calls++; return nil }
	response, err := s.Apply(req, apply)
	if err != nil || response.GetAppliedId() != 1 {
		t.Fatal(response, err)
	}
	response, err = s.Apply(req, apply)
	if err != nil || response.GetAppliedId() != 1 || calls != 1 {
		t.Fatal("duplicate reapplied", response, err, calls)
	}
	req.Entry.Id = 3
	if _, err := s.Apply(req, apply); status.Code(err) != codes.FailedPrecondition || calls != 1 {
		t.Fatal("gap reached callback", err, calls)
	}
}
