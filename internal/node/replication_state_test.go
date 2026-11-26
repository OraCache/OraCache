package node

import (
	"context"
	"encoding/json"
	"testing"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	rpb "github.com/OraCache/oracache/gen/replication/v1"
	"github.com/OraCache/oracache/internal/replication"
	"github.com/OraCache/oracache/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func replicationMachine(t *testing.T, source bool, limit int) *StateMachine {
	t.Helper()
	s, err := store.New(store.Config{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Region: "b", GroupID: "b/0/1", Replication: replication.Config{SourceRegion: "a", SourceGroupID: "a/0/1", OutboxLimit: limit}}
	if source {
		cfg = Config{Region: "a", GroupID: "a/0/1", Replication: replication.Config{TargetRegion: "b", OutboxLimit: limit}}
	}
	state, err := replication.NewState(replication.StateOptions{
		Region: cfg.Region, GroupID: cfg.GroupID, Config: cfg.Replication,
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewStateMachine(s, state)
}

func TestOutboxReferencesCurrentStateAndAcknowledgesOnlyOneID(t *testing.T) {
	ctx := context.Background()
	m := replicationMachine(t, true, 10)
	one, err := m.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	old, _ := m.ReadOutbox()
	two, err := m.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("two")})
	if err != nil {
		t.Fatal(err)
	}
	if string(old.Entry.Versions[0].Data) != "one" {
		t.Fatal("read returned shared storage")
	}
	current, _ := m.ReadOutbox()
	if len(current.Entry.Versions) != 2 || current.Entry.Versions[0].Rev != one.Rev || current.Entry.Versions[1].Rev != two.Rev {
		t.Fatalf("wrong versions: %v", current)
	}
	if _, err := m.AckOutbox(&rpb.AckOutboxRequest{Id: 2}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("accepted out-of-order ACK", err)
	}
	if _, err := m.AckOutbox(&rpb.AckOutboxRequest{Id: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.AckOutbox(&rpb.AckOutboxRequest{Id: 1}); err != nil {
		t.Fatal("ACK retry", err)
	}
	next, _ := m.ReadOutbox()
	if next.Entry.GetId() != 2 {
		t.Fatal("ACK erased a later change", next)
	}
	if _, err := m.Delete(ctx, &pb.DeleteRequest{Key: "k"}); err != nil {
		t.Fatal(err)
	}
	absent, _ := m.ReadOutbox()
	if len(absent.Entry.Versions) != 0 {
		t.Fatal("deleted key must be sent as empty set")
	}
}

func TestOutboxCapacityAndOnlySuccessfulChanges(t *testing.T) {
	ctx := context.Background()
	m := replicationMachine(t, true, 1)
	expected := uint64(99)
	conflict, err := m.Put(ctx, &pb.PutRequest{Key: "k", ExpectedRev: &expected})
	if err != nil || conflict.Status != pb.PutStatus_PUT_STATUS_CONFLICT || hasPending(t, m) {
		t.Fatal(conflict, err)
	}
	if _, err := m.Delete(ctx, &pb.DeleteRequest{Key: "absent"}); err != nil || hasPending(t, m) {
		t.Fatal(err)
	}
	first, err := m.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("lost")}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if _, err := m.Delete(ctx, &pb.DeleteRequest{Key: "k"}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if v := m.s.Peek("k", 0); v.LatestRev != first.Rev || string(v.Version.Data) != "one" {
		t.Fatal("full outbox mutated storage", v)
	}
	if _, err := m.AckOutbox(&rpb.AckOutboxRequest{Id: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Delete(ctx, &pb.DeleteRequest{Key: "k"}); err != nil {
		t.Fatal(err)
	}
	pending, err := m.ReadOutbox()
	if err != nil || pending.Entry.GetId() != 2 {
		t.Fatal("IDs must increase on deletion")
	}
}

func TestPendingEvictedKeyIsSentAsDeletion(t *testing.T) {
	ctx := context.Background()
	m := replicationMachine(t, true, 10)
	s, err := store.New(store.Config{MaxValueBytes: 16, MaxBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	m.s = s
	if _, err := m.Put(ctx, &pb.PutRequest{Key: "first", Data: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Put(ctx, &pb.PutRequest{Key: "second", Data: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if s.Peek("first", 0).Status != store.GetMiss {
		t.Fatal("test did not evict first key")
	}
	pending, err := m.ReadOutbox()
	if err != nil || pending.Entry.Key != "first" || len(pending.Entry.Versions) != 0 {
		t.Fatal("evicted key must be replicated as deletion", pending, err)
	}
}

func incoming(id uint64, versions ...*rpb.Version) *rpb.ReplicateRequest {
	return &rpb.ReplicateRequest{SourceRegion: "a", TargetRegion: "b", StreamId: "a/0/1", Entry: &rpb.Entry{Id: id, Key: "k", Versions: versions}}
}

func TestReceiverDeduplicationDeletionAndCheckpoint(t *testing.T) {
	ctx := context.Background()
	m := replicationMachine(t, false, 10)
	if _, err := m.Put(ctx, &pb.PutRequest{Key: "k"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("replica accepted client write", err)
	}
	req := incoming(1, &rpb.Version{Rev: 42, Data: []byte("one")}, &rpb.Version{Rev: 49, Data: []byte("two")})
	if _, err := m.Replicate(req); err != nil {
		t.Fatal(err)
	}
	if got := m.s.Peek("k", 42); string(got.Version.Data) != "one" || got.LatestRev != 49 {
		t.Fatal(got)
	}
	if _, err := m.Replicate(incoming(3)); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("accepted gap", err)
	}
	wrong := incoming(2)
	wrong.StreamId = "new-stream"
	if _, err := m.Replicate(wrong); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("accepted wrong stream", err)
	}
	wrong = incoming(2)
	wrong.ShardId = 1
	if _, err := m.Replicate(wrong); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("accepted wrong shard", err)
	}
	if _, err := m.Replicate(incoming(2)); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := m.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored := replicationMachine(t, false, 10)
	if err := restored.Restore(checkpoint); err != nil {
		t.Fatal(err)
	}
	ack, err := restored.Replicate(req)
	if err != nil || ack.AppliedId != 2 || restored.s.Peek("k", 0).Status != store.GetMiss {
		t.Fatal("retry resurrected deletion", ack, err)
	}
	if _, err := restored.Replicate(incoming(3, &rpb.Version{Rev: 55, Data: []byte("three")})); err != nil {
		t.Fatal(err)
	}
	if restored.s.Peek("k", 0).LatestRev != 55 || hasPending(t, restored) {
		t.Fatal("incoming generated outbox or changed revision")
	}
}

func TestSourceCheckpointIncludesQueueAndCounter(t *testing.T) {
	ctx := context.Background()
	m := replicationMachine(t, true, 10)
	_, _ = m.Put(ctx, &pb.PutRequest{Key: "k", Data: []byte("one")})
	_, _ = m.Put(ctx, &pb.PutRequest{Key: "other", Data: []byte("two")})
	_, _ = m.AckOutbox(&rpb.AckOutboxRequest{Id: 1})
	data, err := m.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored := replicationMachine(t, true, 10)
	if err := restored.Restore(data); err != nil {
		t.Fatal(err)
	}
	next, _ := restored.ReadOutbox()
	if next.Entry.GetId() != 2 || next.Entry.Key != "other" {
		t.Fatal(next)
	}
	_, _ = restored.Put(ctx, &pb.PutRequest{Key: "new"})
	next, _ = restored.ReadOutbox()
	if _, err := restored.AckOutbox(&rpb.AckOutboxRequest{Id: next.Entry.GetId()}); err != nil {
		t.Fatal(err)
	}
	next, _ = restored.ReadOutbox()
	if next.Entry.GetId() != 3 {
		t.Fatal("sequence was reused")
	}
	before, _ := restored.Checkpoint()
	var corrupt stateCheckpoint
	_ = json.Unmarshal(data, &corrupt)
	var stream map[string]json.RawMessage
	if err := json.Unmarshal(corrupt.Replication, &stream); err != nil {
		t.Fatal(err)
	}
	stream["Outbox"] = json.RawMessage(`[{"ID":100,"Key":"other"}]`)
	corrupt.Replication, _ = json.Marshal(stream)
	broken, _ := json.Marshal(corrupt)
	if err := restored.Restore(broken); err == nil {
		t.Fatal("accepted broken checkpoint")
	}
	after, _ := restored.Checkpoint()
	if string(before) != string(after) {
		t.Fatal("invalid restore changed state")
	}
}

func TestReplicationCheckpointRejectsOldFormatAndDifferentConfig(t *testing.T) {
	m := replicationMachine(t, true, 10)
	_, err := m.Put(context.Background(), &pb.PutRequest{Key: "k", Data: []byte("one")})
	if err != nil {
		t.Fatal(err)
	}
	data, err := m.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint stateCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		t.Fatal(err)
	}
	var saved struct {
		Identity struct{ Config replication.Config }
	}
	if err := json.Unmarshal(checkpoint.Replication, &saved); err != nil {
		t.Fatal(err)
	}
	if saved.Identity.Config != m.replication.Config() {
		t.Fatal("checkpoint lost replication config")
	}
	checkpoint.Format = 1
	oldFormat, err := json.Marshal(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := replicationMachine(t, true, 10).Restore(oldFormat); err == nil {
		t.Fatal("accepted old checkpoint format")
	}
	restored := replicationMachine(t, true, 10)
	cfg := restored.replication.Config()
	cfg.TargetRegion = "c"
	restored.replication, err = replication.NewState(replication.StateOptions{
		Region: "a", GroupID: "a/0/1", Config: cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(data); err == nil {
		t.Fatal("accepted checkpoint for a different target region")
	}
	if restored.s.Peek("k", 0).Status != store.GetMiss {
		t.Fatal("rejected checkpoint changed store")
	}
}

// hasPending observes the queue through the same API used by the sender.
func hasPending(t *testing.T, m *StateMachine) bool {
	t.Helper()
	if m.replication.Config().TargetRegion == "" {
		return false
	}
	out, err := m.ReadOutbox()
	if err != nil {
		t.Fatal(err)
	}
	return out.Entry != nil
}

func TestDisabledReplicationKeepsStoreCheckpoint(t *testing.T) {
	s, err := store.New(store.Config{})
	if err != nil {
		t.Fatal(err)
	}
	state, err := replication.NewState(replication.StateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sm := NewStateMachine(s, state)
	data, err := sm.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Restore(data); err != nil {
		t.Fatal(err)
	}
}

func TestFailedStoreRestoreDoesNotInstallReplicationPosition(t *testing.T) {
	m := replicationMachine(t, false, 10)
	if _, err := m.Replicate(incoming(1, &rpb.Version{Rev: 42, Data: []byte("one")})); err != nil {
		t.Fatal(err)
	}
	data, err := m.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored := replicationMachine(t, false, 10)
	before, err := restored.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	var snapshot stateCheckpoint
	if err := json.Unmarshal(data, &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.Store = json.RawMessage(`{}`)
	broken, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(broken); err == nil {
		t.Fatal("accepted invalid store snapshot")
	}
	after, err := restored.Checkpoint()
	if err != nil || string(before) != string(after) {
		t.Fatal("failed restore changed state", err)
	}
	// Event 1 must still be applied, rather than being mistaken for a duplicate.
	if _, err := restored.Replicate(incoming(1, &rpb.Version{Rev: 42, Data: []byte("one")})); err != nil {
		t.Fatal(err)
	}
	if got := restored.s.Peek("k", 42); got.Status != store.GetHit {
		t.Fatal(got)
	}
}
