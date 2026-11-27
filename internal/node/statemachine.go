package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"github.com/OraCache/oracache/internal/replication"
	"github.com/OraCache/oracache/internal/store"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StateMachine adapts the versioned LRU store to shardraft.StateMachine.
//
// Determinism contract for replication: Raft serves Get on one replica only,
// while Put and Delete are applied on all replicas. Get therefore uses
// Store.Peek and never promotes a key. LRU order, and so eviction, is driven
// exclusively by replicated writes. Reading a key does not keep it alive; this
// is a documented PoC semantic (see docs/architecture).
//
// Revisions are the store's global sequence: strictly increasing and never
// reused, but not dense per key. expected_rev follows the protobuf contract
// (0 means "key must not exist").
type StateMachine struct {
	s             *store.Store
	replication   *replication.State
	outboxPending atomic.Int64
}

var _ shardraft.StateMachine = (*StateMachine)(nil)

// NewStateMachine wraps s. The store must be private to the Raft instance.
func NewStateMachine(s *store.Store, replication *replication.State) *StateMachine {
	return &StateMachine{s: s, replication: replication}
}

func (m *StateMachine) Get(_ context.Context, r *pb.GetRequest, _ ...grpc.CallOption) (*pb.GetResponse, error) {
	if r.GetKey() == "" {
		return nil, errEmptyKey
	}
	res := m.s.Peek(r.Key, r.Rev)
	switch res.Status {
	case store.GetHit:
		return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_HIT, Rev: res.Version.Rev, Data: res.Version.Data, LatestRev: res.LatestRev}, nil
	case store.GetVersionNotFound:
		return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_VERSION_NOT_FOUND, LatestRev: res.LatestRev}, nil
	default:
		return &pb.GetResponse{Status: pb.GetStatus_GET_STATUS_MISS}, nil
	}
}

func (m *StateMachine) Put(_ context.Context, r *pb.PutRequest, _ ...grpc.CallOption) (*pb.PutResponse, error) {
	if r.GetKey() == "" {
		return nil, errEmptyKey
	}
	// Store itself retains even an oversized last entry. The Raft adapter must
	// bound keys too, otherwise a single escaped key could exceed snapshot size.
	if int64(len(r.Key))+int64(len(r.Data))+96 > m.s.Config().MaxBytes {
		return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_TOO_LARGE}, nil
	}
	if err := m.replication.CanWrite(); err != nil {
		return nil, err
	}
	res, err := m.s.Put(r.Key, r.Data, r.ExpectedRev)
	if err != nil {
		if errors.Is(err, store.ErrRevisionsExhausted) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, err
	}
	switch res.Status {
	case store.PutOK:
		if err := m.replication.Enqueue(r.Key); err != nil {
			return nil, err
		}
		m.outboxPending.Store(int64(m.replication.Pending()))
		return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_OK, Rev: res.Rev}, nil
	case store.PutConflict:
		return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_CONFLICT, Rev: res.Rev}, nil
	default:
		return &pb.PutResponse{Status: pb.PutStatus_PUT_STATUS_TOO_LARGE}, nil
	}
}

func (m *StateMachine) Delete(_ context.Context, r *pb.DeleteRequest, _ ...grpc.CallOption) (*pb.DeleteResponse, error) {
	if r.GetKey() == "" {
		return nil, errEmptyKey
	}
	if err := m.replication.CanWrite(); err != nil {
		return nil, err
	}
	if m.s.Delete(r.Key, r.Rev) {
		if err := m.replication.Enqueue(r.Key); err != nil {
			return nil, err
		}
		m.outboxPending.Store(int64(m.replication.Pending()))
		return &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_DELETED}, nil
	}
	return &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_NOT_FOUND}, nil
}

type stateCheckpoint struct {
	Format      int
	Store       json.RawMessage
	Replication json.RawMessage
}

func (m *StateMachine) Checkpoint() ([]byte, error) {
	data, err := m.s.Checkpoint()
	if err != nil {
		return nil, err
	}
	if !m.replication.Configured() {
		return data, nil
	}
	replicationData, err := m.replication.Checkpoint()
	if err != nil {
		return nil, err
	}
	return json.Marshal(stateCheckpoint{Format: 2, Store: data, Replication: replicationData})
}

func (m *StateMachine) Restore(data []byte) error {
	if !m.replication.Configured() {
		return m.s.Restore(data)
	}
	var c stateCheckpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return err
	}
	if c.Format != 2 {
		return fmt.Errorf("node: unsupported checkpoint format %d", c.Format)
	}
	restored, err := m.replication.Restored(c.Replication)
	if err != nil {
		return err
	}
	if err := m.s.Restore(c.Store); err != nil {
		return err
	}
	m.replication = restored
	m.outboxPending.Store(int64(restored.Pending()))
	return nil
}

var errEmptyKey = status.Error(codes.InvalidArgument, "key must not be empty")
