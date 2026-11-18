package node

import (
	"context"
	"errors"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
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
type StateMachine struct{ s *store.Store }

var _ shardraft.StateMachine = (*StateMachine)(nil)

// NewStateMachine wraps s. The store must be private to the Raft instance.
func NewStateMachine(s *store.Store) *StateMachine { return &StateMachine{s: s} }

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
	res, err := m.s.Put(r.Key, r.Data, r.ExpectedRev)
	if err != nil {
		if errors.Is(err, store.ErrRevisionsExhausted) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		return nil, err
	}
	switch res.Status {
	case store.PutOK:
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
	if m.s.Delete(r.Key, r.Rev) {
		return &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_DELETED}, nil
	}
	return &pb.DeleteResponse{Status: pb.DeleteStatus_DELETE_STATUS_NOT_FOUND}, nil
}

func (m *StateMachine) Checkpoint() ([]byte, error) { return m.s.Checkpoint() }
func (m *StateMachine) Restore(b []byte) error      { return m.s.Restore(b) }

var errEmptyKey = status.Error(codes.InvalidArgument, "key must not be empty")
