package node

import (
	pb "github.com/OraCache/oracache/gen/replication/v1"
	"github.com/OraCache/oracache/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (m *StateMachine) ReadOutbox() (*pb.ReadOutboxResponse, error) {
	entry, err := m.replication.Peek()
	if err != nil {
		return nil, err
	}
	if entry != nil {
		for _, v := range m.s.Versions(entry.Key) {
			entry.Versions = append(entry.Versions, &pb.Version{Rev: v.Rev, Data: v.Data})
		}
	}
	return &pb.ReadOutboxResponse{Entry: entry}, nil
}

func (m *StateMachine) AckOutbox(req *pb.AckOutboxRequest) (*pb.AckOutboxResponse, error) {
	if err := m.replication.Acknowledge(req.Id); err != nil {
		return nil, err
	}
	return &pb.AckOutboxResponse{}, nil
}

func (m *StateMachine) Replicate(req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	return m.replication.Apply(req, func(entry *pb.Entry) error {
		versions := make([]store.Version, 0, len(entry.Versions))
		for _, v := range entry.Versions {
			if v == nil {
				return status.Error(codes.InvalidArgument, "nil version")
			}
			versions = append(versions, store.Version{Rev: v.Rev, Data: v.Data})
		}
		if err := m.s.ReplaceVersions(entry.Key, versions); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return nil
	})
}
