package shardraft

import (
	"context"
	pb "github.com/OraCache/oracache/gen/replication/v1"
)

// ReplicationStateMachine is optional. All calls run on the Raft owner loop.
// ReadOutbox uses ReadIndex even when public reads are configured as Local.
type ReplicationStateMachine interface {
	ReadOutbox() (*pb.ReadOutboxResponse, error)
	AckOutbox(*pb.AckOutboxRequest) (*pb.AckOutboxResponse, error)
	Replicate(*pb.ReplicateRequest) (*pb.ReplicateResponse, error)
}

func (n *Instance) ReadOutbox(ctx context.Context) (*pb.ReadOutboxResponse, error) {
	out, err := n.call(ctx, opReadOutbox, &pb.ReadOutboxRequest{}, nil)
	if err != nil {
		return nil, err
	}
	return out.(*pb.ReadOutboxResponse), nil
}

func (n *Instance) AckOutbox(ctx context.Context, id uint64) error {
	_, err := n.call(ctx, opAckOutbox, &pb.AckOutboxRequest{Id: id}, nil)
	return err
}

func (n *Instance) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	out, err := n.call(ctx, opReplicate, req, nil)
	if err != nil {
		return nil, err
	}
	return out.(*pb.ReplicateResponse), nil
}
