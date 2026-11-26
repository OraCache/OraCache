package replication

import (
	"context"
	pb "github.com/OraCache/oracache/gen/replication/v1"
)

type Destination interface {
	Replicate(context.Context, *pb.ReplicateRequest) (*pb.ReplicateResponse, error)
}

type Receiver struct {
	pb.UnimplementedReplicationServiceServer
	Destination Destination
}

func (r *Receiver) Replicate(ctx context.Context, req *pb.ReplicateRequest) (*pb.ReplicateResponse, error) {
	return r.Destination.Replicate(ctx, req)
}
