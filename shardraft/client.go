package shardraft

import (
	"context"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var _ pb.CacheGatewayClient = (*Instance)(nil)

func (n *Instance) call(ctx context.Context, op operation, message proto.Message, opts []grpc.CallOption) (proto.Message, error) {
	// Transport options cannot be implemented faithfully by an in-process API.
	// Reject explicitly rather than silently ignoring headers, credentials, etc.
	if len(opts) != 0 {
		return nil, status.Error(codes.InvalidArgument, "gRPC call options require a network CacheGatewayClient")
	}
	if err := validateRequest(message); err != nil {
		return nil, err
	}
	if proto.Size(message) > n.cfg.MaxCommandBytes-commandHeader {
		return nil, status.Error(codes.ResourceExhausted, "request exceeds command-size limit")
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	if !n.Status().Running {
		return nil, unavailable("instance is not running")
	}
	r := request{ctx: ctx, op: op, data: data, reply: make(chan result, 1)}
	select {
	case n.requests <- r:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-n.stop:
		return nil, unavailable("instance stopping")
	case <-n.done:
		return nil, unavailable("instance stopped")
	}
	select {
	case response := <-r.reply:
		return response.message, response.err
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case <-n.done:
		return nil, unavailable("instance stopped; proposed writes may have committed")
	}
}

func (n *Instance) Get(ctx context.Context, r *pb.GetRequest, opts ...grpc.CallOption) (*pb.GetResponse, error) {
	out, err := n.call(ctx, opGet, r, opts)
	if err != nil {
		return nil, err
	}
	return out.(*pb.GetResponse), nil
}
func (n *Instance) Put(ctx context.Context, r *pb.PutRequest, opts ...grpc.CallOption) (*pb.PutResponse, error) {
	out, err := n.call(ctx, opPut, r, opts)
	if err != nil {
		return nil, err
	}
	return out.(*pb.PutResponse), nil
}
func (n *Instance) Delete(ctx context.Context, r *pb.DeleteRequest, opts ...grpc.CallOption) (*pb.DeleteResponse, error) {
	out, err := n.call(ctx, opDelete, r, opts)
	if err != nil {
		return nil, err
	}
	return out.(*pb.DeleteResponse), nil
}
func (n *Instance) BatchGet(ctx context.Context, r *pb.BatchGetRequest, opts ...grpc.CallOption) (*pb.BatchGetResponse, error) {
	out, err := n.call(ctx, opBatchGet, r, opts)
	if err != nil {
		return nil, err
	}
	return out.(*pb.BatchGetResponse), nil
}
func (n *Instance) BatchPut(ctx context.Context, r *pb.BatchPutRequest, opts ...grpc.CallOption) (*pb.BatchPutResponse, error) {
	out, err := n.call(ctx, opBatchPut, r, opts)
	if err != nil {
		return nil, err
	}
	return out.(*pb.BatchPutResponse), nil
}

// GatewayServer adapts the client-shaped API to the generated server interface.
// The host owns grpc.Server and its listener. It must allow messages large
// enough for the public batch contract (e.g. grpc.MaxRecvMsgSize(16 << 20)).
func GatewayServer(client pb.CacheGatewayClient) pb.CacheGatewayServer {
	return &gatewayServer{client: client}
}

type gatewayServer struct {
	pb.UnimplementedCacheGatewayServer
	client pb.CacheGatewayClient
}

func (s *gatewayServer) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	return s.client.Get(ctx, r)
}
func (s *gatewayServer) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	return s.client.Put(ctx, r)
}
func (s *gatewayServer) Delete(ctx context.Context, r *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	return s.client.Delete(ctx, r)
}
func (s *gatewayServer) BatchGet(ctx context.Context, r *pb.BatchGetRequest) (*pb.BatchGetResponse, error) {
	return s.client.BatchGet(ctx, r)
}
func (s *gatewayServer) BatchPut(ctx context.Context, r *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	return s.client.BatchPut(ctx, r)
}
