package shardraft

import (
	"context"
	"encoding/binary"
	"fmt"

	pb "github.com/OraCache/oracache/gen/gateway/v1"
	rpb "github.com/OraCache/oracache/gen/replication/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type operation byte

const (
	opGet operation = 1 + iota
	opPut
	opDelete
	opBatchGet
	opBatchPut
	opReadOutbox
	opAckOutbox
	opReplicate
)

// A versioned 18-byte header precedes the unmodified protobuf request:
// version:u8, operation:u8, proposing peer:u64, sequence:u64 (big endian).
// The ID correlates a local waiter; it does NOT deduplicate client retries.
const commandHeader = 18
const maxBatch = 1000

type requestID [16]byte

func encodeCommand(op operation, id requestID, data []byte) []byte {
	out := make([]byte, commandHeader+len(data))
	out[0], out[1] = 1, byte(op)
	copy(out[2:commandHeader], id[:])
	copy(out[commandHeader:], data)
	return out
}

func decodeCommand(data []byte) (operation, requestID, []byte, error) {
	var id requestID
	if len(data) < commandHeader || data[0] != 1 {
		return 0, id, nil, fmt.Errorf("invalid command header")
	}
	op := operation(data[1])
	if op != opPut && op != opDelete && op != opBatchPut && op != opAckOutbox && op != opReplicate {
		return 0, id, nil, fmt.Errorf("invalid replicated operation %d", op)
	}
	copy(id[:], data[2:commandHeader])
	if binary.BigEndian.Uint64(id[:8]) == 0 || binary.BigEndian.Uint64(id[8:]) == 0 {
		return 0, id, nil, fmt.Errorf("invalid proposal ID")
	}
	return op, id, data[commandHeader:], nil
}

func decodeRequest(op operation, data []byte) (proto.Message, error) {
	var message proto.Message
	switch op {
	case opGet:
		message = &pb.GetRequest{}
	case opPut:
		message = &pb.PutRequest{}
	case opDelete:
		message = &pb.DeleteRequest{}
	case opBatchGet:
		message = &pb.BatchGetRequest{}
	case opBatchPut:
		message = &pb.BatchPutRequest{}
	case opReadOutbox:
		message = &rpb.ReadOutboxRequest{}
	case opAckOutbox:
		message = &rpb.AckOutboxRequest{}
	case opReplicate:
		message = &rpb.ReplicateRequest{}
	default:
		return nil, fmt.Errorf("unknown operation")
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return nil, err
	}
	return message, validateRequest(message)
}

func validateRequest(message proto.Message) error {
	bad := func() error {
		return status.Error(codes.InvalidArgument, "nonempty keys and at most 1000 batch items are required")
	}
	if message == nil || !message.ProtoReflect().IsValid() {
		return bad()
	}
	switch r := message.(type) {
	case *rpb.ReadOutboxRequest:
	case *rpb.AckOutboxRequest:
		if r.Id == 0 {
			return bad()
		}
	case *rpb.ReplicateRequest:
		if r.SourceRegion == "" || r.TargetRegion == "" || r.StreamId == "" || r.Entry.GetId() == 0 || r.Entry.GetKey() == "" {
			return bad()
		}
		var prev uint64
		for _, v := range r.Entry.Versions {
			if v == nil || v.Rev <= prev {
				return bad()
			}
			prev = v.Rev
		}
	case *pb.GetRequest:
		if r.Key == "" {
			return bad()
		}
	case *pb.PutRequest:
		if r.Key == "" {
			return bad()
		}
	case *pb.DeleteRequest:
		if r.Key == "" {
			return bad()
		}
	case *pb.BatchGetRequest:
		if len(r.Items) > maxBatch {
			return bad()
		}
		for _, item := range r.Items {
			if item.GetKey() == "" {
				return bad()
			}
		}
	case *pb.BatchPutRequest:
		if len(r.Items) > maxBatch {
			return bad()
		}
		for _, item := range r.Items {
			if item.GetKey() == "" {
				return bad()
			}
		}
	default:
		return bad()
	}
	return nil
}

// execute is called only by the owner loop, never by transport or client goroutines.
// Returned status errors must be deterministic application outcomes. Unexpected
// backend failures stop the instance rather than silently skipping a command.
func (n *Instance) execute(message proto.Message) (response proto.Message, err error) {
	defer func() {
		if err == nil && (response == nil || !response.ProtoReflect().IsValid()) {
			response, err = nil, fmt.Errorf("cache returned a nil response without an error")
		}
	}()
	ctx := context.Background()
	switch r := message.(type) {
	case *rpb.ReadOutboxRequest:
		if sm, ok := n.cache.(ReplicationStateMachine); ok {
			return sm.ReadOutbox()
		}
		return nil, status.Error(codes.FailedPrecondition, "replication is not configured")
	case *rpb.AckOutboxRequest:
		if sm, ok := n.cache.(ReplicationStateMachine); ok {
			return sm.AckOutbox(r)
		}
		return nil, status.Error(codes.FailedPrecondition, "replication is not configured")
	case *rpb.ReplicateRequest:
		if sm, ok := n.cache.(ReplicationStateMachine); ok {
			return sm.Replicate(r)
		}
		return nil, status.Error(codes.FailedPrecondition, "replication is not configured")
	case *pb.GetRequest:
		return n.cache.Get(ctx, r)
	case *pb.PutRequest:
		return n.cache.Put(ctx, r)
	case *pb.DeleteRequest:
		return n.cache.Delete(ctx, r)
	case *pb.BatchGetRequest:
		out := &pb.BatchGetResponse{Items: make([]*pb.GetResponse, 0, len(r.Items))}
		for _, item := range r.Items {
			response, err := n.cache.Get(ctx, item)
			if err != nil {
				return nil, err
			}
			if response == nil {
				return nil, fmt.Errorf("cache returned a nil Get response")
			}
			out.Items = append(out.Items, response)
		}
		return out, nil
	case *pb.BatchPutRequest:
		out := &pb.BatchPutResponse{Items: make([]*pb.PutResponse, 0, len(r.Items))}
		for _, item := range r.Items {
			response, err := n.cache.Put(ctx, item)
			if err != nil {
				return nil, err
			}
			if response == nil {
				return nil, fmt.Errorf("cache returned a nil Put response")
			}
			out.Items = append(out.Items, response)
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unexpected state machine operation")
	}
}

func expectedApplicationError(err error) bool {
	if err == nil {
		return true
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.ResourceExhausted, codes.FailedPrecondition, codes.OutOfRange, codes.AlreadyExists, codes.NotFound:
		return true
	default:
		return false
	}
}
