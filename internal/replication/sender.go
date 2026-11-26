// Package replication implements stream state and the network relay. The sender
// reads and acknowledges events through the source Raft instance.
package replication

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/OraCache/oracache/gen/replication/v1"
	"github.com/OraCache/oracache/shardraft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Source interface {
	Status() shardraft.Status
	ReadOutbox(context.Context) (*pb.ReadOutboxResponse, error)
	AckOutbox(context.Context, uint64) error
}

type Sender struct {
	source           Source
	sourceRegion     string
	targetRegion     string
	streamID         string
	shardID          uint32
	resolveAddresses func() []string
	logger           *slog.Logger
	mu               sync.Mutex
	clients          map[string]*senderClient
	closed           bool
}

type senderClient struct {
	conn   *grpc.ClientConn
	client pb.ReplicationServiceClient
}

func NewSender(source Source, sourceRegion, targetRegion, streamID string, shardID uint32, resolveAddresses func() []string, logger *slog.Logger) (*Sender, error) {
	if resolveAddresses == nil {
		return nil, fmt.Errorf("replication: address resolver is required")
	}
	s := &Sender{
		source:           source,
		sourceRegion:     sourceRegion,
		targetRegion:     targetRegion,
		streamID:         streamID,
		shardID:          shardID,
		resolveAddresses: resolveAddresses,
		clients:          make(map[string]*senderClient),
		logger:           logger,
	}
	if _, err := s.resolveClients(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Sender) resolveClients() ([]pb.ReplicationServiceClient, error) {
	addresses := s.resolveAddresses()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, status.Error(codes.Unavailable, "replication sender closed")
	}
	var out []pb.ReplicationServiceClient
	keep := make(map[string]bool, len(addresses))
	for _, addr := range addresses {
		if addr == "" || keep[addr] {
			continue
		}
		keep[addr] = true
		if existing := s.clients[addr]; existing != nil {
			out = append(out, existing.client)
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20), grpc.MaxCallSendMsgSize(16<<20)))
		if err != nil {
			return nil, fmt.Errorf("replication client: %w", err)
		}
		client := pb.NewReplicationServiceClient(conn)
		s.clients[addr] = &senderClient{conn: conn, client: client}
		out = append(out, client)
	}
	for addr, client := range s.clients {
		if !keep[addr] {
			_ = client.conn.Close()
			delete(s.clients, addr)
		}
	}
	return out, nil
}

func (s *Sender) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for addr, client := range s.clients {
		_ = client.conn.Close()
		delete(s.clients, addr)
	}
}

func (s *Sender) active() bool {
	st := s.source.Status()
	return st.Running && st.Ready && st.Role == "StateLeader"
}

func (s *Sender) Run(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for s.active() && ctx.Err() == nil {
			clients, err := s.resolveClients()
			if err != nil || len(clients) == 0 {
				break
			}
			pending, err := s.source.ReadOutbox(ctx)
			if err != nil || pending.Entry == nil {
				break
			}
			req := &pb.ReplicateRequest{
				SourceRegion: s.sourceRegion,
				TargetRegion: s.targetRegion,
				StreamId:     s.streamID,
				ShardId:      s.shardID,
				Entry:        pending.Entry,
			}
			delivered := false
			for _, client := range clients {
				if !s.active() || ctx.Err() != nil {
					break
				}
				attempt, cancel := context.WithTimeout(ctx, time.Second)
				response, sendErr := client.Replicate(attempt, req)
				cancel()
				if sendErr == nil && response.GetAppliedId() >= req.Entry.Id {
					delivered = true
					break
				}
				if sendErr != nil && s.logger != nil {
					s.logger.Debug("replication attempt failed", "id", req.Entry.Id, "error", sendErr)
				}
			}
			if !delivered {
				break
			}
			if err := s.source.AckOutbox(ctx, req.Entry.Id); err != nil {
				break
			}
		}
	}
}
